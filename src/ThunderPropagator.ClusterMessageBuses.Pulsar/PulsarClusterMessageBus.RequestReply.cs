using Microsoft.Extensions.Logging;
using Polly;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.Pulsar
{
    /// <summary>
    /// Shared request/reply plumbing for the three leader/peer-pull operations
    /// (<c>RestoreFromLeaderAsync</c>, <c>SyncDeltaFromLeaderAsync</c>, <c>FetchPeerSubscriptionsAsync</c>).
    /// Requester side: <see cref="SendRequestAsync"/> publishes a <see cref="PulsarClusterRequestEnvelope"/>
    /// to the target's request topic and awaits a matching <see cref="PulsarClusterResponseEnvelope"/>
    /// on this node's own reply topic. Answering side: <see cref="RunRequestListenerLoopAsync"/>
    /// consumes this node's own request topic and dispatches to <see cref="BuildResponseAsync"/>,
    /// implemented per request kind in <c>PulsarClusterMessageBus.Snapshots.cs</c> /
    /// <c>.SubscriptionFetch.cs</c>. Pulsar has no native request/reply (unlike NATS), so this mirrors
    /// <c>KafkaClusterMessageBus</c>'s hand-rolled correlation-id/reply-topic scheme exactly.
    /// </summary>
    internal sealed partial class PulsarClusterMessageBus
    {
        private readonly ResiliencePipeline _resiliencePipeline = ClusterResiliencePipelineFactory.Create();

        /// <summary>Internal (rather than private) so tests can exercise the requester side directly.</summary>
        internal async Task<PulsarClusterResponseEnvelope> SendRequestAsync(
            Uri targetNodeEndpoint,
            PulsarClusterRequestKind kind,
            string? channelName,
            Guid? channelKey,
            long? sinceTicks,
            CancellationToken cancellationToken)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var request = new PulsarClusterRequestEnvelope(Guid.NewGuid(), kind, channelName, channelKey, sinceTicks, _nodeEndpoint);
            var tcs = new TaskCompletionSource<PulsarClusterResponseEnvelope>(TaskCreationOptions.RunContinuationsAsynchronously);

            if (!_pendingRequests.TryAdd(request.CorrelationId, tcs))
                throw new InvalidOperationException($"Duplicate Pulsar cluster request correlation id '{request.CorrelationId}'.");

            try
            {
                var requestTopic = PulsarTopicNaming.RequestTopic(_options.TopicPrefix, targetNodeEndpoint);

                // Only the publish itself is retried/circuit-broken — retrying the full
                // publish-then-await-reply round trip under the same policy would compound the
                // wait (up to RequestTimeout per attempt) and complicate correlating which attempt
                // a late reply belongs to (same reasoning as KafkaClusterMessageBus.SendRequestAsync).
                await _resiliencePipeline.ExecuteAsync(
                    async ct => await _transport.PublishAsync(requestTopic, request.ToNJson(), ct).ConfigureAwait(false),
                    cancellationToken).ConfigureAwait(false);

                using var timeoutCts = new CancellationTokenSource(_options.RequestTimeout);
                using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);
                await using var registration = linkedCts.Token.Register(() => tcs.TrySetCanceled(linkedCts.Token));

                var response = await tcs.Task.ConfigureAwait(false);
                if (!response.Success)
                    throw new InvalidOperationException($"Pulsar cluster request '{kind}' to '{targetNodeEndpoint.Host}' was rejected: {response.ErrorMessage}");

                return response;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (OperationCanceledException)
            {
                throw new TimeoutException($"Pulsar cluster request '{kind}' to '{targetNodeEndpoint.Host}' timed out after {_options.RequestTimeout}.");
            }
            finally
            {
                _pendingRequests.TryRemove(request.CorrelationId, out _);
            }
        }

        internal async Task RunRequestListenerLoopAsync(string requestTopic, string subscriptionName, CancellationToken cancellationToken)
        {
            try
            {
                await foreach (var payload in _transport.SubscribeAsync(requestTopic, subscriptionName, cancellationToken).ConfigureAwait(false))
                {
                    await HandleRequestDeliveryAsync(payload, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
                // Expected on shutdown.
            }
        }

        internal async Task RunReplyListenerLoopAsync(string replyTopic, string subscriptionName, CancellationToken cancellationToken)
        {
            try
            {
                await foreach (var payload in _transport.SubscribeAsync(replyTopic, subscriptionName, cancellationToken).ConfigureAwait(false))
                {
                    await HandleReplyDeliveryAsync(payload, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
                // Expected on shutdown.
            }
        }

        /// <summary>Internal (rather than private) so tests can drive it directly with a raw payload.</summary>
        internal async Task HandleRequestDeliveryAsync(string payload, CancellationToken cancellationToken)
        {
            PulsarClusterRequestEnvelope? request;
            try
            {
                request = payload.FromNJson<PulsarClusterRequestEnvelope>();
            }
            catch (Exception exception)
            {
                Log.RequestUnparseable(_logger, exception);
                return;
            }

            if (request is null)
                return;

            await HandleIncomingRequestAsync(request, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>Internal (rather than private) so tests can drive it directly with a raw payload.</summary>
        internal Task HandleReplyDeliveryAsync(string payload, CancellationToken cancellationToken)
        {
            PulsarClusterResponseEnvelope? response;
            try
            {
                response = payload.FromNJson<PulsarClusterResponseEnvelope>();
            }
            catch (Exception exception)
            {
                Log.ResponseUnparseable(_logger, exception);
                return Task.CompletedTask;
            }

            if (response is not null)
                TryCompletePendingRequest(response);

            return Task.CompletedTask;
        }

        /// <summary>
        /// Completes the pending <see cref="SendRequestAsync"/> call matching
        /// <paramref name="response"/>'s correlation id, if one is still waiting. Shared by
        /// <see cref="HandleReplyDeliveryAsync"/> (the real, only production caller) and tests, which
        /// use it to complete a round trip deterministically without racing a background consumer.
        /// </summary>
        internal bool TryCompletePendingRequest(PulsarClusterResponseEnvelope response)
        {
            if (!_pendingRequests.TryRemove(response.CorrelationId, out var pending))
                return false;

            return pending.TrySetResult(response);
        }

        /// <summary>
        /// Answers an inbound request by dispatching to the per-kind builder (implemented in
        /// <c>PulsarClusterMessageBus.Snapshots.cs</c> / <c>.SubscriptionFetch.cs</c>) and publishes
        /// the result back to the requester's reply topic.
        /// </summary>
        internal async Task HandleIncomingRequestAsync(PulsarClusterRequestEnvelope request, CancellationToken cancellationToken)
        {
            var response = await BuildResponseAsync(request, cancellationToken).ConfigureAwait(false);
            var replyTopic = PulsarTopicNaming.ReplyTopic(_options.TopicPrefix, request.ReplyToNodeEndpoint);

            try
            {
                await _transport.PublishAsync(replyTopic, response.ToNJson(), cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Log.ReplyPublishFailed(_logger, exception, replyTopic);
            }
        }

        internal async Task<PulsarClusterResponseEnvelope> BuildResponseAsync(PulsarClusterRequestEnvelope request, CancellationToken cancellationToken)
        {
            try
            {
                return request.Kind switch
                {
                    PulsarClusterRequestKind.RestoreSnapshot => await BuildRestoreSnapshotResponseAsync(request, cancellationToken).ConfigureAwait(false),
                    PulsarClusterRequestKind.SyncDelta => await BuildSyncDeltaResponseAsync(request, cancellationToken).ConfigureAwait(false),
                    PulsarClusterRequestKind.FetchSubscriptions => BuildFetchSubscriptionsResponse(request),
                    _ => new PulsarClusterResponseEnvelope(request.CorrelationId, false, $"Unknown request kind '{request.Kind}'.", null)
                };
            }
            catch (Exception exception)
            {
                Log.RequestHandlingFaulted(_logger, exception, request.Kind.ToString());
                return new PulsarClusterResponseEnvelope(request.CorrelationId, false, exception.Message, null);
            }
        }

        private static partial class Log
        {
            [LoggerMessage(EventId = 9830, Level = LogLevel.Warning,
                Message = "[Cluster] Pulsar cluster request could not be parsed; skipping it.")]
            public static partial void RequestUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 9831, Level = LogLevel.Warning,
                Message = "[Cluster] Pulsar cluster response could not be parsed; skipping it.")]
            public static partial void ResponseUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 9832, Level = LogLevel.Warning,
                Message = "[Cluster] Pulsar reply publish to topic '{Topic}' failed.")]
            public static partial void ReplyPublishFailed(ILogger logger, Exception exception, string topic);

            [LoggerMessage(EventId = 9833, Level = LogLevel.Error,
                Message = "[Cluster] Pulsar request handling faulted for kind '{Kind}'.")]
            public static partial void RequestHandlingFaulted(ILogger logger, Exception exception, string kind);
        }
    }
}
