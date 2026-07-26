using Microsoft.Extensions.Logging;
using Polly;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.Mqtt
{
    /// <summary>
    /// Shared request/reply plumbing for the three leader/peer-pull operations
    /// (<c>RestoreFromLeaderAsync</c>, <c>SyncDeltaFromLeaderAsync</c>, <c>FetchPeerSubscriptionsAsync</c>).
    /// Requester side: <see cref="SendRequestAsync"/> publishes a <see cref="MqttClusterRequestEnvelope"/>
    /// to the target's request topic and awaits a matching <see cref="MqttClusterResponseEnvelope"/>
    /// on this node's own reply topic. Answering side: <see cref="RunRequestListenerLoopAsync"/>
    /// consumes this node's own request topic and dispatches to <see cref="BuildResponseAsync"/>,
    /// implemented per request kind in <c>MqttClusterMessageBus.Snapshots.cs</c> /
    /// <c>.SubscriptionFetch.cs</c>. MQTT has no native request/reply (unlike NATS), so this mirrors
    /// <c>KafkaClusterMessageBus</c>/<c>PulsarClusterMessageBus</c>'s hand-rolled correlation-id/
    /// reply-topic scheme exactly.
    /// </summary>
    internal sealed partial class MqttClusterMessageBus
    {
        private readonly ResiliencePipeline _resiliencePipeline = ClusterResiliencePipelineFactory.Create();

        /// <summary>Internal (rather than private) so tests can exercise the requester side directly.</summary>
        internal async Task<MqttClusterResponseEnvelope> SendRequestAsync(
            Uri targetNodeEndpoint,
            MqttClusterRequestKind kind,
            string? channelName,
            Guid? channelKey,
            long? sinceTicks,
            CancellationToken cancellationToken)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var request = new MqttClusterRequestEnvelope(Guid.NewGuid(), kind, channelName, channelKey, sinceTicks, _nodeEndpoint);
            var tcs = new TaskCompletionSource<MqttClusterResponseEnvelope>(TaskCreationOptions.RunContinuationsAsynchronously);

            if (!_pendingRequests.TryAdd(request.CorrelationId, tcs))
                throw new InvalidOperationException($"Duplicate MQTT cluster request correlation id '{request.CorrelationId}'.");

            try
            {
                var requestTopic = MqttTopicNaming.RequestTopic(_options.TopicPrefix, targetNodeEndpoint);

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
                    throw new InvalidOperationException($"MQTT cluster request '{kind}' to '{targetNodeEndpoint.Host}' was rejected: {response.ErrorMessage}");

                return response;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (OperationCanceledException)
            {
                throw new TimeoutException($"MQTT cluster request '{kind}' to '{targetNodeEndpoint.Host}' timed out after {_options.RequestTimeout}.");
            }
            finally
            {
                _pendingRequests.TryRemove(request.CorrelationId, out _);
            }
        }

        internal async Task RunRequestListenerLoopAsync(string requestTopic, CancellationToken cancellationToken)
        {
            try
            {
                await foreach (var payload in _transport.SubscribeAsync(requestTopic, cancellationToken).ConfigureAwait(false))
                {
                    await HandleRequestDeliveryAsync(payload, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
                // Expected on shutdown.
            }
        }

        internal async Task RunReplyListenerLoopAsync(string replyTopic, CancellationToken cancellationToken)
        {
            try
            {
                await foreach (var payload in _transport.SubscribeAsync(replyTopic, cancellationToken).ConfigureAwait(false))
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
            MqttClusterRequestEnvelope? request;
            try
            {
                request = payload.FromNJson<MqttClusterRequestEnvelope>();
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
            MqttClusterResponseEnvelope? response;
            try
            {
                response = payload.FromNJson<MqttClusterResponseEnvelope>();
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
        internal bool TryCompletePendingRequest(MqttClusterResponseEnvelope response)
        {
            if (!_pendingRequests.TryRemove(response.CorrelationId, out var pending))
                return false;

            return pending.TrySetResult(response);
        }

        /// <summary>
        /// Answers an inbound request by dispatching to the per-kind builder (implemented in
        /// <c>MqttClusterMessageBus.Snapshots.cs</c> / <c>.SubscriptionFetch.cs</c>) and publishes
        /// the result back to the requester's reply topic.
        /// </summary>
        internal async Task HandleIncomingRequestAsync(MqttClusterRequestEnvelope request, CancellationToken cancellationToken)
        {
            var response = await BuildResponseAsync(request, cancellationToken).ConfigureAwait(false);
            var replyTopic = MqttTopicNaming.ReplyTopic(_options.TopicPrefix, request.ReplyToNodeEndpoint);

            try
            {
                await _transport.PublishAsync(replyTopic, response.ToNJson(), cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Log.ReplyPublishFailed(_logger, exception, replyTopic);
            }
        }

        internal async Task<MqttClusterResponseEnvelope> BuildResponseAsync(MqttClusterRequestEnvelope request, CancellationToken cancellationToken)
        {
            try
            {
                return request.Kind switch
                {
                    MqttClusterRequestKind.RestoreSnapshot => await BuildRestoreSnapshotResponseAsync(request, cancellationToken).ConfigureAwait(false),
                    MqttClusterRequestKind.SyncDelta => await BuildSyncDeltaResponseAsync(request, cancellationToken).ConfigureAwait(false),
                    MqttClusterRequestKind.FetchSubscriptions => BuildFetchSubscriptionsResponse(request),
                    _ => new MqttClusterResponseEnvelope(request.CorrelationId, false, $"Unknown request kind '{request.Kind}'.", null)
                };
            }
            catch (Exception exception)
            {
                Log.RequestHandlingFaulted(_logger, exception, request.Kind.ToString());
                return new MqttClusterResponseEnvelope(request.CorrelationId, false, exception.Message, null);
            }
        }

        private static partial class Log
        {
            [LoggerMessage(EventId = 9930, Level = LogLevel.Warning,
                Message = "[Cluster] MQTT cluster request could not be parsed; skipping it.")]
            public static partial void RequestUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 9931, Level = LogLevel.Warning,
                Message = "[Cluster] MQTT cluster response could not be parsed; skipping it.")]
            public static partial void ResponseUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 9932, Level = LogLevel.Warning,
                Message = "[Cluster] MQTT reply publish to topic '{Topic}' failed.")]
            public static partial void ReplyPublishFailed(ILogger logger, Exception exception, string topic);

            [LoggerMessage(EventId = 9933, Level = LogLevel.Error,
                Message = "[Cluster] MQTT request handling faulted for kind '{Kind}'.")]
            public static partial void RequestHandlingFaulted(ILogger logger, Exception exception, string kind);
        }
    }
}
