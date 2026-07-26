using Microsoft.Extensions.Logging;
using Polly;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.ActiveMQ
{
    /// <summary>
    /// Shared request/reply plumbing for the three leader/peer-pull operations
    /// (<c>RestoreFromLeaderAsync</c>, <c>SyncDeltaFromLeaderAsync</c>, <c>FetchPeerSubscriptionsAsync</c>).
    /// Requester side: <see cref="SendRequestAsync"/> sends an <see cref="ActiveMqClusterRequestEnvelope"/>
    /// to the target's request queue and awaits a matching <see cref="ActiveMqClusterResponseEnvelope"/>
    /// on this node's own reply queue. Answering side: the request consumer's <c>AsyncListener</c>
    /// (wired in <see cref="ActiveMqClusterMessageBus.EnsureInitializedAsync"/>) delivers into
    /// <see cref="HandleRequestDeliveryAsync"/>, which dispatches to <see cref="BuildResponseAsync"/>,
    /// implemented per request kind in <c>ActiveMqClusterMessageBus.Snapshots.cs</c> /
    /// <c>.SubscriptionFetch.cs</c>. ActiveMQ/JMS has no native request/reply (unlike NATS), so this
    /// mirrors <c>KafkaClusterMessageBus</c>/<c>RabbitMqClusterMessageBus</c>'s hand-rolled
    /// correlation-id scheme exactly.
    /// </summary>
    internal sealed partial class ActiveMqClusterMessageBus
    {
        private readonly ResiliencePipeline _resiliencePipeline = ClusterResiliencePipelineFactory.Create();

        /// <summary>Internal (rather than private) so tests can exercise the requester side directly.</summary>
        internal async Task<ActiveMqClusterResponseEnvelope> SendRequestAsync(
            Uri targetNodeEndpoint,
            ActiveMqClusterRequestKind kind,
            string? channelName,
            Guid? channelKey,
            long? sinceTicks,
            CancellationToken cancellationToken)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var request = new ActiveMqClusterRequestEnvelope(Guid.NewGuid(), kind, channelName, channelKey, sinceTicks, _nodeEndpoint);
            var tcs = new TaskCompletionSource<ActiveMqClusterResponseEnvelope>(TaskCreationOptions.RunContinuationsAsynchronously);

            if (!_pendingRequests.TryAdd(request.CorrelationId, tcs))
                throw new InvalidOperationException($"Duplicate ActiveMQ cluster request correlation id '{request.CorrelationId}'.");

            try
            {
                var requestQueueName = ActiveMqTopicNaming.RequestQueue(_options.TopicPrefix, targetNodeEndpoint);

                // Only the send itself is retried/circuit-broken — retrying the full
                // send-then-await-reply round trip under the same policy would compound the wait (up
                // to RequestTimeout per attempt) and complicate correlating which attempt a late reply
                // belongs to (same reasoning as KafkaClusterMessageBus.SendRequestAsync).
                await _resiliencePipeline.ExecuteAsync(async ct =>
                {
                    await _publishLock.WaitAsync(ct).ConfigureAwait(false);
                    try
                    {
                        var requestQueue = await _publishSession!.GetQueueAsync(requestQueueName).ConfigureAwait(false);
                        var textMessage = await _publishSession.CreateTextMessageAsync(request.ToNJson()).ConfigureAwait(false);
                        await _publishProducer!.SendAsync(requestQueue, textMessage).ConfigureAwait(false);
                    }
                    finally
                    {
                        _publishLock.Release();
                    }
                }, cancellationToken).ConfigureAwait(false);

                using var timeoutCts = new CancellationTokenSource(_options.RequestTimeout);
                using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);
                await using var registration = linkedCts.Token.Register(() => tcs.TrySetCanceled(linkedCts.Token));

                var response = await tcs.Task.ConfigureAwait(false);
                if (!response.Success)
                    throw new InvalidOperationException($"ActiveMQ cluster request '{kind}' to '{targetNodeEndpoint.Host}' was rejected: {response.ErrorMessage}");

                return response;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (OperationCanceledException)
            {
                throw new TimeoutException($"ActiveMQ cluster request '{kind}' to '{targetNodeEndpoint.Host}' timed out after {_options.RequestTimeout}.");
            }
            finally
            {
                _pendingRequests.TryRemove(request.CorrelationId, out _);
            }
        }

        /// <summary>Internal (rather than private) so tests can drive it directly with a raw payload.</summary>
        internal async Task HandleRequestDeliveryAsync(string payload, CancellationToken cancellationToken)
        {
            ActiveMqClusterRequestEnvelope? request;
            try
            {
                request = payload.FromNJson<ActiveMqClusterRequestEnvelope>();
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
            ActiveMqClusterResponseEnvelope? response;
            try
            {
                response = payload.FromNJson<ActiveMqClusterResponseEnvelope>();
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
        internal bool TryCompletePendingRequest(ActiveMqClusterResponseEnvelope response)
        {
            if (!_pendingRequests.TryRemove(response.CorrelationId, out var pending))
                return false;

            return pending.TrySetResult(response);
        }

        /// <summary>
        /// Answers an inbound request by dispatching to the per-kind builder (implemented in
        /// <c>ActiveMqClusterMessageBus.Snapshots.cs</c> / <c>.SubscriptionFetch.cs</c>) and sends
        /// the result back to the requester's reply queue.
        /// </summary>
        internal async Task HandleIncomingRequestAsync(ActiveMqClusterRequestEnvelope request, CancellationToken cancellationToken)
        {
            var response = await BuildResponseAsync(request, cancellationToken).ConfigureAwait(false);
            var replyQueueName = ActiveMqTopicNaming.ReplyQueue(_options.TopicPrefix, request.ReplyToNodeEndpoint);

            await _publishLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                var replyQueue = await _publishSession!.GetQueueAsync(replyQueueName).ConfigureAwait(false);
                var textMessage = await _publishSession.CreateTextMessageAsync(response.ToNJson()).ConfigureAwait(false);
                await _publishProducer!.SendAsync(replyQueue, textMessage).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Log.ReplyPublishFailed(_logger, exception, replyQueueName);
            }
            finally
            {
                _publishLock.Release();
            }
        }

        internal async Task<ActiveMqClusterResponseEnvelope> BuildResponseAsync(ActiveMqClusterRequestEnvelope request, CancellationToken cancellationToken)
        {
            try
            {
                return request.Kind switch
                {
                    ActiveMqClusterRequestKind.RestoreSnapshot => await BuildRestoreSnapshotResponseAsync(request, cancellationToken).ConfigureAwait(false),
                    ActiveMqClusterRequestKind.SyncDelta => await BuildSyncDeltaResponseAsync(request, cancellationToken).ConfigureAwait(false),
                    ActiveMqClusterRequestKind.FetchSubscriptions => BuildFetchSubscriptionsResponse(request),
                    _ => new ActiveMqClusterResponseEnvelope(request.CorrelationId, false, $"Unknown request kind '{request.Kind}'.", null)
                };
            }
            catch (Exception exception)
            {
                Log.RequestHandlingFaulted(_logger, exception, request.Kind.ToString());
                return new ActiveMqClusterResponseEnvelope(request.CorrelationId, false, exception.Message, null);
            }
        }

        private static partial class Log
        {
            [LoggerMessage(EventId = 91030, Level = LogLevel.Warning,
                Message = "[Cluster] ActiveMQ cluster request could not be parsed; skipping it.")]
            public static partial void RequestUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91031, Level = LogLevel.Warning,
                Message = "[Cluster] ActiveMQ cluster response could not be parsed; skipping it.")]
            public static partial void ResponseUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91032, Level = LogLevel.Warning,
                Message = "[Cluster] ActiveMQ reply publish to queue '{Queue}' failed.")]
            public static partial void ReplyPublishFailed(ILogger logger, Exception exception, string queue);

            [LoggerMessage(EventId = 91033, Level = LogLevel.Error,
                Message = "[Cluster] ActiveMQ request handling faulted for kind '{Kind}'.")]
            public static partial void RequestHandlingFaulted(ILogger logger, Exception exception, string kind);
        }
    }
}
