using Microsoft.Extensions.Logging;
using Polly;
using StackExchange.Redis;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.RedisPubSub
{
    /// <summary>
    /// Shared request/reply plumbing for the three leader/peer-pull operations
    /// (<c>RestoreFromLeaderAsync</c>, <c>SyncDeltaFromLeaderAsync</c>, <c>FetchPeerSubscriptionsAsync</c>).
    /// Requester side: <see cref="SendRequestAsync"/> publishes a <see cref="RedisPubSubClusterRequestEnvelope"/>
    /// to the target's request channel and awaits a matching <see cref="RedisPubSubClusterResponseEnvelope"/>
    /// on this node's own reply channel. Answering side: the request subscription's handler (wired in
    /// <see cref="RedisPubSubClusterMessageBus.EnsureInitializedAsync"/>) delivers into
    /// <see cref="HandleRequestDeliveryAsync"/>, which dispatches to <see cref="BuildResponseAsync"/>,
    /// implemented per request kind in <c>RedisPubSubClusterMessageBus.Snapshots.cs</c> /
    /// <c>.SubscriptionFetch.cs</c>. Redis pub/sub has no native request/reply (unlike NATS), so this
    /// mirrors <c>KafkaClusterMessageBus</c>/<c>PulsarClusterMessageBus</c>/<c>MqttClusterMessageBus</c>/
    /// <c>ActiveMqClusterMessageBus</c>'s hand-rolled correlation-id scheme exactly.
    /// </summary>
    internal sealed partial class RedisPubSubClusterMessageBus
    {
        private readonly ResiliencePipeline _resiliencePipeline = ClusterResiliencePipelineFactory.Create();

        /// <summary>Internal (rather than private) so tests can exercise the requester side directly.</summary>
        internal async Task<RedisPubSubClusterResponseEnvelope> SendRequestAsync(
            Uri targetNodeEndpoint,
            RedisPubSubClusterRequestKind kind,
            string? channelName,
            Guid? channelKey,
            long? sinceTicks,
            CancellationToken cancellationToken)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var request = new RedisPubSubClusterRequestEnvelope(Guid.NewGuid(), kind, channelName, channelKey, sinceTicks, _nodeEndpoint);
            var tcs = new TaskCompletionSource<RedisPubSubClusterResponseEnvelope>(TaskCreationOptions.RunContinuationsAsynchronously);

            if (!_pendingRequests.TryAdd(request.CorrelationId, tcs))
                throw new InvalidOperationException($"Duplicate Redis pub/sub cluster request correlation id '{request.CorrelationId}'.");

            try
            {
                var requestChannelName = RedisChannelNaming.RequestChannel(_options.ChannelPrefix, targetNodeEndpoint);

                // Only the publish itself is retried/circuit-broken — retrying the full
                // publish-then-await-reply round trip under the same policy would compound the
                // wait (up to RequestTimeout per attempt) and complicate correlating which attempt
                // a late reply belongs to (same reasoning as KafkaClusterMessageBus.SendRequestAsync).
                await _resiliencePipeline.ExecuteAsync(
                    async ct => await _subscriber!.PublishAsync(RedisChannel.Literal(requestChannelName), request.ToNJson()).ConfigureAwait(false),
                    cancellationToken).ConfigureAwait(false);

                using var timeoutCts = new CancellationTokenSource(_options.RequestTimeout);
                using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);
                await using var registration = linkedCts.Token.Register(() => tcs.TrySetCanceled(linkedCts.Token));

                var response = await tcs.Task.ConfigureAwait(false);
                if (!response.Success)
                    throw new InvalidOperationException($"Redis pub/sub cluster request '{kind}' to '{targetNodeEndpoint.Host}' was rejected: {response.ErrorMessage}");

                return response;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (OperationCanceledException)
            {
                throw new TimeoutException($"Redis pub/sub cluster request '{kind}' to '{targetNodeEndpoint.Host}' timed out after {_options.RequestTimeout}.");
            }
            finally
            {
                _pendingRequests.TryRemove(request.CorrelationId, out _);
            }
        }

        /// <summary>Internal (rather than private) so tests can drive it directly with a raw payload.</summary>
        internal async Task HandleRequestDeliveryAsync(string payload, CancellationToken cancellationToken)
        {
            RedisPubSubClusterRequestEnvelope? request;
            try
            {
                request = payload.FromNJson<RedisPubSubClusterRequestEnvelope>();
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
            RedisPubSubClusterResponseEnvelope? response;
            try
            {
                response = payload.FromNJson<RedisPubSubClusterResponseEnvelope>();
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
        /// use it to complete a round trip deterministically without racing a background subscription.
        /// </summary>
        internal bool TryCompletePendingRequest(RedisPubSubClusterResponseEnvelope response)
        {
            if (!_pendingRequests.TryRemove(response.CorrelationId, out var pending))
                return false;

            return pending.TrySetResult(response);
        }

        /// <summary>
        /// Answers an inbound request by dispatching to the per-kind builder (implemented in
        /// <c>RedisPubSubClusterMessageBus.Snapshots.cs</c> / <c>.SubscriptionFetch.cs</c>) and
        /// publishes the result back to the requester's reply channel.
        /// </summary>
        internal async Task HandleIncomingRequestAsync(RedisPubSubClusterRequestEnvelope request, CancellationToken cancellationToken)
        {
            var response = await BuildResponseAsync(request, cancellationToken).ConfigureAwait(false);
            var replyChannelName = RedisChannelNaming.ReplyChannel(_options.ChannelPrefix, request.ReplyToNodeEndpoint);

            try
            {
                await _subscriber!.PublishAsync(RedisChannel.Literal(replyChannelName), response.ToNJson()).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Log.ReplyPublishFailed(_logger, exception, replyChannelName);
            }
        }

        internal async Task<RedisPubSubClusterResponseEnvelope> BuildResponseAsync(RedisPubSubClusterRequestEnvelope request, CancellationToken cancellationToken)
        {
            try
            {
                return request.Kind switch
                {
                    RedisPubSubClusterRequestKind.RestoreSnapshot => await BuildRestoreSnapshotResponseAsync(request, cancellationToken).ConfigureAwait(false),
                    RedisPubSubClusterRequestKind.SyncDelta => await BuildSyncDeltaResponseAsync(request, cancellationToken).ConfigureAwait(false),
                    RedisPubSubClusterRequestKind.FetchSubscriptions => BuildFetchSubscriptionsResponse(request),
                    _ => new RedisPubSubClusterResponseEnvelope(request.CorrelationId, false, $"Unknown request kind '{request.Kind}'.", null)
                };
            }
            catch (Exception exception)
            {
                Log.RequestHandlingFaulted(_logger, exception, request.Kind.ToString());
                return new RedisPubSubClusterResponseEnvelope(request.CorrelationId, false, exception.Message, null);
            }
        }

        private static partial class Log
        {
            [LoggerMessage(EventId = 91130, Level = LogLevel.Warning,
                Message = "[Cluster] Redis pub/sub cluster request could not be parsed; skipping it.")]
            public static partial void RequestUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91131, Level = LogLevel.Warning,
                Message = "[Cluster] Redis pub/sub cluster response could not be parsed; skipping it.")]
            public static partial void ResponseUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91132, Level = LogLevel.Warning,
                Message = "[Cluster] Redis pub/sub reply publish to channel '{Channel}' failed.")]
            public static partial void ReplyPublishFailed(ILogger logger, Exception exception, string channel);

            [LoggerMessage(EventId = 91133, Level = LogLevel.Error,
                Message = "[Cluster] Redis pub/sub request handling faulted for kind '{Kind}'.")]
            public static partial void RequestHandlingFaulted(ILogger logger, Exception exception, string kind);
        }
    }
}
