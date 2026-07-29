using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using Polly;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.GcpPubSub
{
    /// <summary>
    /// Shared request/reply plumbing for the three leader/peer-pull operations
    /// (<c>RestoreFromLeaderAsync</c>, <c>SyncDeltaFromLeaderAsync</c>, <c>FetchPeerSubscriptionsAsync</c>).
    /// Requester side: <see cref="SendRequestAsync"/> publishes a <see cref="GcpPubSubClusterRequestEnvelope"/>
    /// to the target's request topic (built from its own <c>NodeEndpoint</c>) and awaits a matching
    /// <see cref="GcpPubSubClusterResponseEnvelope"/> on this node's own reply subscription (polled by
    /// <see cref="GcpPubSubClusterMessageBus.EnsureInitializedAsync"/>'s background loop). Answering
    /// side: that same initialization also starts the request-subscription poller, which delivers into
    /// <see cref="HandleRequestDeliveryAsync"/>, dispatching to <see cref="BuildResponseAsync"/>,
    /// implemented per request kind in <c>GcpPubSubClusterMessageBus.Snapshots.cs</c> / <c>.SubscriptionFetch.cs</c>.
    /// Unlike <c>AwsSqsClusterMessageBus</c> (which addresses real SQS queues), Pub/Sub has no queue
    /// primitive at all — every node's request/reply "destination" here is a topic with exactly one
    /// subscription (its own), which behaves like a point-to-point queue as long as no second
    /// subscription is ever added (see <see cref="GcpPubSubResourceNaming"/>), matching CLAUDE.md's
    /// documented pattern for brokers with only topics/subscriptions. This mirrors
    /// <c>RedisPubSubClusterMessageBus</c>'s hand-rolled correlation-id scheme almost exactly, since
    /// Redis pub/sub has the same lack of a native request/reply primitive.
    /// </summary>
    internal sealed partial class GcpPubSubClusterMessageBus
    {
        private readonly ResiliencePipeline _resiliencePipeline = ClusterResiliencePipelineFactory.Create();

        /// <summary>Internal (rather than private) so tests can exercise the requester side directly.</summary>
        internal async Task<GcpPubSubClusterResponseEnvelope> SendRequestAsync(
            Uri targetNodeEndpoint,
            GcpPubSubClusterRequestKind kind,
            string? channelName,
            Guid? channelKey,
            long? sinceTicks,
            CancellationToken cancellationToken)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var request = new GcpPubSubClusterRequestEnvelope(Guid.NewGuid(), kind, channelName, channelKey, sinceTicks, _nodeEndpoint);
            var tcs = new TaskCompletionSource<GcpPubSubClusterResponseEnvelope>(TaskCreationOptions.RunContinuationsAsynchronously);

            if (!_pendingRequests.TryAdd(request.CorrelationId, tcs))
                throw new InvalidOperationException($"Duplicate GcpPubSub cluster request correlation id '{request.CorrelationId}'.");

            try
            {
                var requestTopicName = new TopicName(_options.ProjectId, GcpPubSubResourceNaming.RequestTopicId(_options.ResourcePrefix, targetNodeEndpoint));
                var requestSubscriptionName = new SubscriptionName(_options.ProjectId, GcpPubSubResourceNaming.RequestSubscriptionId(_options.ResourcePrefix, targetNodeEndpoint));

                // Only the send itself is retried/circuit-broken — retrying the full
                // send-then-await-reply round trip under the same policy would compound the wait (up
                // to RequestTimeout per attempt), same reasoning as every other broker-style
                // transport in this repo.
                await _resiliencePipeline.ExecuteAsync(async ct =>
                {
                    // The requester ensures the target's request topic/subscription exist too: on a
                    // freshly-started target that hasn't called EnsureInitializedAsync yet, publishing
                    // to a not-yet-created topic would otherwise just silently vanish (Pub/Sub topics
                    // don't buffer for subscriptions that don't exist yet) rather than erroring, unlike
                    // sending to a real queue that already exists.
                    await EnsureTopicAsync(requestTopicName, ct).ConfigureAwait(false);
                    await EnsureSubscriptionAsync(requestSubscriptionName, requestTopicName, ct).ConfigureAwait(false);

                    var pubsubMessage = new PubsubMessage { Data = ByteString.CopyFromUtf8(request.ToNJson()) };
                    await _publisher!.PublishAsync(requestTopicName, [pubsubMessage], ct).ConfigureAwait(false);
                }, cancellationToken).ConfigureAwait(false);

                using var timeoutCts = new CancellationTokenSource(_options.RequestTimeout);
                using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);
                await using var registration = linkedCts.Token.Register(() => tcs.TrySetCanceled(linkedCts.Token));

                var response = await tcs.Task.ConfigureAwait(false);
                if (!response.Success)
                    throw new InvalidOperationException($"GcpPubSub cluster request '{kind}' to '{targetNodeEndpoint.Host}' was rejected: {response.ErrorMessage}");

                return response;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (OperationCanceledException)
            {
                throw new TimeoutException($"GcpPubSub cluster request '{kind}' to '{targetNodeEndpoint.Host}' timed out after {_options.RequestTimeout}.");
            }
            finally
            {
                _pendingRequests.TryRemove(request.CorrelationId, out _);
            }
        }

        /// <summary>Internal (rather than private) so tests can drive it directly with a raw message body.</summary>
        internal async Task HandleRequestDeliveryAsync(string body, CancellationToken cancellationToken)
        {
            GcpPubSubClusterRequestEnvelope? request;
            try
            {
                request = body.FromNJson<GcpPubSubClusterRequestEnvelope>();
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

        /// <summary>Internal (rather than private) so tests can drive it directly with a raw message body.</summary>
        internal Task HandleReplyDeliveryAsync(string body, CancellationToken cancellationToken)
        {
            GcpPubSubClusterResponseEnvelope? response;
            try
            {
                response = body.FromNJson<GcpPubSubClusterResponseEnvelope>();
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
        /// <paramref name="response"/>'s correlation id, if one is still waiting.
        /// </summary>
        internal bool TryCompletePendingRequest(GcpPubSubClusterResponseEnvelope response)
        {
            if (!_pendingRequests.TryRemove(response.CorrelationId, out var pending))
                return false;

            return pending.TrySetResult(response);
        }

        /// <summary>
        /// Answers an inbound request by dispatching to the per-kind builder (implemented in
        /// <c>GcpPubSubClusterMessageBus.Snapshots.cs</c> / <c>.SubscriptionFetch.cs</c>) and publishes
        /// the result to the requester's own reply topic — derived from
        /// <see cref="GcpPubSubClusterRequestEnvelope.ReplyToNodeEndpoint"/> rather than trusting a
        /// destination supplied on the wire.
        /// </summary>
        internal async Task HandleIncomingRequestAsync(GcpPubSubClusterRequestEnvelope request, CancellationToken cancellationToken)
        {
            var response = await BuildResponseAsync(request, cancellationToken).ConfigureAwait(false);
            var replyTopicName = new TopicName(_options.ProjectId, GcpPubSubResourceNaming.ReplyTopicId(_options.ResourcePrefix, request.ReplyToNodeEndpoint));

            try
            {
                var replySubscriptionName = new SubscriptionName(_options.ProjectId, GcpPubSubResourceNaming.ReplySubscriptionId(_options.ResourcePrefix, request.ReplyToNodeEndpoint));
                await EnsureTopicAsync(replyTopicName, cancellationToken).ConfigureAwait(false);
                await EnsureSubscriptionAsync(replySubscriptionName, replyTopicName, cancellationToken).ConfigureAwait(false);

                var pubsubMessage = new PubsubMessage { Data = ByteString.CopyFromUtf8(response.ToNJson()) };
                await _publisher!.PublishAsync(replyTopicName, [pubsubMessage], cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Log.ReplySendFailed(_logger, exception, replyTopicName.TopicId);
            }
        }

        internal async Task<GcpPubSubClusterResponseEnvelope> BuildResponseAsync(GcpPubSubClusterRequestEnvelope request, CancellationToken cancellationToken)
        {
            try
            {
                return request.Kind switch
                {
                    GcpPubSubClusterRequestKind.RestoreSnapshot => await BuildRestoreSnapshotResponseAsync(request, cancellationToken).ConfigureAwait(false),
                    GcpPubSubClusterRequestKind.SyncDelta => await BuildSyncDeltaResponseAsync(request, cancellationToken).ConfigureAwait(false),
                    GcpPubSubClusterRequestKind.FetchSubscriptions => BuildFetchSubscriptionsResponse(request),
                    _ => new GcpPubSubClusterResponseEnvelope(request.CorrelationId, false, $"Unknown request kind '{request.Kind}'.", null)
                };
            }
            catch (Exception exception)
            {
                Log.RequestHandlingFaulted(_logger, exception, request.Kind.ToString());
                return new GcpPubSubClusterResponseEnvelope(request.CorrelationId, false, exception.Message, null);
            }
        }

        private static partial class Log
        {
            [LoggerMessage(EventId = 9830, Level = LogLevel.Warning,
                Message = "[Cluster] GcpPubSub cluster request could not be parsed; skipping it.")]
            public static partial void RequestUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 9831, Level = LogLevel.Warning,
                Message = "[Cluster] GcpPubSub cluster response could not be parsed; skipping it.")]
            public static partial void ResponseUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 9832, Level = LogLevel.Warning,
                Message = "[Cluster] Sending a GcpPubSub cluster reply to topic '{Topic}' failed.")]
            public static partial void ReplySendFailed(ILogger logger, Exception exception, string topic);

            [LoggerMessage(EventId = 9833, Level = LogLevel.Error,
                Message = "[Cluster] GcpPubSub request handling faulted for kind '{Kind}'.")]
            public static partial void RequestHandlingFaulted(ILogger logger, Exception exception, string kind);
        }
    }
}
