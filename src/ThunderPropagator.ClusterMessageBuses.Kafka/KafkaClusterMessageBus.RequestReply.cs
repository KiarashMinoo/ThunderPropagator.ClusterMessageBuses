using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Polly;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.Kafka
{
    /// <summary>
    /// Shared broker-native request/reply plumbing for the three leader/peer-pull operations
    /// (<c>RestoreFromLeaderAsync</c>, <c>SyncDeltaFromLeaderAsync</c>, <c>FetchPeerSubscriptionsAsync</c>).
    /// Requester side: <see cref="SendRequestAsync"/> publishes a <see cref="ClusterRequestEnvelope"/>
    /// to the target's request topic and awaits a matching <see cref="ClusterResponseEnvelope"/> on
    /// this node's own reply topic. Answering side: <see cref="RunRequestListenerLoopAsync"/> consumes
    /// this node's own request topic and dispatches to <see cref="BuildResponseAsync"/>, implemented per
    /// request kind in <c>KafkaClusterMessageBus.Snapshots.cs</c> / <c>.SubscriptionFetch.cs</c>.
    /// </summary>
    internal sealed partial class KafkaClusterMessageBus
    {
        private readonly ResiliencePipeline _resiliencePipeline = ClusterResiliencePipelineFactory.Create();

        /// <summary>Internal (rather than private) so tests can exercise the requester side directly.</summary>
        internal async Task<ClusterResponseEnvelope> SendRequestAsync(
            Uri targetNodeEndpoint,
            ClusterRequestKind kind,
            string? channelName,
            Guid? channelKey,
            long? sinceTicks,
            CancellationToken cancellationToken)
        {
            var request = new ClusterRequestEnvelope(Guid.NewGuid(), kind, channelName, channelKey, sinceTicks, _nodeEndpoint);
            var tcs = _pendingRequests.Register(request.CorrelationId);

            try
            {
                var requestTopic = KafkaTopicNaming.RequestTopic(_options.TopicPrefix, targetNodeEndpoint);

                // Only the publish itself is retried/circuit-broken — retrying the full
                // produce-then-await-reply round trip under the same policy would compound the
                // wait (up to RequestTimeout per attempt) and complicate correlating which attempt
                // a late reply belongs to.
                await _resiliencePipeline.ExecuteAsync(
                    async ct => await _producer.ProduceAsync(requestTopic, new Message<string, string> { Value = request.ToNJson() }, ct).ConfigureAwait(false),
                    cancellationToken).ConfigureAwait(false);

                using var timeoutCts = new CancellationTokenSource(_options.RequestTimeout);
                using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);
                await using var registration = linkedCts.Token.Register(() => tcs.TrySetCanceled(linkedCts.Token));

                var response = await tcs.Task.ConfigureAwait(false);
                if (!response.Success)
                    throw new InvalidOperationException($"Kafka cluster request '{kind}' to '{targetNodeEndpoint.Host}' was rejected: {response.ErrorMessage}");

                return response;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (OperationCanceledException)
            {
                throw new TimeoutException($"Kafka cluster request '{kind}' to '{targetNodeEndpoint.Host}' timed out after {_options.RequestTimeout}.");
            }
            finally
            {
                _pendingRequests.Remove(request.CorrelationId);
            }
        }

        private async Task RunRequestListenerLoopAsync(CancellationToken cancellationToken)
        {
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    ConsumeResult<string, string>? result;
                    try
                    {
                        result = _requestConsumer.Consume(cancellationToken);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                    catch (ConsumeException exception)
                    {
                        Log.RequestListenerConsumeFailed(_logger, exception);
                        continue;
                    }

                    ClusterRequestEnvelope? request;
                    try
                    {
                        request = result?.Message?.Value?.FromNJson<ClusterRequestEnvelope>();
                    }
                    catch (Exception exception)
                    {
                        Log.RequestUnparseable(_logger, exception);
                        continue;
                    }

                    if (request is null)
                        continue;

                    await HandleIncomingRequestAsync(request, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
                // Expected on shutdown.
            }
        }

        private async Task RunReplyListenerLoopAsync(CancellationToken cancellationToken)
        {
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    ConsumeResult<string, string>? result;
                    try
                    {
                        result = _replyConsumer.Consume(cancellationToken);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                    catch (ConsumeException exception)
                    {
                        Log.ReplyListenerConsumeFailed(_logger, exception);
                        continue;
                    }

                    ClusterResponseEnvelope? response;
                    try
                    {
                        response = result?.Message?.Value?.FromNJson<ClusterResponseEnvelope>();
                    }
                    catch (Exception exception)
                    {
                        Log.ResponseUnparseable(_logger, exception);
                        continue;
                    }

                    if (response is null)
                        continue;

                    TryCompletePendingRequest(response);
                }
            }
            catch (OperationCanceledException)
            {
                // Expected on shutdown.
            }
        }

        /// <summary>
        /// Completes the pending <see cref="SendRequestAsync"/> call matching
        /// <paramref name="response"/>'s correlation id, if one is still waiting. Shared by
        /// <see cref="RunReplyListenerLoopAsync"/> (the real, only production caller) and tests,
        /// which use it to complete a round trip deterministically without racing a background
        /// consumer loop.
        /// </summary>
        internal bool TryCompletePendingRequest(ClusterResponseEnvelope response) =>
            _pendingRequests.TryComplete(response.CorrelationId, () => response);

        /// <summary>
        /// Answers an inbound request by dispatching to the per-kind builder (implemented in
        /// <c>KafkaClusterMessageBus.Snapshots.cs</c> / <c>.SubscriptionFetch.cs</c>) and publishes
        /// the result back to the requester's reply topic.
        /// </summary>
        internal async Task HandleIncomingRequestAsync(ClusterRequestEnvelope request, CancellationToken cancellationToken)
        {
            var response = await BuildResponseAsync(request, cancellationToken).ConfigureAwait(false);
            ArgumentNullException.ThrowIfNull(request.ReplyToNodeEndpoint);
            var replyTopic = KafkaTopicNaming.ReplyTopic(_options.TopicPrefix, request.ReplyToNodeEndpoint);

            try
            {
                await _producer.ProduceAsync(replyTopic, new Message<string, string> { Value = response.ToNJson() }, cancellationToken).ConfigureAwait(false);
            }
            catch (ProduceException<string, string> exception)
            {
                Log.ReplyPublishFailed(_logger, exception, replyTopic);
            }
        }

        internal async Task<ClusterResponseEnvelope> BuildResponseAsync(ClusterRequestEnvelope request, CancellationToken cancellationToken)
        {
            try
            {
                return request.Kind switch
                {
                    ClusterRequestKind.RestoreSnapshot => await BuildRestoreSnapshotResponseAsync(request, cancellationToken).ConfigureAwait(false),
                    ClusterRequestKind.SyncDelta => await BuildSyncDeltaResponseAsync(request, cancellationToken).ConfigureAwait(false),
                    ClusterRequestKind.FetchSubscriptions => BuildFetchSubscriptionsResponse(request),
                    ClusterRequestKind.PullSnapshotBytes => await BuildPullSnapshotBytesResponseAsync(request, cancellationToken).ConfigureAwait(false),
                    _ => new ClusterResponseEnvelope(request.CorrelationId, false, $"Unknown request kind '{request.Kind}'.", null)
                };
            }
            catch (Exception exception)
            {
                Log.RequestHandlingFaulted(_logger, exception, request.Kind.ToString());
                return new ClusterResponseEnvelope(request.CorrelationId, false, exception.Message, null);
            }
        }

        private static partial class Log
        {
            [LoggerMessage(EventId = 9530, Level = LogLevel.Warning,
                Message = "[Cluster] Kafka request-listener consume failed.")]
            public static partial void RequestListenerConsumeFailed(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 9531, Level = LogLevel.Warning,
                Message = "[Cluster] Kafka reply-listener consume failed.")]
            public static partial void ReplyListenerConsumeFailed(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 9534, Level = LogLevel.Warning,
                Message = "[Cluster] Kafka cluster request could not be parsed; skipping it.")]
            public static partial void RequestUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 9535, Level = LogLevel.Warning,
                Message = "[Cluster] Kafka cluster response could not be parsed; skipping it.")]
            public static partial void ResponseUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 9532, Level = LogLevel.Warning,
                Message = "[Cluster] Kafka reply publish to topic '{Topic}' failed.")]
            public static partial void ReplyPublishFailed(ILogger logger, Exception exception, string topic);

            [LoggerMessage(EventId = 9533, Level = LogLevel.Error,
                Message = "[Cluster] Kafka request handling faulted for kind '{Kind}'.")]
            public static partial void RequestHandlingFaulted(ILogger logger, Exception exception, string kind);
        }
    }
}
