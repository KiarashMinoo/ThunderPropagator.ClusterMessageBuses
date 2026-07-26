using System.Text;
using Microsoft.Extensions.Logging;
using Polly;
using RabbitMQ.Client;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.RabbitMQ
{
    /// <summary>
    /// Shared broker-native request/reply plumbing for the three leader/peer-pull operations
    /// (<c>RestoreFromLeaderAsync</c>, <c>SyncDeltaFromLeaderAsync</c>, <c>FetchPeerSubscriptionsAsync</c>).
    /// Requester side: <see cref="SendRequestAsync"/> publishes a <see cref="RabbitMqClusterRequestEnvelope"/>
    /// to the target's request queue (via the default exchange) and awaits a matching
    /// <see cref="RabbitMqClusterResponseEnvelope"/> on this node's own reply queue. Answering side:
    /// the reply/request consumers started in <see cref="RabbitMqClusterMessageBus.EnsureInitializedAsync"/>
    /// deliver into <see cref="HandleRequestDeliveryAsync"/>, which dispatches to
    /// <see cref="BuildResponseAsync"/>, implemented per request kind in
    /// <c>RabbitMqClusterMessageBus.Snapshots.cs</c> / <c>.SubscriptionFetch.cs</c>.
    /// </summary>
    internal sealed partial class RabbitMqClusterMessageBus
    {
        private readonly ResiliencePipeline _resiliencePipeline = ClusterResiliencePipelineFactory.Create();

        /// <summary>Internal (rather than private) so tests can exercise the requester side directly.</summary>
        internal async Task<RabbitMqClusterResponseEnvelope> SendRequestAsync(
            Uri targetNodeEndpoint,
            RabbitMqClusterRequestKind kind,
            string? channelName,
            Guid? channelKey,
            long? sinceTicks,
            CancellationToken cancellationToken)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var request = new RabbitMqClusterRequestEnvelope(Guid.NewGuid(), kind, channelName, channelKey, sinceTicks, _nodeEndpoint);
            var tcs = new TaskCompletionSource<RabbitMqClusterResponseEnvelope>(TaskCreationOptions.RunContinuationsAsynchronously);

            if (!_pendingRequests.TryAdd(request.CorrelationId, tcs))
                throw new InvalidOperationException($"Duplicate RabbitMQ cluster request correlation id '{request.CorrelationId}'.");

            try
            {
                var requestQueue = RabbitMqTopicNaming.RequestQueue(_options.ExchangePrefix, targetNodeEndpoint);
                var body = Encoding.UTF8.GetBytes(request.ToNJson());

                // Only the publish itself is retried/circuit-broken — retrying the full
                // publish-then-await-reply round trip under the same policy would compound the
                // wait (up to RequestTimeout per attempt) and complicate correlating which attempt
                // a late reply belongs to (same reasoning as KafkaClusterMessageBus.SendRequestAsync).
                await _resiliencePipeline.ExecuteAsync(async ct =>
                {
                    await _publishLock.WaitAsync(ct).ConfigureAwait(false);
                    try
                    {
                        await _publishChannel!.BasicPublishAsync(string.Empty, requestQueue, false, new BasicProperties(), body).ConfigureAwait(false);
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
                    throw new InvalidOperationException($"RabbitMQ cluster request '{kind}' to '{targetNodeEndpoint.Host}' was rejected: {response.ErrorMessage}");

                return response;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (OperationCanceledException)
            {
                throw new TimeoutException($"RabbitMQ cluster request '{kind}' to '{targetNodeEndpoint.Host}' timed out after {_options.RequestTimeout}.");
            }
            finally
            {
                _pendingRequests.TryRemove(request.CorrelationId, out _);
            }
        }

        /// <summary>Internal (rather than private) so tests can drive it directly with a raw message body.</summary>
        internal async Task HandleRequestDeliveryAsync(byte[] body, CancellationToken cancellationToken)
        {
            RabbitMqClusterRequestEnvelope? request;
            try
            {
                request = Encoding.UTF8.GetString(body).FromNJson<RabbitMqClusterRequestEnvelope>();
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

        /// <summary>
        /// Internal (rather than private) so tests can drive it directly with a raw message body.
        /// Deliberately synchronous under the hood (no actual awaits are needed) but returns
        /// <see cref="Task"/> since it's invoked from an <c>async</c> event handler via <c>await</c>.
        /// </summary>
        internal Task HandleReplyDeliveryAsync(byte[] body, CancellationToken cancellationToken)
        {
            RabbitMqClusterResponseEnvelope? response;
            try
            {
                response = Encoding.UTF8.GetString(body).FromNJson<RabbitMqClusterResponseEnvelope>();
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
        internal bool TryCompletePendingRequest(RabbitMqClusterResponseEnvelope response)
        {
            if (!_pendingRequests.TryRemove(response.CorrelationId, out var pending))
                return false;

            return pending.TrySetResult(response);
        }

        /// <summary>
        /// Answers an inbound request by dispatching to the per-kind builder (implemented in
        /// <c>RabbitMqClusterMessageBus.Snapshots.cs</c> / <c>.SubscriptionFetch.cs</c>) and
        /// publishes the result back to the requester's reply queue.
        /// </summary>
        internal async Task HandleIncomingRequestAsync(RabbitMqClusterRequestEnvelope request, CancellationToken cancellationToken)
        {
            var response = await BuildResponseAsync(request, cancellationToken).ConfigureAwait(false);
            var replyQueue = RabbitMqTopicNaming.ReplyQueue(_options.ExchangePrefix, request.ReplyToNodeEndpoint);
            var body = Encoding.UTF8.GetBytes(response.ToNJson());

            await _publishLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                await _publishChannel!.BasicPublishAsync(string.Empty, replyQueue, false, new BasicProperties(), body).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Log.ReplyPublishFailed(_logger, exception, replyQueue);
            }
            finally
            {
                _publishLock.Release();
            }
        }

        internal async Task<RabbitMqClusterResponseEnvelope> BuildResponseAsync(RabbitMqClusterRequestEnvelope request, CancellationToken cancellationToken)
        {
            try
            {
                return request.Kind switch
                {
                    RabbitMqClusterRequestKind.RestoreSnapshot => await BuildRestoreSnapshotResponseAsync(request, cancellationToken).ConfigureAwait(false),
                    RabbitMqClusterRequestKind.SyncDelta => await BuildSyncDeltaResponseAsync(request, cancellationToken).ConfigureAwait(false),
                    RabbitMqClusterRequestKind.FetchSubscriptions => BuildFetchSubscriptionsResponse(request),
                    _ => new RabbitMqClusterResponseEnvelope(request.CorrelationId, false, $"Unknown request kind '{request.Kind}'.", null)
                };
            }
            catch (Exception exception)
            {
                Log.RequestHandlingFaulted(_logger, exception, request.Kind.ToString());
                return new RabbitMqClusterResponseEnvelope(request.CorrelationId, false, exception.Message, null);
            }
        }

        private static partial class Log
        {
            [LoggerMessage(EventId = 9630, Level = LogLevel.Warning,
                Message = "[Cluster] RabbitMQ cluster request could not be parsed; skipping it.")]
            public static partial void RequestUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 9631, Level = LogLevel.Warning,
                Message = "[Cluster] RabbitMQ cluster response could not be parsed; skipping it.")]
            public static partial void ResponseUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 9632, Level = LogLevel.Warning,
                Message = "[Cluster] RabbitMQ reply publish to queue '{Queue}' failed.")]
            public static partial void ReplyPublishFailed(ILogger logger, Exception exception, string queue);

            [LoggerMessage(EventId = 9633, Level = LogLevel.Error,
                Message = "[Cluster] RabbitMQ request handling faulted for kind '{Kind}'.")]
            public static partial void RequestHandlingFaulted(ILogger logger, Exception exception, string kind);
        }
    }
}
