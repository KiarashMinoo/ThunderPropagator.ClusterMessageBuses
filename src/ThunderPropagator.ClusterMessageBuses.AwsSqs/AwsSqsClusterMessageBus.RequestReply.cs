using Amazon.SQS.Model;
using Microsoft.Extensions.Logging;
using Polly;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.AwsSqs
{
    /// <summary>
    /// Shared broker-native request/reply plumbing for the three leader/peer-pull operations
    /// (<c>RestoreFromLeaderAsync</c>, <c>SyncDeltaFromLeaderAsync</c>, <c>FetchPeerSubscriptionsAsync</c>).
    /// Requester side: <see cref="SendRequestAsync"/> sends an <see cref="AwsSqsClusterRequestEnvelope"/>
    /// to the target's request queue (resolved via <c>GetQueueUrlAsync</c>) and awaits a matching
    /// <see cref="AwsSqsClusterResponseEnvelope"/> on this node's own reply queue (polled by
    /// <see cref="AwsSqsClusterMessageBus.EnsureInitializedAsync"/>'s background loop). Answering
    /// side: that same initialization also starts the request-queue poller, which delivers into
    /// <see cref="HandleRequestDeliveryAsync"/>, dispatching to <see cref="BuildResponseAsync"/>,
    /// implemented per request kind in <c>AwsSqsClusterMessageBus.Snapshots.cs</c> / <c>.SubscriptionFetch.cs</c>.
    /// </summary>
    internal sealed partial class AwsSqsClusterMessageBus
    {
        private readonly ResiliencePipeline _resiliencePipeline = ClusterResiliencePipelineFactory.Create();

        /// <summary>Internal (rather than private) so tests can exercise the requester side directly.</summary>
        internal async Task<AwsSqsClusterResponseEnvelope> SendRequestAsync(
            Uri targetNodeEndpoint,
            AwsSqsClusterRequestKind kind,
            string? channelName,
            Guid? channelKey,
            long? sinceTicks,
            CancellationToken cancellationToken)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var request = new AwsSqsClusterRequestEnvelope(Guid.NewGuid(), kind, channelName, channelKey, sinceTicks, _nodeEndpoint);
            var tcs = new TaskCompletionSource<AwsSqsClusterResponseEnvelope>(TaskCreationOptions.RunContinuationsAsynchronously);

            if (!_pendingRequests.TryAdd(request.CorrelationId, tcs))
                throw new InvalidOperationException($"Duplicate AwsSqs cluster request correlation id '{request.CorrelationId}'.");

            try
            {
                var requestQueueName = AwsSqsResourceNaming.RequestQueue(_options.ResourcePrefix, targetNodeEndpoint);

                // Only the send itself is retried/circuit-broken — retrying the full
                // send-then-await-reply round trip under the same policy would compound the wait (up
                // to RequestTimeout per attempt), same reasoning as every other broker-style
                // transport in this repo.
                await _resiliencePipeline.ExecuteAsync(async ct =>
                {
                    var requestQueueUrl = (await _sqs!.GetQueueUrlAsync(requestQueueName, ct).ConfigureAwait(false)).QueueUrl;
                    await _sqs!.SendMessageAsync(new SendMessageRequest
                    {
                        QueueUrl = requestQueueUrl,
                        MessageBody = request.ToNJson(),
                    }, ct).ConfigureAwait(false);
                }, cancellationToken).ConfigureAwait(false);

                using var timeoutCts = new CancellationTokenSource(_options.RequestTimeout);
                using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);
                await using var registration = linkedCts.Token.Register(() => tcs.TrySetCanceled(linkedCts.Token));

                var response = await tcs.Task.ConfigureAwait(false);
                if (!response.Success)
                    throw new InvalidOperationException($"AwsSqs cluster request '{kind}' to '{targetNodeEndpoint.Host}' was rejected: {response.ErrorMessage}");

                return response;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (OperationCanceledException)
            {
                throw new TimeoutException($"AwsSqs cluster request '{kind}' to '{targetNodeEndpoint.Host}' timed out after {_options.RequestTimeout}.");
            }
            finally
            {
                _pendingRequests.TryRemove(request.CorrelationId, out _);
            }
        }

        /// <summary>Internal (rather than private) so tests can drive it directly with a raw message body.</summary>
        internal async Task HandleRequestDeliveryAsync(string body, CancellationToken cancellationToken)
        {
            AwsSqsClusterRequestEnvelope? request;
            try
            {
                request = body.FromNJson<AwsSqsClusterRequestEnvelope>();
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
            AwsSqsClusterResponseEnvelope? response;
            try
            {
                response = body.FromNJson<AwsSqsClusterResponseEnvelope>();
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
        internal bool TryCompletePendingRequest(AwsSqsClusterResponseEnvelope response)
        {
            if (!_pendingRequests.TryRemove(response.CorrelationId, out var pending))
                return false;

            return pending.TrySetResult(response);
        }

        /// <summary>
        /// Answers an inbound request by dispatching to the per-kind builder (implemented in
        /// <c>AwsSqsClusterMessageBus.Snapshots.cs</c> / <c>.SubscriptionFetch.cs</c>) and sends the
        /// result to the requester's own reply queue — derived from <see cref="AwsSqsClusterRequestEnvelope.ReplyToNodeEndpoint"/>
        /// rather than trusting a queue URL supplied on the wire.
        /// </summary>
        internal async Task HandleIncomingRequestAsync(AwsSqsClusterRequestEnvelope request, CancellationToken cancellationToken)
        {
            var response = await BuildResponseAsync(request, cancellationToken).ConfigureAwait(false);
            var replyQueueName = AwsSqsResourceNaming.ReplyQueue(_options.ResourcePrefix, request.ReplyToNodeEndpoint);

            try
            {
                var replyQueueUrl = (await _sqs!.GetQueueUrlAsync(replyQueueName, cancellationToken).ConfigureAwait(false)).QueueUrl;
                await _sqs!.SendMessageAsync(new SendMessageRequest
                {
                    QueueUrl = replyQueueUrl,
                    MessageBody = response.ToNJson(),
                }, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Log.ReplySendFailed(_logger, exception, replyQueueName);
            }
        }

        internal async Task<AwsSqsClusterResponseEnvelope> BuildResponseAsync(AwsSqsClusterRequestEnvelope request, CancellationToken cancellationToken)
        {
            try
            {
                return request.Kind switch
                {
                    AwsSqsClusterRequestKind.RestoreSnapshot => await BuildRestoreSnapshotResponseAsync(request, cancellationToken).ConfigureAwait(false),
                    AwsSqsClusterRequestKind.SyncDelta => await BuildSyncDeltaResponseAsync(request, cancellationToken).ConfigureAwait(false),
                    AwsSqsClusterRequestKind.FetchSubscriptions => BuildFetchSubscriptionsResponse(request),
                    _ => new AwsSqsClusterResponseEnvelope(request.CorrelationId, false, $"Unknown request kind '{request.Kind}'.", null)
                };
            }
            catch (Exception exception)
            {
                Log.RequestHandlingFaulted(_logger, exception, request.Kind.ToString());
                return new AwsSqsClusterResponseEnvelope(request.CorrelationId, false, exception.Message, null);
            }
        }

        private static partial class Log
        {
            [LoggerMessage(EventId = 9730, Level = LogLevel.Warning,
                Message = "[Cluster] AwsSqs cluster request could not be parsed; skipping it.")]
            public static partial void RequestUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 9731, Level = LogLevel.Warning,
                Message = "[Cluster] AwsSqs cluster response could not be parsed; skipping it.")]
            public static partial void ResponseUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 9732, Level = LogLevel.Warning,
                Message = "[Cluster] Sending an AwsSqs cluster reply to queue '{Queue}' failed.")]
            public static partial void ReplySendFailed(ILogger logger, Exception exception, string queue);

            [LoggerMessage(EventId = 9733, Level = LogLevel.Error,
                Message = "[Cluster] AwsSqs request handling faulted for kind '{Kind}'.")]
            public static partial void RequestHandlingFaulted(ILogger logger, Exception exception, string kind);
        }
    }
}
