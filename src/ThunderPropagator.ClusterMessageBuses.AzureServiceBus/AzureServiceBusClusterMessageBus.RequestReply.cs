using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using Polly;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.AzureServiceBus
{
    /// <summary>
    /// Shared broker-native request/reply plumbing for the three leader/peer-pull operations
    /// (<c>RestoreFromLeaderAsync</c>, <c>SyncDeltaFromLeaderAsync</c>, <c>FetchPeerSubscriptionsAsync</c>).
    /// Requester side: <see cref="SendRequestAsync"/> sends a <see cref="ClusterRequestEnvelope"/>
    /// to the target's request queue (addressed directly by name) and awaits a matching
    /// <see cref="ClusterResponseEnvelope"/> on this node's own reply queue (polled by
    /// <see cref="AzureServiceBusClusterMessageBus.EnsureInitializedAsync"/>'s background loop).
    /// Answering side: that same initialization also starts the request-queue poller, which delivers
    /// into <see cref="HandleRequestDeliveryAsync"/>, dispatching to <see cref="BuildResponseAsync"/>,
    /// implemented per request kind in <c>AzureServiceBusClusterMessageBus.Snapshots.cs</c> /
    /// <c>.SubscriptionFetch.cs</c>.
    /// </summary>
    internal sealed partial class AzureServiceBusClusterMessageBus
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
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var request = new ClusterRequestEnvelope(Guid.NewGuid(), kind, channelName, channelKey, sinceTicks, _nodeEndpoint);
            var tcs = _pendingRequests.Register(request.CorrelationId);

            try
            {
                var requestQueueName = AzureServiceBusResourceNaming.RequestQueue(_options.ResourcePrefix, targetNodeEndpoint);

                // Only the send itself is retried/circuit-broken — retrying the full
                // send-then-await-reply round trip under the same policy would compound the wait (up
                // to RequestTimeout per attempt), same reasoning as every other broker-style
                // transport in this repo.
                await _resiliencePipeline.ExecuteAsync(async ct =>
                {
                    await GetSender(requestQueueName).SendMessageAsync(new ServiceBusMessage(request.ToNJson()), ct).ConfigureAwait(false);
                }, cancellationToken).ConfigureAwait(false);

                using var timeoutCts = new CancellationTokenSource(_options.RequestTimeout);
                using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);
                await using var registration = linkedCts.Token.Register(() => tcs.TrySetCanceled(linkedCts.Token));

                var response = await tcs.Task.ConfigureAwait(false);
                if (!response.Success)
                    throw new InvalidOperationException($"AzureServiceBus cluster request '{kind}' to '{targetNodeEndpoint.Host}' was rejected: {response.ErrorMessage}");

                return response;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (OperationCanceledException)
            {
                throw new TimeoutException($"AzureServiceBus cluster request '{kind}' to '{targetNodeEndpoint.Host}' timed out after {_options.RequestTimeout}.");
            }
            finally
            {
                _pendingRequests.Remove(request.CorrelationId);
            }
        }

        /// <summary>Internal (rather than private) so tests can drive it directly with a raw message body.</summary>
        internal async Task HandleRequestDeliveryAsync(string body, CancellationToken cancellationToken)
        {
            ClusterRequestEnvelope? request;
            try
            {
                request = body.FromNJson<ClusterRequestEnvelope>();
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
            ClusterResponseEnvelope? response;
            try
            {
                response = body.FromNJson<ClusterResponseEnvelope>();
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
        internal bool TryCompletePendingRequest(ClusterResponseEnvelope response)
        {
            return _pendingRequests.TryComplete(response.CorrelationId, () => response);
        }

        /// <summary>
        /// Answers an inbound request by dispatching to the per-kind builder (implemented in
        /// <c>AzureServiceBusClusterMessageBus.Snapshots.cs</c> / <c>.SubscriptionFetch.cs</c>) and
        /// sends the result to the requester's own reply queue — derived from
        /// <see cref="ClusterRequestEnvelope.ReplyToNodeEndpoint"/> rather than
        /// trusting a queue name supplied on the wire.
        /// </summary>
        internal async Task HandleIncomingRequestAsync(ClusterRequestEnvelope request, CancellationToken cancellationToken)
        {
            var response = await BuildResponseAsync(request, cancellationToken).ConfigureAwait(false);
            var replyQueueName = AzureServiceBusResourceNaming.ReplyQueue(_options.ResourcePrefix, request.ReplyToNodeEndpoint);

            try
            {
                await GetSender(replyQueueName).SendMessageAsync(new ServiceBusMessage(response.ToNJson()), cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Log.ReplySendFailed(_logger, exception, replyQueueName);
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
            [LoggerMessage(EventId = 9830, Level = LogLevel.Warning,
                Message = "[Cluster] AzureServiceBus cluster request could not be parsed; skipping it.")]
            public static partial void RequestUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 9831, Level = LogLevel.Warning,
                Message = "[Cluster] AzureServiceBus cluster response could not be parsed; skipping it.")]
            public static partial void ResponseUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 9832, Level = LogLevel.Warning,
                Message = "[Cluster] Sending an AzureServiceBus cluster reply to queue '{Queue}' failed.")]
            public static partial void ReplySendFailed(ILogger logger, Exception exception, string queue);

            [LoggerMessage(EventId = 9833, Level = LogLevel.Error,
                Message = "[Cluster] AzureServiceBus request handling faulted for kind '{Kind}'.")]
            public static partial void RequestHandlingFaulted(ILogger logger, Exception exception, string kind);
        }
    }
}
