using Microsoft.Extensions.Logging;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.ZeroMQ
{
    internal sealed partial class ZeroMqClusterMessageBus
    {
        /// <summary>
        /// Shared plumbing for the three leader/peer-pull operations: sends a
        /// <see cref="ClusterRequestKind"/> request to <paramref name="targetPeerEndpoint"/> over
        /// its cached DEALER connection, and awaits the matching reply by correlation id — ROUTER/
        /// DEALER has no native request/reply, so this hand-rolled correlation-id scheme is required
        /// (the same reasoning as the broker transports and WebSocketClusterMessageBus elsewhere in
        /// this repo).
        /// </summary>
        private async Task<ClusterResponseEnvelope> SendRequestAsync(
            Uri targetPeerEndpoint,
            ClusterRequestKind kind,
            string? channelName,
            Guid? channelKey,
            long? sinceTicks,
            CancellationToken cancellationToken)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var request = new ClusterRequestEnvelope(Guid.NewGuid(), kind, channelName, channelKey, sinceTicks);
            var frame = new ClusterFrame(ClusterFrameKind.Request, request.ToNJson());

            var tcs = _pendingRequests.Register(request.CorrelationId);

            try
            {
                var connection = await GetOrCreateOutboundConnectionAsync(targetPeerEndpoint, cancellationToken).ConfigureAwait(false);
                await _resiliencePipeline.ExecuteAsync(
                    _ => { connection.SendFrame(frame); return ValueTask.CompletedTask; },
                    cancellationToken).ConfigureAwait(false);

                using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                timeoutCts.CancelAfter(_options.RequestTimeout);

                await using var registration = timeoutCts.Token.Register(() =>
                {
                    if (cancellationToken.IsCancellationRequested)
                        tcs.TrySetCanceled(cancellationToken);
                    else
                        tcs.TrySetException(new TimeoutException(
                            $"No response arrived from '{targetPeerEndpoint.Host}' within {_options.RequestTimeout}."));
                });

                var response = await tcs.Task.ConfigureAwait(false);

                if (!response.Success)
                    throw new InvalidOperationException(response.ErrorMessage);

                return response;
            }
            finally
            {
                _pendingRequests.Remove(request.CorrelationId);
            }
        }

        /// <summary>Answering side: parses the request, builds a response via the request-kind-specific builder, and replies over the same connection the request arrived on.</summary>
        private async Task HandleRequestFrameAsync(byte[] identity, string payloadJson, CancellationToken cancellationToken)
        {
            ClusterRequestEnvelope? request;
            try
            {
                request = payloadJson.FromNJson<ClusterRequestEnvelope>();
            }
            catch (Exception exception)
            {
                Log.RequestUnparseable(_logger, exception);
                return;
            }

            if (request is null)
                return;

            var response = await BuildResponseAsync(request, cancellationToken).ConfigureAwait(false);
            var responseFrame = new ClusterFrame(ClusterFrameKind.Response, response.ToNJson());

            _host!.SendReply(identity, responseFrame);
        }

        /// <summary>Internal (rather than private) so tests can drive it directly against a substitute <c>IClusterChannelResolver</c>.</summary>
        internal Task<ClusterResponseEnvelope> BuildResponseAsync(ClusterRequestEnvelope request, CancellationToken cancellationToken) =>
            request.Kind switch
            {
                ClusterRequestKind.RestoreSnapshot => BuildRestoreSnapshotResponseAsync(request, cancellationToken),
                ClusterRequestKind.SyncDelta => BuildSyncDeltaResponseAsync(request, cancellationToken),
                ClusterRequestKind.FetchSubscriptions => BuildFetchSubscriptionsResponseAsync(request, cancellationToken),
                ClusterRequestKind.PullSnapshotBytes => BuildPullSnapshotBytesResponseAsync(request, cancellationToken),
                _ => Task.FromResult(new ClusterResponseEnvelope(request.CorrelationId, false, $"Unknown request kind '{request.Kind}'.", null)),
            };

        /// <summary>Internal (rather than private) so tests can drive it directly.</summary>
        internal bool TryCompletePendingRequest(ClusterResponseEnvelope response) =>
            _pendingRequests.TryComplete(response.CorrelationId, () => response);

        private static partial class Log
        {
            [LoggerMessage(EventId = 91530, Level = LogLevel.Warning,
                Message = "[Cluster] ZeroMQ request frame could not be parsed; skipping it.")]
            public static partial void RequestUnparseable(ILogger logger, Exception exception);
        }
    }
}
