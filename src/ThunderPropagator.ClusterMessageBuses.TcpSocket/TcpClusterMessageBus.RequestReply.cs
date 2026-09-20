using Microsoft.Extensions.Logging;
using Polly;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.TcpSocket
{
    /// <summary>
    /// Shared request/reply plumbing for the three leader/peer-pull operations
    /// (<c>RestoreFromLeaderAsync</c>, <c>SyncDeltaFromLeaderAsync</c>, <c>FetchPeerSubscriptionsAsync</c>).
    /// Requester side: <see cref="SendRequestAsync"/> sends a <see cref="ClusterRequestEnvelope"/>
    /// over this node's own outbound connection to the target peer and awaits a matching
    /// <see cref="ClusterResponseEnvelope"/> — which arrives back over that exact same connection
    /// rather than a separate reply channel (see <see cref="ClusterRequestEnvelope"/>'s remarks).
    /// Answering side: <see cref="HandleRequestFrameAsync"/> dispatches to <see cref="BuildResponseAsync"/>,
    /// implemented per request kind in <c>TcpClusterMessageBus.Snapshots.cs</c> /
    /// <c>.SubscriptionFetch.cs</c>, then writes the response back via the caller-supplied
    /// <c>sendResponseAsync</c> delegate — decoupled from the concrete connection type so tests can
    /// exercise it with a simple capturing lambda instead of a real socket.
    /// </summary>
    internal sealed partial class TcpClusterMessageBus
    {
        private readonly ResiliencePipeline _resiliencePipeline = ClusterResiliencePipelineFactory.Create();

        /// <summary>Internal (rather than private) so tests can exercise the requester side directly.</summary>
        internal async Task<ClusterResponseEnvelope> SendRequestAsync(
            Uri targetPeerEndpoint,
            ClusterRequestKind kind,
            string? channelName,
            Guid? channelKey,
            long? sinceTicks,
            CancellationToken cancellationToken)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var request = new ClusterRequestEnvelope(Guid.NewGuid(), kind, channelName, channelKey, sinceTicks);
            var tcs = _pendingRequests.Register(request.CorrelationId);

            try
            {
                var connection = await GetOrCreateOutboundConnectionAsync(targetPeerEndpoint, cancellationToken).ConfigureAwait(false);
                var frame = new ClusterFrame(ClusterFrameKind.Request, request.ToNJson());

                // Only the send itself is retried/circuit-broken — retrying the full
                // send-then-await-reply round trip under the same policy would compound the wait
                // (up to RequestTimeout per attempt), same reasoning as every other transport here.
                await _resiliencePipeline.ExecuteAsync(
                    async ct => await connection.SendFrameAsync(frame, ct).ConfigureAwait(false),
                    cancellationToken).ConfigureAwait(false);

                using var timeoutCts = new CancellationTokenSource(_options.RequestTimeout);
                using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);
                await using var registration = linkedCts.Token.Register(() => tcs.TrySetCanceled(linkedCts.Token));

                var response = await tcs.Task.ConfigureAwait(false);
                if (!response.Success)
                    throw new InvalidOperationException($"TCP cluster request '{kind}' to '{targetPeerEndpoint.Host}' was rejected: {response.ErrorMessage}");

                return response;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (OperationCanceledException)
            {
                throw new TimeoutException($"TCP cluster request '{kind}' to '{targetPeerEndpoint.Host}' timed out after {_options.RequestTimeout}.");
            }
            finally
            {
                _pendingRequests.Remove(request.CorrelationId);
            }
        }

        /// <summary>
        /// Answers an inbound request frame by dispatching to <see cref="BuildResponseAsync"/> and
        /// invoking <paramref name="sendResponseAsync"/> with the resulting <see cref="ClusterFrame"/>.
        /// Internal (rather than private) so tests can drive it directly with a raw payload and a
        /// capturing delegate instead of a real connection.
        /// </summary>
        internal async Task HandleRequestFrameAsync(
            string payload,
            Func<ClusterFrame, CancellationToken, Task> sendResponseAsync,
            CancellationToken cancellationToken)
        {
            ClusterRequestEnvelope? request;
            try
            {
                request = payload.FromNJson<ClusterRequestEnvelope>();
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

            try
            {
                await sendResponseAsync(responseFrame, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Log.ResponseSendFailed(_logger, exception);
            }
        }

        /// <summary>Internal (rather than private) so tests can drive it directly with a raw payload.</summary>
        internal Task HandleResponseDeliveryAsync(string payload, CancellationToken cancellationToken)
        {
            ClusterResponseEnvelope? response;
            try
            {
                response = payload.FromNJson<ClusterResponseEnvelope>();
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
            [LoggerMessage(EventId = 91430, Level = LogLevel.Warning,
                Message = "[Cluster] TCP cluster request could not be parsed; skipping it.")]
            public static partial void RequestUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91431, Level = LogLevel.Warning,
                Message = "[Cluster] TCP cluster response could not be parsed; skipping it.")]
            public static partial void ResponseUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91432, Level = LogLevel.Warning,
                Message = "[Cluster] Sending a TCP cluster response back to the requester failed.")]
            public static partial void ResponseSendFailed(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91433, Level = LogLevel.Error,
                Message = "[Cluster] TCP request handling faulted for kind '{Kind}'.")]
            public static partial void RequestHandlingFaulted(ILogger logger, Exception exception, string kind);
        }
    }
}
