using Microsoft.Extensions.Logging;
using Polly;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.Srd
{
    /// <summary>
    /// Shared request/reply plumbing for the leader/peer-pull operations (<c>RestoreFromLeaderAsync</c>,
    /// <c>SyncDeltaFromLeaderAsync</c>, <c>FetchPeerSubscriptionsAsync</c>, <c>PullSnapshotAsync</c>).
    /// </summary>
    /// <remarks>
    /// Unlike <c>UdpClusterMessageBus</c>, <see cref="SendRequestAsync"/> does not resend the whole
    /// request on its own timer -- EFA's SRD transport is reliable (unlike plain UDP), so only the
    /// initial send is wrapped in
    /// <see cref="ThunderPropagator.ClusterMessageBuses.SharedKernel.ClusterResiliencePipelineFactory"/>'s
    /// retry/circuit-breaker policy, exactly like <c>TcpClusterMessageBus</c>/<c>WebSocketClusterMessageBus</c>
    /// do for their own reliable, connection-oriented transports. Retrying the full
    /// send-then-await-reply round trip under the same policy would compound the wait (up to
    /// <see cref="SrdClusterMessageBusOptions.RequestTimeout"/> per attempt) -- same reasoning as every
    /// other transport here.
    /// </remarks>
    internal sealed partial class SrdClusterMessageBus
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

            // ReplyToNodeEndpoint is always populated (unlike broker-based transports' optional use
            // of it): this project's native-interop scope has no way to recover a completion's
            // source address the way a normal socket accept/recvfrom would (see
            // Wire/SrdClusterFrame.cs's remarks), so the answering side needs this node's own
            // identity carried in the request payload to know where to reply.
            var request = new ClusterRequestEnvelope(Guid.NewGuid(), kind, channelName, channelKey, sinceTicks, _nodeEndpoint);
            var tcs = _pendingRequests.Register(request.CorrelationId);

            try
            {
                var frame = new SrdClusterFrame(SrdClusterFrameKind.Request, request.ToNJson());

                await _resiliencePipeline.ExecuteAsync(
                    async ct => await _endpoint!.SendFrameAsync(frame, targetPeerEndpoint, ct).ConfigureAwait(false),
                    cancellationToken).ConfigureAwait(false);

                using var timeoutCts = new CancellationTokenSource(_options.RequestTimeout);
                using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);
                using var registration = linkedCts.Token.Register(() => tcs.TrySetCanceled(linkedCts.Token));

                var response = await tcs.Task.ConfigureAwait(false);
                if (!response.Success)
                    throw new InvalidOperationException($"SRD cluster request '{kind}' to '{targetPeerEndpoint.Host}' was rejected: {response.ErrorMessage}");

                return response;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (OperationCanceledException)
            {
                throw new TimeoutException($"SRD cluster request '{kind}' to '{targetPeerEndpoint.Host}' timed out after {_options.RequestTimeout}.");
            }
            finally
            {
                _pendingRequests.Remove(request.CorrelationId);
            }
        }

        /// <summary>
        /// Answers an inbound request frame by dispatching to <see cref="BuildResponseAsync"/> and
        /// invoking <paramref name="sendResponseAsync"/> with the resulting <see cref="SrdClusterFrame"/>
        /// and the request's own embedded
        /// <see cref="ThunderPropagator.ClusterMessageBuses.SharedKernel.ClusterRequestEnvelope.ReplyToNodeEndpoint"/>
        /// (populated by <see cref="SendRequestAsync"/> with this node's own identity -- see that
        /// call's own remarks for why, unlike the UDP/TCP transports, this transport needs it
        /// carried in the payload at all). Internal (rather than private) so tests can drive it
        /// directly with a raw payload and a capturing delegate instead of a real endpoint.
        /// </summary>
        internal async Task HandleRequestFrameAsync(
            string payload,
            Func<SrdClusterFrame, Uri, CancellationToken, Task> sendResponseAsync,
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
            var responseFrame = new SrdClusterFrame(SrdClusterFrameKind.Response, response.ToNJson());

            try
            {
                // Every request this transport sends (see SendRequestAsync above) always populates
                // ReplyToNodeEndpoint, so it is safe to assert non-null here.
                await sendResponseAsync(responseFrame, request.ReplyToNodeEndpoint!, cancellationToken).ConfigureAwait(false);
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
        internal bool TryCompletePendingRequest(ClusterResponseEnvelope response) =>
            _pendingRequests.TryComplete(response.CorrelationId, () => response);

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
            [LoggerMessage(EventId = 91830, Level = LogLevel.Warning,
                Message = "[Cluster] SRD cluster request could not be parsed; skipping it.")]
            public static partial void RequestUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91831, Level = LogLevel.Warning,
                Message = "[Cluster] SRD cluster response could not be parsed; skipping it.")]
            public static partial void ResponseUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91832, Level = LogLevel.Warning,
                Message = "[Cluster] Sending an SRD cluster response back to the requester failed.")]
            public static partial void ResponseSendFailed(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91833, Level = LogLevel.Error,
                Message = "[Cluster] SRD request handling faulted for kind '{Kind}'.")]
            public static partial void RequestHandlingFaulted(ILogger logger, Exception exception, string kind);
        }
    }
}
