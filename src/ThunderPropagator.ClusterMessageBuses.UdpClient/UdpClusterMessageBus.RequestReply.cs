using System.Net;
using Microsoft.Extensions.Logging;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.UdpClient
{
    /// <summary>
    /// Shared request/reply plumbing for the three leader/peer-pull operations
    /// (<c>RestoreFromLeaderAsync</c>, <c>SyncDeltaFromLeaderAsync</c>, <c>FetchPeerSubscriptionsAsync</c>).
    /// </summary>
    /// <remarks>
    /// Unlike every other transport in this repo, <see cref="SendRequestAsync"/> does not use
    /// <see cref="ThunderPropagator.ClusterMessageBuses.SharedKernel.ClusterResiliencePipelineFactory"/>
    /// — that factory's retry/circuit-breaker policy exists to protect against a transient failure
    /// of a single send call, which assumes the underlying transport otherwise guarantees delivery of
    /// whatever it did manage to send. Plain UDP gives no such guarantee: a "successfully sent"
    /// datagram can still be silently dropped anywhere on the network path. So this transport instead
    /// resends the whole request datagram on its own timer
    /// (<see cref="UdpClusterMessageBusOptions.ResendInterval"/>) for as long as
    /// <see cref="UdpClusterMessageBusOptions.RequestTimeout"/> has not yet elapsed, racing each
    /// resend against the same pending <see cref="TaskCompletionSource{TResult}"/> rather than
    /// creating a new one per attempt (a matching response completes whichever attempt is currently
    /// waiting).
    /// </remarks>
    internal sealed partial class UdpClusterMessageBus
    {
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

            var remoteEndpoint = await _peerEndpointResolver(targetPeerEndpoint, cancellationToken).ConfigureAwait(false);
            var request = new ClusterRequestEnvelope(Guid.NewGuid(), kind, channelName, channelKey, sinceTicks);
            var frame = new ClusterFrame(ClusterFrameKind.Request, request.ToNJson());
            var tcs = _pendingRequests.Register(request.CorrelationId);

            try
            {
                var response = await ResendOnTimerRequestCoordinator.SendWithResendAsync(
                    tcs,
                    ct => SendFrameAsync(frame, remoteEndpoint, ct),
                    _options.ResendInterval,
                    _options.RequestTimeout,
                    $"UDP cluster request '{kind}' to '{targetPeerEndpoint.Host}' timed out after {_options.RequestTimeout}.",
                    cancellationToken).ConfigureAwait(false);

                if (!response.Success)
                    throw new InvalidOperationException($"UDP cluster request '{kind}' to '{targetPeerEndpoint.Host}' was rejected: {response.ErrorMessage}");

                return response;
            }
            finally
            {
                _pendingRequests.Remove(request.CorrelationId);
            }
        }

        /// <summary>
        /// Answers an inbound request datagram by dispatching to <see cref="BuildResponseAsync"/> and
        /// invoking <paramref name="sendResponseAsync"/> with the resulting <see cref="ClusterFrame"/>
        /// and the <paramref name="remoteEndpoint"/> the request actually arrived from. Internal
        /// (rather than private) so tests can drive it directly with a raw payload and a capturing
        /// delegate instead of a real socket.
        /// </summary>
        internal async Task HandleRequestFrameAsync(
            string payload,
            IPEndPoint remoteEndpoint,
            Func<ClusterFrame, IPEndPoint, CancellationToken, Task> sendResponseAsync,
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
                await sendResponseAsync(responseFrame, remoteEndpoint, cancellationToken).ConfigureAwait(false);
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
        /// <paramref name="response"/>'s correlation id, if one is still waiting. A retried request
        /// may be answered more than once (e.g. a resend crosses in flight with a delayed original
        /// response) — only the first matching response to arrive completes the pending call; later
        /// duplicates simply find no pending entry left and are dropped.
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
            [LoggerMessage(EventId = 91530, Level = LogLevel.Warning,
                Message = "[Cluster] UDP cluster request could not be parsed; skipping it.")]
            public static partial void RequestUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91531, Level = LogLevel.Warning,
                Message = "[Cluster] UDP cluster response could not be parsed; skipping it.")]
            public static partial void ResponseUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91532, Level = LogLevel.Warning,
                Message = "[Cluster] Sending a UDP cluster response back to the requester failed.")]
            public static partial void ResponseSendFailed(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91533, Level = LogLevel.Error,
                Message = "[Cluster] UDP request handling faulted for kind '{Kind}'.")]
            public static partial void RequestHandlingFaulted(ILogger logger, Exception exception, string kind);
        }
    }
}
