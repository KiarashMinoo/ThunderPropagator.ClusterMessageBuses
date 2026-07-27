using System.Net;
using Microsoft.Extensions.Logging;
using ThunderPropagator.BuildingBlocks.Application.Helpers;

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
        internal async Task<UdpClusterResponseEnvelope> SendRequestAsync(
            Uri targetPeerEndpoint,
            UdpClusterRequestKind kind,
            string? channelName,
            Guid? channelKey,
            long? sinceTicks,
            CancellationToken cancellationToken)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var remoteEndpoint = await _peerEndpointResolver(targetPeerEndpoint, cancellationToken).ConfigureAwait(false);
            var request = new UdpClusterRequestEnvelope(Guid.NewGuid(), kind, channelName, channelKey, sinceTicks);
            var frame = new UdpClusterFrame(UdpClusterFrameKind.Request, request.ToNJson());
            var tcs = new TaskCompletionSource<UdpClusterResponseEnvelope>(TaskCreationOptions.RunContinuationsAsynchronously);

            if (!_pendingRequests.TryAdd(request.CorrelationId, tcs))
                throw new InvalidOperationException($"Duplicate UDP cluster request correlation id '{request.CorrelationId}'.");

            try
            {
                var deadline = DateTime.UtcNow + _options.RequestTimeout;

                while (true)
                {
                    await SendFrameAsync(frame, remoteEndpoint, cancellationToken).ConfigureAwait(false);

                    var remaining = deadline - DateTime.UtcNow;
                    if (remaining <= TimeSpan.Zero)
                        throw new TimeoutException($"UDP cluster request '{kind}' to '{targetPeerEndpoint.Host}' timed out after {_options.RequestTimeout}.");

                    var waitTime = remaining < _options.ResendInterval ? remaining : _options.ResendInterval;
                    using var waitCts = new CancellationTokenSource(waitTime);
                    using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, waitCts.Token);

                    var delayTask = Task.Delay(Timeout.InfiniteTimeSpan, linkedCts.Token);
                    var completed = await Task.WhenAny(tcs.Task, delayTask).ConfigureAwait(false);

                    if (completed == tcs.Task)
                    {
                        var response = await tcs.Task.ConfigureAwait(false);
                        if (!response.Success)
                            throw new InvalidOperationException($"UDP cluster request '{kind}' to '{targetPeerEndpoint.Host}' was rejected: {response.ErrorMessage}");

                        return response;
                    }

                    // The delay "won" the race — either this attempt's resend interval elapsed (loop
                    // around and resend) or the caller's own cancellationToken fired (propagate
                    // immediately rather than resending forever).
                    cancellationToken.ThrowIfCancellationRequested();
                }
            }
            finally
            {
                _pendingRequests.TryRemove(request.CorrelationId, out _);
            }
        }

        /// <summary>
        /// Answers an inbound request datagram by dispatching to <see cref="BuildResponseAsync"/> and
        /// invoking <paramref name="sendResponseAsync"/> with the resulting <see cref="UdpClusterFrame"/>
        /// and the <paramref name="remoteEndpoint"/> the request actually arrived from. Internal
        /// (rather than private) so tests can drive it directly with a raw payload and a capturing
        /// delegate instead of a real socket.
        /// </summary>
        internal async Task HandleRequestFrameAsync(
            string payload,
            IPEndPoint remoteEndpoint,
            Func<UdpClusterFrame, IPEndPoint, CancellationToken, Task> sendResponseAsync,
            CancellationToken cancellationToken)
        {
            UdpClusterRequestEnvelope? request;
            try
            {
                request = payload.FromNJson<UdpClusterRequestEnvelope>();
            }
            catch (Exception exception)
            {
                Log.RequestUnparseable(_logger, exception);
                return;
            }

            if (request is null)
                return;

            var response = await BuildResponseAsync(request, cancellationToken).ConfigureAwait(false);
            var responseFrame = new UdpClusterFrame(UdpClusterFrameKind.Response, response.ToNJson());

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
            UdpClusterResponseEnvelope? response;
            try
            {
                response = payload.FromNJson<UdpClusterResponseEnvelope>();
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
        internal bool TryCompletePendingRequest(UdpClusterResponseEnvelope response)
        {
            if (!_pendingRequests.TryRemove(response.CorrelationId, out var pending))
                return false;

            return pending.TrySetResult(response);
        }

        internal async Task<UdpClusterResponseEnvelope> BuildResponseAsync(UdpClusterRequestEnvelope request, CancellationToken cancellationToken)
        {
            try
            {
                return request.Kind switch
                {
                    UdpClusterRequestKind.RestoreSnapshot => await BuildRestoreSnapshotResponseAsync(request, cancellationToken).ConfigureAwait(false),
                    UdpClusterRequestKind.SyncDelta => await BuildSyncDeltaResponseAsync(request, cancellationToken).ConfigureAwait(false),
                    UdpClusterRequestKind.FetchSubscriptions => BuildFetchSubscriptionsResponse(request),
                    _ => new UdpClusterResponseEnvelope(request.CorrelationId, false, $"Unknown request kind '{request.Kind}'.", null)
                };
            }
            catch (Exception exception)
            {
                Log.RequestHandlingFaulted(_logger, exception, request.Kind.ToString());
                return new UdpClusterResponseEnvelope(request.CorrelationId, false, exception.Message, null);
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
