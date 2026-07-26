using Microsoft.Extensions.Logging;
using Polly;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.WebSocket
{
    /// <summary>
    /// Shared request/reply plumbing for the three leader/peer-pull operations
    /// (<c>RestoreFromLeaderAsync</c>, <c>SyncDeltaFromLeaderAsync</c>, <c>FetchPeerSubscriptionsAsync</c>).
    /// Requester side: <see cref="SendRequestAsync"/> sends a <see cref="WebSocketClusterRequestEnvelope"/>
    /// over this node's own outbound connection to the target peer and awaits a matching
    /// <see cref="WebSocketClusterResponseEnvelope"/> — which, unlike every broker transport in this
    /// repo, arrives back over that exact same connection rather than a separate reply channel (see
    /// <see cref="WebSocketClusterRequestEnvelope"/>'s remarks). Answering side:
    /// <see cref="HandleRequestFrameAsync"/> dispatches to <see cref="BuildResponseAsync"/>,
    /// implemented per request kind in <c>WebSocketClusterMessageBus.Snapshots.cs</c> /
    /// <c>.SubscriptionFetch.cs</c>, then writes the response back via the caller-supplied
    /// <c>sendResponseAsync</c> delegate — decoupled from the concrete connection type so tests can
    /// exercise it with a simple capturing lambda instead of a real socket.
    /// </summary>
    internal sealed partial class WebSocketClusterMessageBus
    {
        private readonly ResiliencePipeline _resiliencePipeline = ClusterResiliencePipelineFactory.Create();

        /// <summary>Internal (rather than private) so tests can exercise the requester side directly.</summary>
        internal async Task<WebSocketClusterResponseEnvelope> SendRequestAsync(
            Uri targetPeerEndpoint,
            WebSocketClusterRequestKind kind,
            string? channelName,
            Guid? channelKey,
            long? sinceTicks,
            CancellationToken cancellationToken)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var request = new WebSocketClusterRequestEnvelope(Guid.NewGuid(), kind, channelName, channelKey, sinceTicks);
            var tcs = new TaskCompletionSource<WebSocketClusterResponseEnvelope>(TaskCreationOptions.RunContinuationsAsynchronously);

            if (!_pendingRequests.TryAdd(request.CorrelationId, tcs))
                throw new InvalidOperationException($"Duplicate WebSocket cluster request correlation id '{request.CorrelationId}'.");

            try
            {
                var connection = await GetOrCreateOutboundConnectionAsync(targetPeerEndpoint, cancellationToken).ConfigureAwait(false);
                var frame = new WebSocketClusterFrame(WebSocketClusterFrameKind.Request, request.ToNJson());

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
                    throw new InvalidOperationException($"WebSocket cluster request '{kind}' to '{targetPeerEndpoint.Host}' was rejected: {response.ErrorMessage}");

                return response;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (OperationCanceledException)
            {
                throw new TimeoutException($"WebSocket cluster request '{kind}' to '{targetPeerEndpoint.Host}' timed out after {_options.RequestTimeout}.");
            }
            finally
            {
                _pendingRequests.TryRemove(request.CorrelationId, out _);
            }
        }

        /// <summary>
        /// Answers an inbound request frame by dispatching to <see cref="BuildResponseAsync"/> and
        /// invoking <paramref name="sendResponseAsync"/> with the resulting
        /// <see cref="WebSocketClusterFrame"/>. Internal (rather than private) so tests can drive it
        /// directly with a raw payload and a capturing delegate instead of a real connection.
        /// </summary>
        internal async Task HandleRequestFrameAsync(
            string payload,
            Func<WebSocketClusterFrame, CancellationToken, Task> sendResponseAsync,
            CancellationToken cancellationToken)
        {
            WebSocketClusterRequestEnvelope? request;
            try
            {
                request = payload.FromNJson<WebSocketClusterRequestEnvelope>();
            }
            catch (Exception exception)
            {
                Log.RequestUnparseable(_logger, exception);
                return;
            }

            if (request is null)
                return;

            var response = await BuildResponseAsync(request, cancellationToken).ConfigureAwait(false);
            var responseFrame = new WebSocketClusterFrame(WebSocketClusterFrameKind.Response, response.ToNJson());

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
            WebSocketClusterResponseEnvelope? response;
            try
            {
                response = payload.FromNJson<WebSocketClusterResponseEnvelope>();
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
        internal bool TryCompletePendingRequest(WebSocketClusterResponseEnvelope response)
        {
            if (!_pendingRequests.TryRemove(response.CorrelationId, out var pending))
                return false;

            return pending.TrySetResult(response);
        }

        internal async Task<WebSocketClusterResponseEnvelope> BuildResponseAsync(WebSocketClusterRequestEnvelope request, CancellationToken cancellationToken)
        {
            try
            {
                return request.Kind switch
                {
                    WebSocketClusterRequestKind.RestoreSnapshot => await BuildRestoreSnapshotResponseAsync(request, cancellationToken).ConfigureAwait(false),
                    WebSocketClusterRequestKind.SyncDelta => await BuildSyncDeltaResponseAsync(request, cancellationToken).ConfigureAwait(false),
                    WebSocketClusterRequestKind.FetchSubscriptions => BuildFetchSubscriptionsResponse(request),
                    _ => new WebSocketClusterResponseEnvelope(request.CorrelationId, false, $"Unknown request kind '{request.Kind}'.", null)
                };
            }
            catch (Exception exception)
            {
                Log.RequestHandlingFaulted(_logger, exception, request.Kind.ToString());
                return new WebSocketClusterResponseEnvelope(request.CorrelationId, false, exception.Message, null);
            }
        }

        private static partial class Log
        {
            [LoggerMessage(EventId = 91230, Level = LogLevel.Warning,
                Message = "[Cluster] WebSocket cluster request could not be parsed; skipping it.")]
            public static partial void RequestUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91231, Level = LogLevel.Warning,
                Message = "[Cluster] WebSocket cluster response could not be parsed; skipping it.")]
            public static partial void ResponseUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91232, Level = LogLevel.Warning,
                Message = "[Cluster] Sending a WebSocket cluster response back to the requester failed.")]
            public static partial void ResponseSendFailed(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91233, Level = LogLevel.Error,
                Message = "[Cluster] WebSocket request handling faulted for kind '{Kind}'.")]
            public static partial void RequestHandlingFaulted(ILogger logger, Exception exception, string kind);
        }
    }
}
