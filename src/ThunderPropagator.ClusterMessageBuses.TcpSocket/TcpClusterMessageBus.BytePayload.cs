using Microsoft.Extensions.Logging;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.TcpSocket
{
    /// <summary>
    /// Byte-oriented counterpart to <c>TcpClusterMessageBus.FanOut.cs</c> /
    /// <c>.RequestReply.cs</c> -- see <see cref="ClusterByteMessage"/>'s own doc comment for why
    /// this surface exists alongside, not instead of, the <c>ClusterFanOutMessage</c>-based one.
    /// Fan-out is multiplexed over the same physical connection as every other concern, using the
    /// <see cref="ClusterFrameKind.ByteFanOut"/> discriminator and the shared
    /// <see cref="ClusterChannelEnvelope{TMessage}"/> wrapper -- mirroring how it wraps the
    /// channel key alongside a <c>ClusterFanOutMessage</c>. The snapshot pull reuses the existing
    /// hand-rolled request/reply plumbing in <c>TcpClusterMessageBus.RequestReply.cs</c> via a new
    /// <see cref="ClusterRequestKind.PullSnapshotBytes"/> kind, exactly like
    /// <c>RestoreFromLeaderAsync</c> reuses it for <see cref="ClusterRequestKind.RestoreSnapshot"/>.
    /// </summary>
    internal sealed partial class TcpClusterMessageBus
    {
        /// <inheritdoc />
        public override async Task PublishAsync(Guid channelKey, ClusterByteMessage message, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var stamped = message with { OriginId = _selfId };
            var frame = new ClusterFrame(ClusterFrameKind.ByteFanOut, new ClusterChannelEnvelope<ClusterByteMessage>(channelKey, stamped).ToNJson());

            var peers = await _discovery.GetPeersAsync(cancellationToken).ConfigureAwait(false);

            await Task.WhenAll(peers.Select(async peer =>
            {
                try
                {
                    var connection = await GetOrCreateOutboundConnectionAsync(peer.Endpoint, cancellationToken).ConfigureAwait(false);
                    await connection.SendFrameAsync(frame, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    // One unreachable peer must not fail the whole fan-out -- same rationale as the
                    // ClusterFanOutMessage overload in TcpClusterMessageBus.FanOut.cs.
                    Log.ByteFanOutSendToPeerFailed(_logger, exception, peer.Endpoint.Host);
                }
            })).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public override Task<IAsyncDisposable> SubscribeAsync(Guid channelKey, Func<ClusterByteMessage, CancellationToken, Task> onMessage, CancellationToken cancellationToken = default)
        {
            return Task.FromResult(_byteFanOutHandlers.Register(channelKey, onMessage));
        }

        /// <summary>Internal (rather than private) so tests can drive it directly with a raw payload.</summary>
        internal async Task HandleByteFanOutDeliveryAsync(string payload, CancellationToken cancellationToken)
        {
            ClusterChannelEnvelope<ClusterByteMessage>? byteFanOut;
            try
            {
                byteFanOut = payload.FromNJson<ClusterChannelEnvelope<ClusterByteMessage>>();
            }
            catch (Exception exception)
            {
                Log.ByteFanOutMessageUnparseable(_logger, exception);
                return;
            }

            if (byteFanOut is null || byteFanOut.Message.OriginId == _selfId)
                return;

            try
            {
                await _byteFanOutHandlers.InvokeAsync(byteFanOut.ChannelKey, byteFanOut.Message, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Log.ByteFanOutHandlerFaulted(_logger, exception);
            }
        }

        /// <inheritdoc />
        public override async Task<byte[]?> PullSnapshotAsync(Uri leaderEndpoint, Guid channelKey, CancellationToken cancellationToken = default)
        {
            var response = await SendRequestAsync(
                leaderEndpoint, ClusterRequestKind.PullSnapshotBytes, channelName: null, channelKey, sinceTicks: null, cancellationToken)
                .ConfigureAwait(false);

            return response.PayloadJson?.FromNJson<byte[]?>();
        }

        /// <summary>
        /// Answering side of <see cref="PullSnapshotAsync"/>. Returns a successful response with a
        /// JSON-null payload when no <see cref="IClusterByteSnapshotProvider"/> is registered, or it
        /// has nothing to offer yet -- mirrored by <see cref="PullSnapshotAsync"/> deserializing that
        /// back to a null byte[], exactly like <c>BuildRestoreSnapshotResponseAsync</c> answers with
        /// an empty array rather than a failure when a channel has nothing to restore.
        /// </summary>
        private async Task<ClusterResponseEnvelope> BuildPullSnapshotBytesResponseAsync(ClusterRequestEnvelope request, CancellationToken cancellationToken)
        {
            if (_byteSnapshotProvider is null)
                return new ClusterResponseEnvelope(request.CorrelationId, true, null, "null");

            var snapshot = await _byteSnapshotProvider.GetSnapshotAsync(request.ChannelKey!.Value, cancellationToken).ConfigureAwait(false);
            return new ClusterResponseEnvelope(request.CorrelationId, true, null, snapshot.ToNJson());
        }

        private static partial class Log
        {
            [LoggerMessage(EventId = 91460, Level = LogLevel.Warning,
                Message = "[Cluster] TCP byte fan-out send to peer '{Host}' failed.")]
            public static partial void ByteFanOutSendToPeerFailed(ILogger logger, Exception exception, string host);

            [LoggerMessage(EventId = 91461, Level = LogLevel.Warning,
                Message = "[Cluster] TCP byte fan-out message could not be parsed; skipping it.")]
            public static partial void ByteFanOutMessageUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91462, Level = LogLevel.Error,
                Message = "[Cluster] TCP byte fan-out handler faulted.")]
            public static partial void ByteFanOutHandlerFaulted(ILogger logger, Exception exception);
        }
    }
}
