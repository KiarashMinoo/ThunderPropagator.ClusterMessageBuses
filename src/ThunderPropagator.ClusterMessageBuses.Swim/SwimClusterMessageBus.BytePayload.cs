using Microsoft.Extensions.Logging;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.Swim
{
    /// <summary>
    /// Byte-oriented counterpart to <c>SwimClusterMessageBus.FanOut.cs</c> -- see
    /// <see cref="ClusterByteMessage"/>'s own doc comment for why this surface exists alongside, not
    /// instead of, the <see cref="ClusterFanOutMessage"/>-based one. Fan-out is queued for gossip
    /// dissemination exactly like the <see cref="ClusterFanOutMessage"/> overload; the snapshot pull
    /// reuses the existing hand-rolled, resend-on-timer direct-unicast request/reply plumbing in
    /// <c>SwimClusterMessageBus.RequestReply.cs</c> via <see cref="ClusterRequestKind.PullSnapshotBytes"/>,
    /// exactly like <c>RestoreFromLeaderAsync</c> reuses it for <see cref="ClusterRequestKind.RestoreSnapshot"/>.
    /// </summary>
    internal sealed partial class SwimClusterMessageBus
    {
        /// <inheritdoc />
        public override async Task PublishAsync(Guid channelKey, ClusterByteMessage message, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var stamped = message with { OriginId = _selfId };
            var payload = new ClusterChannelEnvelope<ClusterByteMessage>(channelKey, stamped);
            _appBroadcastQueue.Enqueue(new SwimDatagram(SwimMessageKind.GossipByteFanOut, payload.ToNJson()));

            Log.ByteFanOutQueued(_logger, channelKey);
        }

        /// <inheritdoc />
        public override Task<IAsyncDisposable> SubscribeAsync(Guid channelKey, Func<ClusterByteMessage, CancellationToken, Task> onMessage, CancellationToken cancellationToken = default)
        {
            return Task.FromResult(_byteFanOutHandlers.Register(channelKey, onMessage));
        }

        /// <summary>
        /// Handles one <see cref="ClusterChannelEnvelope{TMessage}"/> of <see cref="ClusterByteMessage"/>,
        /// whether it arrived as a standalone <see cref="SwimMessageKind.GossipByteFanOut"/> datagram
        /// or piggybacked inside a Ping/PingReq/Ack. Internal (rather than private) so tests can drive
        /// it directly with a raw payload.
        /// </summary>
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

            // Keep the epidemic spreading -- see HandleFanOutDeliveryAsync's identical comment.
            _appBroadcastQueue.Enqueue(new SwimDatagram(SwimMessageKind.GossipByteFanOut, payload));

            if (!_byteFanOutHandlers.TryGetHandler(byteFanOut.ChannelKey, out var handler) || handler is null)
                return;

            try
            {
                await handler(byteFanOut.Message, cancellationToken).ConfigureAwait(false);
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
            [LoggerMessage(EventId = 91660, Level = LogLevel.Debug,
                Message = "[Cluster] SWIM byte fan-out message for channel '{ChannelKey}' queued for gossip dissemination.")]
            public static partial void ByteFanOutQueued(ILogger logger, Guid channelKey);

            [LoggerMessage(EventId = 91661, Level = LogLevel.Warning,
                Message = "[Cluster] SWIM byte fan-out message could not be parsed; skipping it.")]
            public static partial void ByteFanOutMessageUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91662, Level = LogLevel.Error,
                Message = "[Cluster] SWIM byte fan-out handler faulted.")]
            public static partial void ByteFanOutHandlerFaulted(ILogger logger, Exception exception);
        }
    }
}
