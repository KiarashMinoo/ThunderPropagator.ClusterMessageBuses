using Microsoft.Extensions.Logging;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.Swim
{
    internal sealed partial class SwimClusterMessageBus
    {
        // Unlike every other transport in this repo -- including this project's closest structural
        // relative, the raw-UDP UdpClusterMessageBus -- PublishAsync does not unicast this message to
        // every discovered peer up front. It only stamps OriginId and enqueues the result into this
        // bus's shared application broadcast queue; the message only actually leaves this node the
        // next time the gossip-round loop or the SWIM probe loop happens to piggyback it onto a
        // datagram already headed to some subset of peers. See this class's own remarks (in
        // SwimClusterMessageBus.cs) for the full epidemic-dissemination design and its latency
        // tradeoff versus this repo's other 16 transports.
        public override async Task PublishAsync(Guid channelKey, ClusterFanOutMessage message, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var stamped = message with { OriginId = _selfId };
            var payload = new ClusterChannelEnvelope<ClusterFanOutMessage>(channelKey, stamped);
            _appBroadcastQueue.Enqueue(new SwimDatagram(SwimMessageKind.GossipFanOut, payload.ToNJson()));

            Log.FanOutQueued(_logger, channelKey);
        }

        public override Task<IAsyncDisposable> SubscribeAsync(Guid channelKey, Func<ClusterFanOutMessage, CancellationToken, Task> onMessage, CancellationToken cancellationToken = default)
        {
            return Task.FromResult(_fanOutHandlers.Register(channelKey, onMessage));
        }

        /// <summary>
        /// Handles one <see cref="ClusterChannelEnvelope{TMessage}"/> of <see cref="ClusterFanOutMessage"/>,
        /// whether it arrived as a standalone <see cref="SwimMessageKind.GossipFanOut"/> datagram or
        /// piggybacked inside a Ping/PingReq/Ack. Internal (rather than private) so tests can drive it
        /// directly with a raw payload.
        /// </summary>
        internal async Task HandleFanOutDeliveryAsync(string payload, CancellationToken cancellationToken)
        {
            ClusterChannelEnvelope<ClusterFanOutMessage>? fanOut;
            try
            {
                fanOut = payload.FromNJson<ClusterChannelEnvelope<ClusterFanOutMessage>>();
            }
            catch (Exception exception)
            {
                Log.FanOutMessageUnparseable(_logger, exception);
                return;
            }

            if (fanOut is null || fanOut.Message.OriginId == _selfId)
                return;

            // Keep the epidemic spreading: re-queue for this node's own outgoing gossip rounds so a
            // message doesn't stop propagating after just the hop(s) it took to reach this node.
            _appBroadcastQueue.Enqueue(new SwimDatagram(SwimMessageKind.GossipFanOut, payload));

            if (!_fanOutHandlers.TryGetHandler(fanOut.ChannelKey, out var handler) || handler is null)
                return;

            try
            {
                await handler(fanOut.Message, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Log.FanOutHandlerFaulted(_logger, exception);
            }
        }

        private static partial class Log
        {
            [LoggerMessage(EventId = 91610, Level = LogLevel.Debug,
                Message = "[Cluster] SWIM fan-out message for channel '{ChannelKey}' queued for gossip dissemination.")]
            public static partial void FanOutQueued(ILogger logger, Guid channelKey);

            [LoggerMessage(EventId = 91611, Level = LogLevel.Warning,
                Message = "[Cluster] SWIM fan-out message could not be parsed; skipping it.")]
            public static partial void FanOutMessageUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91612, Level = LogLevel.Error,
                Message = "[Cluster] SWIM fan-out handler faulted.")]
            public static partial void FanOutHandlerFaulted(ILogger logger, Exception exception);
        }
    }
}
