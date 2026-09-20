using Microsoft.Extensions.Logging;
using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.Swim
{
    internal sealed partial class SwimClusterMessageBus
    {
        // See SwimClusterMessageBus.FanOut.cs's PublishAsync for why this enqueues into the shared
        // broadcast queue instead of unicasting to every discovered peer.
        public override async Task PublishAsync(Guid channelKey, ClusterSubscriptionEvent subscriptionEvent, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var stamped = subscriptionEvent with { OriginId = _selfId };
            var payload = new ClusterChannelEnvelope<ClusterSubscriptionEvent>(channelKey, stamped);
            _appBroadcastQueue.Enqueue(new SwimDatagram(SwimMessageKind.GossipSubscriptionEvent, payload.ToNJson()));

            Log.SubscriptionEventQueued(_logger, channelKey);
        }

        public override Task<IAsyncDisposable> SubscribeAsync(Guid channelKey, Func<ClusterSubscriptionEvent, CancellationToken, Task> onEvent, CancellationToken cancellationToken = default)
        {
            return Task.FromResult(_subscriptionEventHandlers.Register(channelKey, onEvent));
        }

        /// <summary>
        /// Handles one <see cref="ClusterChannelEnvelope{TMessage}"/> of <see cref="ClusterSubscriptionEvent"/>,
        /// whether it arrived as a standalone <see cref="SwimMessageKind.GossipSubscriptionEvent"/>
        /// datagram or piggybacked inside a Ping/PingReq/Ack. Internal (rather than private) so tests
        /// can drive it directly with a raw payload.
        /// </summary>
        internal async Task HandleSubscriptionEventDeliveryAsync(string payload, CancellationToken cancellationToken)
        {
            ClusterChannelEnvelope<ClusterSubscriptionEvent>? subscriptionEventPayload;
            try
            {
                subscriptionEventPayload = payload.FromNJson<ClusterChannelEnvelope<ClusterSubscriptionEvent>>();
            }
            catch (Exception exception)
            {
                Log.SubscriptionEventUnparseable(_logger, exception);
                return;
            }

            if (subscriptionEventPayload is null || subscriptionEventPayload.Message.OriginId == _selfId)
                return;

            // Keep the epidemic spreading -- see HandleFanOutDeliveryAsync's identical comment.
            _appBroadcastQueue.Enqueue(new SwimDatagram(SwimMessageKind.GossipSubscriptionEvent, payload));

            if (!_subscriptionEventHandlers.TryGetHandler(subscriptionEventPayload.ChannelKey, out var handler) || handler is null)
                return;

            try
            {
                await handler(subscriptionEventPayload.Message, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Log.SubscriptionEventHandlerFaulted(_logger, exception);
            }
        }

        private static partial class Log
        {
            [LoggerMessage(EventId = 91620, Level = LogLevel.Debug,
                Message = "[Cluster] SWIM subscription event for channel '{ChannelKey}' queued for gossip dissemination.")]
            public static partial void SubscriptionEventQueued(ILogger logger, Guid channelKey);

            [LoggerMessage(EventId = 91621, Level = LogLevel.Warning,
                Message = "[Cluster] SWIM subscription event could not be parsed; skipping it.")]
            public static partial void SubscriptionEventUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91622, Level = LogLevel.Error,
                Message = "[Cluster] SWIM subscription-event handler faulted.")]
            public static partial void SubscriptionEventHandlerFaulted(ILogger logger, Exception exception);
        }
    }
}
