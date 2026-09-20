using Microsoft.Extensions.Logging;
using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.WebSocket
{
    internal sealed partial class WebSocketClusterMessageBus
    {
        public override async Task PublishAsync(Guid channelKey, ClusterSubscriptionEvent subscriptionEvent, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var stamped = subscriptionEvent with { OriginId = _selfId };
            var frame = new ClusterFrame(ClusterFrameKind.SubscriptionEvent, new ClusterChannelEnvelope<ClusterSubscriptionEvent>(channelKey, stamped).ToNJson());

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
                    Log.SubscriptionEventSendToPeerFailed(_logger, exception, peer.Endpoint.Host);
                }
            })).ConfigureAwait(false);
        }

        public override Task<IAsyncDisposable> SubscribeAsync(Guid channelKey, Func<ClusterSubscriptionEvent, CancellationToken, Task> onEvent, CancellationToken cancellationToken = default)
        {
            return Task.FromResult(_subscriptionEventHandlers.Register(channelKey, onEvent));
        }

        /// <summary>Internal (rather than private) so tests can drive it directly with a raw payload.</summary>
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
            [LoggerMessage(EventId = 91220, Level = LogLevel.Warning,
                Message = "[Cluster] WebSocket subscription-event send to peer '{Host}' failed.")]
            public static partial void SubscriptionEventSendToPeerFailed(ILogger logger, Exception exception, string host);

            [LoggerMessage(EventId = 91221, Level = LogLevel.Warning,
                Message = "[Cluster] WebSocket subscription event could not be parsed; skipping it.")]
            public static partial void SubscriptionEventUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91222, Level = LogLevel.Error,
                Message = "[Cluster] WebSocket subscription-event handler faulted.")]
            public static partial void SubscriptionEventHandlerFaulted(ILogger logger, Exception exception);
        }
    }
}
