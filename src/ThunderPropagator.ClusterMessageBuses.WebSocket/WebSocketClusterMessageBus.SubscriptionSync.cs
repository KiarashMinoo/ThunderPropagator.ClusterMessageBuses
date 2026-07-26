using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.BuildingBlocks.Application.Helpers;

namespace ThunderPropagator.ClusterMessageBuses.WebSocket
{
    internal sealed partial class WebSocketClusterMessageBus
    {
        public override async Task PublishAsync(Guid channelKey, ClusterSubscriptionEvent subscriptionEvent, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var stamped = subscriptionEvent with { OriginId = _selfId };
            var frame = new WebSocketClusterFrame(WebSocketClusterFrameKind.SubscriptionEvent, new WebSocketSubscriptionEventPayload(channelKey, stamped).ToNJson());

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
            _subscriptionEventHandlers[channelKey] = onEvent;
            return Task.FromResult<IAsyncDisposable>(new SubscriptionEventSubscription(_subscriptionEventHandlers, channelKey));
        }

        /// <summary>Internal (rather than private) so tests can drive it directly with a raw payload.</summary>
        internal async Task HandleSubscriptionEventDeliveryAsync(string payload, CancellationToken cancellationToken)
        {
            WebSocketSubscriptionEventPayload? subscriptionEventPayload;
            try
            {
                subscriptionEventPayload = payload.FromNJson<WebSocketSubscriptionEventPayload>();
            }
            catch (Exception exception)
            {
                Log.SubscriptionEventUnparseable(_logger, exception);
                return;
            }

            if (subscriptionEventPayload is null || subscriptionEventPayload.Event.OriginId == _selfId)
                return;

            if (!_subscriptionEventHandlers.TryGetValue(subscriptionEventPayload.ChannelKey, out var handler))
                return;

            try
            {
                await handler(subscriptionEventPayload.Event, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Log.SubscriptionEventHandlerFaulted(_logger, exception);
            }
        }

        private sealed class SubscriptionEventSubscription : IAsyncDisposable
        {
            private readonly ConcurrentDictionary<Guid, Func<ClusterSubscriptionEvent, CancellationToken, Task>> _handlers;
            private readonly Guid _channelKey;

            internal SubscriptionEventSubscription(ConcurrentDictionary<Guid, Func<ClusterSubscriptionEvent, CancellationToken, Task>> handlers, Guid channelKey)
            {
                _handlers = handlers;
                _channelKey = channelKey;
            }

            public ValueTask DisposeAsync()
            {
                _handlers.TryRemove(_channelKey, out _);
                return ValueTask.CompletedTask;
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
