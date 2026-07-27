using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.BuildingBlocks.Application.Helpers;

namespace ThunderPropagator.ClusterMessageBuses.UdpClient
{
    internal sealed partial class UdpClusterMessageBus
    {
        public override async Task PublishAsync(Guid channelKey, ClusterSubscriptionEvent subscriptionEvent, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var stamped = subscriptionEvent with { OriginId = _selfId };
            var frame = new UdpClusterFrame(UdpClusterFrameKind.SubscriptionEvent, new UdpSubscriptionEventPayload(channelKey, stamped).ToNJson());

            var peers = await _discovery.GetPeersAsync(cancellationToken).ConfigureAwait(false);

            await Task.WhenAll(peers.Select(async peer =>
            {
                try
                {
                    var remoteEndpoint = await _peerEndpointResolver(peer.Endpoint, cancellationToken).ConfigureAwait(false);
                    await SendFrameAsync(frame, remoteEndpoint, cancellationToken).ConfigureAwait(false);
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
            UdpSubscriptionEventPayload? subscriptionEventPayload;
            try
            {
                subscriptionEventPayload = payload.FromNJson<UdpSubscriptionEventPayload>();
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
            [LoggerMessage(EventId = 91520, Level = LogLevel.Warning,
                Message = "[Cluster] UDP subscription-event send to peer '{Host}' failed.")]
            public static partial void SubscriptionEventSendToPeerFailed(ILogger logger, Exception exception, string host);

            [LoggerMessage(EventId = 91521, Level = LogLevel.Warning,
                Message = "[Cluster] UDP subscription event could not be parsed; skipping it.")]
            public static partial void SubscriptionEventUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91522, Level = LogLevel.Error,
                Message = "[Cluster] UDP subscription-event handler faulted.")]
            public static partial void SubscriptionEventHandlerFaulted(ILogger logger, Exception exception);
        }
    }
}
