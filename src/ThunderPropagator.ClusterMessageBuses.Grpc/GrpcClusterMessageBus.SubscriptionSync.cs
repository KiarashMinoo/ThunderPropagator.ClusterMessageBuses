using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.Grpc.Protos;

namespace ThunderPropagator.ClusterMessageBuses.Grpc
{
    internal sealed partial class GrpcClusterMessageBus
    {
        public override async Task PublishAsync(Guid channelKey, ClusterSubscriptionEvent subscriptionEvent, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var stamped = subscriptionEvent with { OriginId = _selfId };
            _lastPublishedSubscriptionEvents[channelKey] = stamped;

            var wireEvent = new SubscriptionEvent { ChannelKey = channelKey.ToString(), PayloadJson = stamped.ToNJson() };
            var peers = await _discovery.GetPeersAsync(cancellationToken).ConfigureAwait(false);

            await Task.WhenAll(peers.Select(async peer =>
            {
                try
                {
                    var connection = await GetOrCreateOutboundConnectionAsync(peer.Endpoint, cancellationToken).ConfigureAwait(false);
                    await _resiliencePipeline.ExecuteAsync(
                        async ct => await connection.SendSubscriptionEventAsync(wireEvent, ct).ConfigureAwait(false),
                        cancellationToken).ConfigureAwait(false);
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

        /// <summary>Internal (rather than private) so tests can drive it directly, and so <c>ClusterSubscriptionSyncGrpcService</c> can dispatch inbound deliveries into it.</summary>
        internal async Task HandleSubscriptionEventDeliveryAsync(SubscriptionEvent wireEvent, CancellationToken cancellationToken)
        {
            if (!Guid.TryParse(wireEvent.ChannelKey, out var channelKey))
            {
                Log.SubscriptionEventUnparseable(_logger, new FormatException($"'{wireEvent.ChannelKey}' is not a valid channel key."));
                return;
            }

            ClusterSubscriptionEvent? subscriptionEvent;
            try
            {
                subscriptionEvent = wireEvent.PayloadJson.FromNJson<ClusterSubscriptionEvent>();
            }
            catch (Exception exception)
            {
                Log.SubscriptionEventUnparseable(_logger, exception);
                return;
            }

            if (subscriptionEvent is null || subscriptionEvent.OriginId == _selfId)
                return;

            if (!_subscriptionEventHandlers.TryGetValue(channelKey, out var handler))
                return;

            try
            {
                await handler(subscriptionEvent, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Log.SubscriptionEventHandlerFaulted(_logger, exception);
            }
        }

        /// <summary>
        /// Replays the most recently published subscription-sync event per channel to a peer whose
        /// stream has just (re)connected, so a peer that missed events while disconnected still
        /// converges on this node's current subscription state (the ticket's "re-sends all currently
        /// active local subscriptions on reconnect" requirement).
        /// </summary>
        private async Task ResendActiveSubscriptionsAsync(GrpcPeerConnection connection, CancellationToken cancellationToken)
        {
            foreach (var (channelKey, subscriptionEvent) in _lastPublishedSubscriptionEvents)
            {
                var wireEvent = new SubscriptionEvent { ChannelKey = channelKey.ToString(), PayloadJson = subscriptionEvent.ToNJson() };

                try
                {
                    await connection.SendSubscriptionEventAsync(wireEvent, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    Log.SubscriptionResendFailed(_logger, exception);
                }
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
            [LoggerMessage(EventId = 91420, Level = LogLevel.Warning,
                Message = "[Cluster] gRPC subscription-sync send to peer '{Host}' failed.")]
            public static partial void SubscriptionEventSendToPeerFailed(ILogger logger, Exception exception, string host);

            [LoggerMessage(EventId = 91421, Level = LogLevel.Warning,
                Message = "[Cluster] gRPC subscription-sync event could not be parsed; skipping it.")]
            public static partial void SubscriptionEventUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91422, Level = LogLevel.Error,
                Message = "[Cluster] gRPC subscription-sync handler faulted.")]
            public static partial void SubscriptionEventHandlerFaulted(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91423, Level = LogLevel.Warning,
                Message = "[Cluster] Resending active subscriptions to a reconnected gRPC peer failed.")]
            public static partial void SubscriptionResendFailed(ILogger logger, Exception exception);
        }
    }
}
