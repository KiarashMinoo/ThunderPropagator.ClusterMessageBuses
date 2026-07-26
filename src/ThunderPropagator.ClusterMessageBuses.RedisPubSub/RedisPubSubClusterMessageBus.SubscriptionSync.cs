using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.BuildingBlocks.Application.Helpers;

namespace ThunderPropagator.ClusterMessageBuses.RedisPubSub
{
    internal sealed partial class RedisPubSubClusterMessageBus
    {
        public override async Task PublishAsync(Guid channelKey, ClusterSubscriptionEvent subscriptionEvent, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var stamped = subscriptionEvent with { OriginId = _selfId };
            var channelName = RedisChannelNaming.SubscriptionEventChannel(_options.ChannelPrefix, channelKey);

            try
            {
                await _subscriber!.PublishAsync(RedisChannel.Literal(channelName), stamped.ToNJson()).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Log.SubscriptionEventPublishFailed(_logger, exception, channelName);
            }
        }

        public override async Task<IAsyncDisposable> SubscribeAsync(Guid channelKey, Func<ClusterSubscriptionEvent, CancellationToken, Task> onEvent, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var channelName = RedisChannelNaming.SubscriptionEventChannel(_options.ChannelPrefix, channelKey);
            var redisChannel = RedisChannel.Literal(channelName);

            Action<RedisChannel, RedisValue> handler = (_, value) =>
                _ = HandleSubscriptionEventDeliveryAsync(value.ToString() ?? string.Empty, onEvent, _lifetimeCts.Token);

            await _subscriber!.SubscribeAsync(redisChannel, handler).ConfigureAwait(false);

            var subscription = new SubscriptionEventSubscription(_subscriptionEventSubscriptions, channelKey, _subscriber, redisChannel, handler);
            _subscriptionEventSubscriptions[channelKey] = subscription;

            return subscription;
        }

        /// <summary>Internal (rather than private) so tests can drive it directly with a raw payload.</summary>
        internal async Task HandleSubscriptionEventDeliveryAsync(string payload, Func<ClusterSubscriptionEvent, CancellationToken, Task> onEvent, CancellationToken cancellationToken)
        {
            ClusterSubscriptionEvent? subscriptionEvent;
            try
            {
                subscriptionEvent = payload.FromNJson<ClusterSubscriptionEvent>();
            }
            catch (Exception exception)
            {
                Log.SubscriptionEventUnparseable(_logger, exception);
                return;
            }

            if (subscriptionEvent is null || subscriptionEvent.OriginId == _selfId)
                return;

            try
            {
                await onEvent(subscriptionEvent, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Log.SubscriptionEventHandlerFaulted(_logger, exception);
            }
        }

        private sealed class SubscriptionEventSubscription : IAsyncDisposable
        {
            private readonly ConcurrentDictionary<Guid, SubscriptionEventSubscription> _subscriptions;
            private readonly Guid _channelKey;
            private readonly ISubscriber _subscriber;
            private readonly RedisChannel _redisChannel;
            private readonly Action<RedisChannel, RedisValue> _handler;

            internal SubscriptionEventSubscription(
                ConcurrentDictionary<Guid, SubscriptionEventSubscription> subscriptions,
                Guid channelKey,
                ISubscriber subscriber,
                RedisChannel redisChannel,
                Action<RedisChannel, RedisValue> handler)
            {
                _subscriptions = subscriptions;
                _channelKey = channelKey;
                _subscriber = subscriber;
                _redisChannel = redisChannel;
                _handler = handler;
            }

            public async ValueTask DisposeAsync()
            {
                _subscriptions.TryRemove(_channelKey, out _);

                try
                {
                    await _subscriber.UnsubscribeAsync(_redisChannel, _handler).ConfigureAwait(false);
                }
                catch (Exception)
                {
                    // Best-effort: the subscription is torn down regardless.
                }
            }
        }

        private static partial class Log
        {
            [LoggerMessage(EventId = 91120, Level = LogLevel.Warning,
                Message = "[Cluster] Redis pub/sub subscription-event publish to channel '{Channel}' failed.")]
            public static partial void SubscriptionEventPublishFailed(ILogger logger, Exception exception, string channel);

            [LoggerMessage(EventId = 91121, Level = LogLevel.Warning,
                Message = "[Cluster] Redis pub/sub subscription event could not be parsed; skipping it.")]
            public static partial void SubscriptionEventUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91122, Level = LogLevel.Error,
                Message = "[Cluster] Redis pub/sub subscription-event handler faulted.")]
            public static partial void SubscriptionEventHandlerFaulted(ILogger logger, Exception exception);
        }
    }
}
