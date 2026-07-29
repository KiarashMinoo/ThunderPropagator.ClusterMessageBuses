using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.BuildingBlocks.Application.Helpers;

namespace ThunderPropagator.ClusterMessageBuses.RedisPubSub
{
    internal sealed partial class RedisPubSubClusterMessageBus
    {
        public override async Task PublishAsync(Guid channelKey, ClusterFanOutMessage message, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var stamped = message with { OriginId = _selfId };
            var channelName = RedisChannelNaming.FanOutChannel(_options.ChannelPrefix, channelKey);

            try
            {
                await _subscriber!.PublishAsync(RedisChannel.Literal(channelName), stamped.ToNJson()).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Log.FanOutPublishFailed(_logger, exception, channelName);
            }
        }

        public override async Task<IAsyncDisposable> SubscribeAsync(Guid channelKey, Func<ClusterFanOutMessage, CancellationToken, Task> onMessage, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var channelName = RedisChannelNaming.FanOutChannel(_options.ChannelPrefix, channelKey);
            var redisChannel = RedisChannel.Literal(channelName);

            Action<RedisChannel, RedisValue> handler = (channel, value) =>
                _ = HandleFanOutDeliveryAsync(value.ToString() ?? string.Empty, onMessage, _lifetimeCts.Token);

            await _subscriber!.SubscribeAsync(redisChannel, handler).ConfigureAwait(false);

            var subscription = new FanOutSubscription(_fanOutSubscriptions, channelKey, _subscriber, redisChannel, handler);
            _fanOutSubscriptions[channelKey] = subscription;

            return subscription;
        }

        /// <summary>Internal (rather than private) so tests can drive it directly with a raw payload.</summary>
        internal async Task HandleFanOutDeliveryAsync(string payload, Func<ClusterFanOutMessage, CancellationToken, Task> onMessage, CancellationToken cancellationToken)
        {
            ClusterFanOutMessage? message;
            try
            {
                message = payload.FromNJson<ClusterFanOutMessage>();
            }
            catch (Exception exception)
            {
                // A single malformed delivery must not take down an otherwise-healthy subscription
                // — log and move on (mirrors the fix applied to every other transport's consume
                // loops after a dedicated test proved the unguarded deserialize could crash them).
                Log.FanOutMessageUnparseable(_logger, exception);
                return;
            }

            if (message is null || message.OriginId == _selfId)
                return;

            try
            {
                await onMessage(message, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Log.FanOutHandlerFaulted(_logger, exception);
            }
        }

        private sealed class FanOutSubscription : IAsyncDisposable
        {
            private readonly ConcurrentDictionary<Guid, FanOutSubscription> _subscriptions;
            private readonly Guid _channelKey;
            private readonly ISubscriber _subscriber;
            private readonly RedisChannel _redisChannel;
            private readonly Action<RedisChannel, RedisValue> _handler;

            internal FanOutSubscription(
                ConcurrentDictionary<Guid, FanOutSubscription> subscriptions,
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
                    // Best-effort: the subscription is torn down regardless (it may already be
                    // broken if the connection dropped).
                }
            }
        }

        private static partial class Log
        {
            [LoggerMessage(EventId = 91110, Level = LogLevel.Warning,
                Message = "[Cluster] Redis pub/sub fan-out publish to channel '{Channel}' failed.")]
            public static partial void FanOutPublishFailed(ILogger logger, Exception exception, string channel);

            [LoggerMessage(EventId = 91111, Level = LogLevel.Warning,
                Message = "[Cluster] Redis pub/sub fan-out message could not be parsed; skipping it.")]
            public static partial void FanOutMessageUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91112, Level = LogLevel.Error,
                Message = "[Cluster] Redis pub/sub fan-out handler faulted.")]
            public static partial void FanOutHandlerFaulted(ILogger logger, Exception exception);
        }
    }
}
