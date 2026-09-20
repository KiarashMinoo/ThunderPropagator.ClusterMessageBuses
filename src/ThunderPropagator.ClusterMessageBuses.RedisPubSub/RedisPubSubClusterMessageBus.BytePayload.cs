using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.RedisPubSub
{
    /// <summary>
    /// Byte-oriented counterpart to <c>RedisPubSubClusterMessageBus.FanOut.cs</c> /
    /// <c>.RequestReply.cs</c> -- see <see cref="ClusterByteMessage"/>'s own doc comment for why
    /// this surface exists alongside, not instead of, the <c>ClusterFanOutMessage</c>-based one.
    /// Fan-out uses its own per-channel Redis channel
    /// (<see cref="RedisChannelNaming.ByteFanOutChannel"/>) — plain Redis pub/sub already delivers a
    /// copy of every published message to every subscribing client, so no consumer-group/exclusive-
    /// queue/unique-subscription-name trick is needed here, exactly like the existing
    /// <c>ClusterFanOutMessage</c> fan-out. The snapshot pull reuses the existing hand-rolled
    /// request/reply plumbing in <c>RedisPubSubClusterMessageBus.RequestReply.cs</c> via a new
    /// <see cref="ClusterRequestKind.PullSnapshotBytes"/> kind, exactly like
    /// <c>RestoreFromLeaderAsync</c> reuses it for <see cref="ClusterRequestKind.RestoreSnapshot"/>.
    /// </summary>
    internal sealed partial class RedisPubSubClusterMessageBus
    {
        public override async Task PublishAsync(Guid channelKey, ClusterByteMessage message, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var stamped = message with { OriginId = _selfId };
            var channelName = RedisChannelNaming.ByteFanOutChannel(_options.ChannelPrefix, channelKey);

            try
            {
                await _subscriber!.PublishAsync(RedisChannel.Literal(channelName), stamped.ToNJson()).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Log.ByteFanOutPublishFailed(_logger, exception, channelName);
            }
        }

        public override async Task<IAsyncDisposable> SubscribeAsync(Guid channelKey, Func<ClusterByteMessage, CancellationToken, Task> onMessage, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var channelName = RedisChannelNaming.ByteFanOutChannel(_options.ChannelPrefix, channelKey);
            var redisChannel = RedisChannel.Literal(channelName);

            Action<RedisChannel, RedisValue> handler = (channel, value) =>
                _ = HandleByteFanOutDeliveryAsync(value.ToString() ?? string.Empty, onMessage, _lifetimeCts.Token);

            await _subscriber!.SubscribeAsync(redisChannel, handler).ConfigureAwait(false);

            var subscription = new ByteFanOutSubscription(_byteFanOutSubscriptions, channelKey, _subscriber, redisChannel, handler);
            _byteFanOutSubscriptions[channelKey] = subscription;

            return subscription;
        }

        /// <summary>Internal (rather than private) so tests can drive it directly with a raw payload.</summary>
        internal async Task HandleByteFanOutDeliveryAsync(string payload, Func<ClusterByteMessage, CancellationToken, Task> onMessage, CancellationToken cancellationToken)
        {
            ClusterByteMessage? message;
            try
            {
                message = payload.FromNJson<ClusterByteMessage>();
            }
            catch (Exception exception)
            {
                Log.ByteFanOutMessageUnparseable(_logger, exception);
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
                Log.ByteFanOutHandlerFaulted(_logger, exception);
            }
        }

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
        /// has nothing to offer yet -- mirrored by <see cref="PullSnapshotAsync"/> deserializing
        /// that back to a null byte[], exactly like <c>BuildRestoreSnapshotResponseAsync</c>
        /// answers with an empty array rather than a failure when a channel has nothing to restore.
        /// </summary>
        private async Task<ClusterResponseEnvelope> BuildPullSnapshotBytesResponseAsync(ClusterRequestEnvelope request, CancellationToken cancellationToken)
        {
            if (_byteSnapshotProvider is null)
                return new ClusterResponseEnvelope(request.CorrelationId, true, null, "null");

            var snapshot = await _byteSnapshotProvider.GetSnapshotAsync(request.ChannelKey!.Value, cancellationToken).ConfigureAwait(false);
            return new ClusterResponseEnvelope(request.CorrelationId, true, null, snapshot.ToNJson());
        }

        private sealed class ByteFanOutSubscription : IAsyncDisposable
        {
            private readonly ConcurrentDictionary<Guid, ByteFanOutSubscription> _subscriptions;
            private readonly Guid _channelKey;
            private readonly ISubscriber _subscriber;
            private readonly RedisChannel _redisChannel;
            private readonly Action<RedisChannel, RedisValue> _handler;

            internal ByteFanOutSubscription(
                ConcurrentDictionary<Guid, ByteFanOutSubscription> subscriptions,
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
            [LoggerMessage(EventId = 91160, Level = LogLevel.Warning,
                Message = "[Cluster] Redis pub/sub byte fan-out publish to channel '{Channel}' failed.")]
            public static partial void ByteFanOutPublishFailed(ILogger logger, Exception exception, string channel);

            [LoggerMessage(EventId = 91161, Level = LogLevel.Warning,
                Message = "[Cluster] Redis pub/sub byte fan-out message could not be parsed; skipping it.")]
            public static partial void ByteFanOutMessageUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91162, Level = LogLevel.Error,
                Message = "[Cluster] Redis pub/sub byte fan-out handler faulted.")]
            public static partial void ByteFanOutHandlerFaulted(ILogger logger, Exception exception);
        }
    }
}
