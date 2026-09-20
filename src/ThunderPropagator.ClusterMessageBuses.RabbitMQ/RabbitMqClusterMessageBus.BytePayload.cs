using System.Collections.Concurrent;
using System.Text;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.RabbitMQ
{
    /// <summary>
    /// Byte-oriented counterpart to <c>RabbitMqClusterMessageBus.FanOut.cs</c> /
    /// <c>.RequestReply.cs</c> -- see <see cref="ClusterByteMessage"/>'s own doc comment for why
    /// this surface exists alongside, not instead of, the <c>ClusterFanOutMessage</c>-based one.
    /// Fan-out uses its own per-channel "fanout"-type exchange
    /// (<see cref="RabbitMqTopicNaming.ByteFanOutExchange"/>), with every node binding its own
    /// exclusive, auto-delete queue to it exactly like <c>SubscribeAsync(Guid, Func{ClusterFanOutMessage,...})</c>
    /// does; the snapshot pull reuses the existing broker-native request/reply plumbing in
    /// <c>RabbitMqClusterMessageBus.RequestReply.cs</c> via a new
    /// <see cref="ClusterRequestKind.PullSnapshotBytes"/> kind, exactly like
    /// <c>RestoreFromLeaderAsync</c> reuses it for <see cref="ClusterRequestKind.RestoreSnapshot"/>.
    /// </summary>
    internal sealed partial class RabbitMqClusterMessageBus
    {
        public override async Task PublishAsync(Guid channelKey, ClusterByteMessage message, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var stamped = message with { OriginId = _selfId };
            var exchange = RabbitMqTopicNaming.ByteFanOutExchange(_options.ExchangePrefix, channelKey);
            var body = Encoding.UTF8.GetBytes(stamped.ToNJson());

            await _publishLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                await _publishChannel!.ExchangeDeclareAsync(exchange, ExchangeType.Fanout).ConfigureAwait(false);
                await _publishChannel!.BasicPublishAsync(exchange, string.Empty, false, new BasicProperties(), body).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Log.ByteFanOutPublishFailed(_logger, exception, exchange);
            }
            finally
            {
                _publishLock.Release();
            }
        }

        public override async Task<IAsyncDisposable> SubscribeAsync(Guid channelKey, Func<ClusterByteMessage, CancellationToken, Task> onMessage, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var exchange = RabbitMqTopicNaming.ByteFanOutExchange(_options.ExchangePrefix, channelKey);
            var channel = await _connection!.CreateChannelAsync().ConfigureAwait(false);
            await channel.ExchangeDeclareAsync(exchange, ExchangeType.Fanout).ConfigureAwait(false);

            var declareResult = await channel.QueueDeclareAsync(string.Empty, false, true, true, null).ConfigureAwait(false);
            var queueName = declareResult.QueueName;
            await channel.QueueBindAsync(queueName, exchange, string.Empty, null).ConfigureAwait(false);

            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ReceivedAsync += async (_, delivery) =>
                await HandleByteFanOutDeliveryAsync(delivery.Body.ToArray(), onMessage, cancellationToken).ConfigureAwait(false);
            var consumerTag = await channel.BasicConsumeAsync(queueName, true, consumer).ConfigureAwait(false);

            var subscription = new ByteFanOutSubscription(_byteFanOutSubscriptions, channelKey, channel, consumerTag);
            _byteFanOutSubscriptions[channelKey] = subscription;

            return subscription;
        }

        /// <summary>Internal (rather than private) so tests can drive it directly with a raw message body.</summary>
        internal async Task HandleByteFanOutDeliveryAsync(byte[] body, Func<ClusterByteMessage, CancellationToken, Task> onMessage, CancellationToken cancellationToken)
        {
            ClusterByteMessage? message;
            try
            {
                message = Encoding.UTF8.GetString(body).FromNJson<ClusterByteMessage>();
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
            private readonly IChannel _channel;
            private readonly string _consumerTag;

            internal ByteFanOutSubscription(
                ConcurrentDictionary<Guid, ByteFanOutSubscription> subscriptions,
                Guid channelKey,
                IChannel channel,
                string consumerTag)
            {
                _subscriptions = subscriptions;
                _channelKey = channelKey;
                _channel = channel;
                _consumerTag = consumerTag;
            }

            public async ValueTask DisposeAsync()
            {
                _subscriptions.TryRemove(_channelKey, out _);

                try
                {
                    await _channel.BasicCancelAsync(_consumerTag).ConfigureAwait(false);
                }
                catch (Exception)
                {
                    // Best-effort: the channel is closed/disposed regardless below (it may already
                    // be broken if the connection dropped).
                }

                try
                {
                    await _channel.CloseAsync().ConfigureAwait(false);
                }
                catch (Exception)
                {
                    // Same best-effort reasoning as above.
                }

                await _channel.DisposeAsync().ConfigureAwait(false);
            }
        }

        private static partial class Log
        {
            [LoggerMessage(EventId = 9660, Level = LogLevel.Warning,
                Message = "[Cluster] RabbitMQ byte fan-out publish to exchange '{Exchange}' failed.")]
            public static partial void ByteFanOutPublishFailed(ILogger logger, Exception exception, string exchange);

            [LoggerMessage(EventId = 9661, Level = LogLevel.Warning,
                Message = "[Cluster] RabbitMQ byte fan-out message could not be parsed; skipping it.")]
            public static partial void ByteFanOutMessageUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 9662, Level = LogLevel.Error,
                Message = "[Cluster] RabbitMQ byte fan-out handler faulted.")]
            public static partial void ByteFanOutHandlerFaulted(ILogger logger, Exception exception);
        }
    }
}
