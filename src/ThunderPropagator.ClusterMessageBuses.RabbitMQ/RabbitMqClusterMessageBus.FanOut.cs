using System.Collections.Concurrent;
using System.Text;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.BuildingBlocks.Application.Helpers;

namespace ThunderPropagator.ClusterMessageBuses.RabbitMQ
{
    internal sealed partial class RabbitMqClusterMessageBus
    {
        public override async Task PublishAsync(Guid channelKey, ClusterFanOutMessage message, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var stamped = message with { OriginId = _selfId };
            var exchange = RabbitMqTopicNaming.FanOutExchange(_options.ExchangePrefix, channelKey);
            var body = Encoding.UTF8.GetBytes(stamped.ToNJson());

            await _publishLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                await _publishChannel!.ExchangeDeclareAsync(exchange, ExchangeType.Fanout).ConfigureAwait(false);
                await _publishChannel.BasicPublishAsync(exchange, string.Empty, false, new BasicProperties(), body).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Log.FanOutPublishFailed(_logger, exception, exchange);
            }
            finally
            {
                _publishLock.Release();
            }
        }

        public override async Task<IAsyncDisposable> SubscribeAsync(Guid channelKey, Func<ClusterFanOutMessage, CancellationToken, Task> onMessage, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var exchange = RabbitMqTopicNaming.FanOutExchange(_options.ExchangePrefix, channelKey);
            var channel = await _connection!.CreateChannelAsync().ConfigureAwait(false);
            await channel.ExchangeDeclareAsync(exchange, ExchangeType.Fanout).ConfigureAwait(false);

            var declareResult = await channel.QueueDeclareAsync(string.Empty, false, true, true, null).ConfigureAwait(false);
            var queueName = declareResult.QueueName;
            await channel.QueueBindAsync(queueName, exchange, string.Empty, null).ConfigureAwait(false);

            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ReceivedAsync += async (_, delivery) =>
                await HandleFanOutDeliveryAsync(delivery.Body.ToArray(), onMessage, cancellationToken).ConfigureAwait(false);
            var consumerTag = await channel.BasicConsumeAsync(queueName, true, consumer).ConfigureAwait(false);

            var subscription = new FanOutSubscription(_fanOutSubscriptions, channelKey, channel, consumerTag);
            _fanOutSubscriptions[channelKey] = subscription;

            return subscription;
        }

        /// <summary>Internal (rather than private) so tests can drive it directly with a raw message body.</summary>
        internal async Task HandleFanOutDeliveryAsync(byte[] body, Func<ClusterFanOutMessage, CancellationToken, Task> onMessage, CancellationToken cancellationToken)
        {
            ClusterFanOutMessage? message;
            try
            {
                message = Encoding.UTF8.GetString(body).FromNJson<ClusterFanOutMessage>();
            }
            catch (Exception exception)
            {
                // A single malformed delivery must not take down an otherwise-healthy subscription
                // — log and move on (mirrors the fix applied to KafkaClusterMessageBus's consume
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
            private readonly IChannel _channel;
            private readonly string _consumerTag;

            internal FanOutSubscription(
                ConcurrentDictionary<Guid, FanOutSubscription> subscriptions,
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
            [LoggerMessage(EventId = 9610, Level = LogLevel.Warning,
                Message = "[Cluster] RabbitMQ fan-out publish to exchange '{Exchange}' failed.")]
            public static partial void FanOutPublishFailed(ILogger logger, Exception exception, string exchange);

            [LoggerMessage(EventId = 9611, Level = LogLevel.Warning,
                Message = "[Cluster] RabbitMQ fan-out message could not be parsed; skipping it.")]
            public static partial void FanOutMessageUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 9612, Level = LogLevel.Error,
                Message = "[Cluster] RabbitMQ fan-out handler faulted.")]
            public static partial void FanOutHandlerFaulted(ILogger logger, Exception exception);
        }
    }
}
