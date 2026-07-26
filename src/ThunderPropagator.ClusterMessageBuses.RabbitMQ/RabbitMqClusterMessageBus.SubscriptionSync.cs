using System.Collections.Concurrent;
using System.Text;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.BuildingBlocks.Application.Helpers;

namespace ThunderPropagator.ClusterMessageBuses.RabbitMQ
{
    internal sealed partial class RabbitMqClusterMessageBus
    {
        public override async Task PublishAsync(Guid channelKey, ClusterSubscriptionEvent subscriptionEvent, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var stamped = subscriptionEvent with { OriginId = _selfId };
            var exchange = RabbitMqTopicNaming.SubscriptionEventExchange(_options.ExchangePrefix, channelKey);
            var body = Encoding.UTF8.GetBytes(stamped.ToNJson());

            await _publishLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                await _publishChannel!.ExchangeDeclareAsync(exchange, ExchangeType.Fanout).ConfigureAwait(false);
                await _publishChannel.BasicPublishAsync(exchange, string.Empty, false, new BasicProperties(), body).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Log.SubscriptionEventPublishFailed(_logger, exception, exchange);
            }
            finally
            {
                _publishLock.Release();
            }
        }

        public override async Task<IAsyncDisposable> SubscribeAsync(Guid channelKey, Func<ClusterSubscriptionEvent, CancellationToken, Task> onEvent, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var exchange = RabbitMqTopicNaming.SubscriptionEventExchange(_options.ExchangePrefix, channelKey);
            var channel = await _connection!.CreateChannelAsync().ConfigureAwait(false);
            await channel.ExchangeDeclareAsync(exchange, ExchangeType.Fanout).ConfigureAwait(false);

            var declareResult = await channel.QueueDeclareAsync(string.Empty, false, true, true, null).ConfigureAwait(false);
            var queueName = declareResult.QueueName;
            await channel.QueueBindAsync(queueName, exchange, string.Empty, null).ConfigureAwait(false);

            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ReceivedAsync += async (_, delivery) =>
                await HandleSubscriptionEventDeliveryAsync(delivery.Body.ToArray(), onEvent, cancellationToken).ConfigureAwait(false);
            var consumerTag = await channel.BasicConsumeAsync(queueName, true, consumer).ConfigureAwait(false);

            var subscription = new SubscriptionEventSubscription(_subscriptionEventSubscriptions, channelKey, channel, consumerTag);
            _subscriptionEventSubscriptions[channelKey] = subscription;

            return subscription;
        }

        /// <summary>Internal (rather than private) so tests can drive it directly with a raw message body.</summary>
        internal async Task HandleSubscriptionEventDeliveryAsync(byte[] body, Func<ClusterSubscriptionEvent, CancellationToken, Task> onEvent, CancellationToken cancellationToken)
        {
            ClusterSubscriptionEvent? subscriptionEvent;
            try
            {
                subscriptionEvent = Encoding.UTF8.GetString(body).FromNJson<ClusterSubscriptionEvent>();
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
            private readonly IChannel _channel;
            private readonly string _consumerTag;

            internal SubscriptionEventSubscription(
                ConcurrentDictionary<Guid, SubscriptionEventSubscription> subscriptions,
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
                    // Best-effort — see FanOutSubscription.DisposeAsync for the same reasoning.
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
            [LoggerMessage(EventId = 9620, Level = LogLevel.Warning,
                Message = "[Cluster] RabbitMQ subscription-event publish to exchange '{Exchange}' failed.")]
            public static partial void SubscriptionEventPublishFailed(ILogger logger, Exception exception, string exchange);

            [LoggerMessage(EventId = 9621, Level = LogLevel.Warning,
                Message = "[Cluster] RabbitMQ subscription event could not be parsed; skipping it.")]
            public static partial void SubscriptionEventUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 9622, Level = LogLevel.Error,
                Message = "[Cluster] RabbitMQ subscription-event handler faulted.")]
            public static partial void SubscriptionEventHandlerFaulted(ILogger logger, Exception exception);
        }
    }
}
