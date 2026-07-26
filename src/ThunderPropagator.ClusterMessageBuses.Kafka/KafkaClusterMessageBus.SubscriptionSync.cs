using System.Collections.Concurrent;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.BuildingBlocks.Application.Helpers;

namespace ThunderPropagator.ClusterMessageBuses.Kafka
{
    internal sealed partial class KafkaClusterMessageBus
    {
        public override async Task PublishAsync(Guid channelKey, ClusterSubscriptionEvent subscriptionEvent, CancellationToken cancellationToken = default)
        {
            var stamped = subscriptionEvent with { OriginId = _selfId };
            var topic = KafkaTopicNaming.SubscriptionEventTopic(_options.TopicPrefix, channelKey);

            try
            {
                await _producer.ProduceAsync(topic, new Message<string, string> { Value = stamped.ToNJson() }, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (ProduceException<string, string> exception)
            {
                Log.SubscriptionEventPublishFailed(_logger, exception, topic);
            }
        }

        public override Task<IAsyncDisposable> SubscribeAsync(Guid channelKey, Func<ClusterSubscriptionEvent, CancellationToken, Task> onEvent, CancellationToken cancellationToken = default)
        {
            var topic = KafkaTopicNaming.SubscriptionEventTopic(_options.TopicPrefix, channelKey);
            var consumer = _consumerFactory(BuildConsumerConfig($"{_options.ConsumerGroupPrefix}-subscriptions-{Guid.NewGuid():N}"));
            consumer.Subscribe(topic);

            var loopCts = CancellationTokenSource.CreateLinkedTokenSource(_lifetimeCts.Token);
            var loopTask = Task.Run(() => RunSubscriptionEventConsumerLoopAsync(consumer, onEvent, loopCts.Token), loopCts.Token);

            var subscription = new SubscriptionEventSubscription(_subscriptionEventSubscriptions, channelKey, consumer, loopCts, loopTask);
            _subscriptionEventSubscriptions[channelKey] = subscription;

            return Task.FromResult<IAsyncDisposable>(subscription);
        }

        /// <summary>Internal (rather than private) so tests can drive it directly with a controlled fake consumer.</summary>
        internal async Task RunSubscriptionEventConsumerLoopAsync(IConsumer<string, string> consumer, Func<ClusterSubscriptionEvent, CancellationToken, Task> onEvent, CancellationToken cancellationToken)
        {
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    ConsumeResult<string, string>? result;
                    try
                    {
                        result = consumer.Consume(cancellationToken);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                    catch (ConsumeException exception)
                    {
                        Log.SubscriptionEventConsumeFailed(_logger, exception);
                        continue;
                    }

                    ClusterSubscriptionEvent? subscriptionEvent;
                    try
                    {
                        subscriptionEvent = result?.Message?.Value?.FromNJson<ClusterSubscriptionEvent>();
                    }
                    catch (Exception exception)
                    {
                        Log.SubscriptionEventUnparseable(_logger, exception);
                        continue;
                    }

                    if (subscriptionEvent is null || subscriptionEvent.OriginId == _selfId)
                        continue;

                    try
                    {
                        await onEvent(subscriptionEvent, cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception exception)
                    {
                        Log.SubscriptionEventHandlerFaulted(_logger, exception);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Expected on shutdown/unsubscribe.
            }
        }

        private sealed class SubscriptionEventSubscription : IAsyncDisposable
        {
            private readonly ConcurrentDictionary<Guid, SubscriptionEventSubscription> _subscriptions;
            private readonly Guid _channelKey;
            private readonly IConsumer<string, string> _consumer;
            private readonly CancellationTokenSource _loopCts;
            private readonly Task _loopTask;

            internal SubscriptionEventSubscription(
                ConcurrentDictionary<Guid, SubscriptionEventSubscription> subscriptions,
                Guid channelKey,
                IConsumer<string, string> consumer,
                CancellationTokenSource loopCts,
                Task loopTask)
            {
                _subscriptions = subscriptions;
                _channelKey = channelKey;
                _consumer = consumer;
                _loopCts = loopCts;
                _loopTask = loopTask;
            }

            public async ValueTask DisposeAsync()
            {
                _subscriptions.TryRemove(_channelKey, out _);

                await _loopCts.CancelAsync().ConfigureAwait(false);
                try
                {
                    await _loopTask.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
                }
                catch (Exception exception) when (exception is TimeoutException or OperationCanceledException)
                {
                    // Best-effort: the consumer is closed/disposed regardless below.
                }

                _consumer.Close();
                _consumer.Dispose();
                _loopCts.Dispose();
            }
        }

        private static partial class Log
        {
            [LoggerMessage(EventId = 9520, Level = LogLevel.Warning,
                Message = "[Cluster] Kafka subscription-event publish to topic '{Topic}' failed.")]
            public static partial void SubscriptionEventPublishFailed(ILogger logger, Exception exception, string topic);

            [LoggerMessage(EventId = 9521, Level = LogLevel.Warning,
                Message = "[Cluster] Kafka subscription-event consume failed.")]
            public static partial void SubscriptionEventConsumeFailed(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 9523, Level = LogLevel.Warning,
                Message = "[Cluster] Kafka subscription event could not be parsed; skipping it.")]
            public static partial void SubscriptionEventUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 9522, Level = LogLevel.Error,
                Message = "[Cluster] Kafka subscription-event handler faulted.")]
            public static partial void SubscriptionEventHandlerFaulted(ILogger logger, Exception exception);
        }
    }
}
