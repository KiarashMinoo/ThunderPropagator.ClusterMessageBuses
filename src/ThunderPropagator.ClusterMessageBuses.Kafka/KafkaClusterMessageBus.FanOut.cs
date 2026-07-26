using System.Collections.Concurrent;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.BuildingBlocks.Application.Helpers;

namespace ThunderPropagator.ClusterMessageBuses.Kafka
{
    internal sealed partial class KafkaClusterMessageBus
    {
        public override async Task PublishAsync(Guid channelKey, ClusterFanOutMessage message, CancellationToken cancellationToken = default)
        {
            var stamped = message with { OriginId = _selfId };
            var topic = KafkaTopicNaming.FanOutTopic(_options.TopicPrefix, channelKey);

            try
            {
                await _producer.ProduceAsync(topic, new Message<string, string> { Value = stamped.ToNJson() }, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (ProduceException<string, string> exception)
            {
                Log.FanOutPublishFailed(_logger, exception, topic);
            }
        }

        public override Task<IAsyncDisposable> SubscribeAsync(Guid channelKey, Func<ClusterFanOutMessage, CancellationToken, Task> onMessage, CancellationToken cancellationToken = default)
        {
            var topic = KafkaTopicNaming.FanOutTopic(_options.TopicPrefix, channelKey);
            var consumer = _consumerFactory(BuildConsumerConfig($"{_options.ConsumerGroupPrefix}-fanout-{Guid.NewGuid():N}"));
            consumer.Subscribe(topic);

            var loopCts = CancellationTokenSource.CreateLinkedTokenSource(_lifetimeCts.Token);
            var loopTask = Task.Run(() => RunFanOutConsumerLoopAsync(consumer, onMessage, loopCts.Token), loopCts.Token);

            var subscription = new FanOutSubscription(_fanOutSubscriptions, channelKey, consumer, loopCts, loopTask);
            _fanOutSubscriptions[channelKey] = subscription;

            return Task.FromResult<IAsyncDisposable>(subscription);
        }

        /// <summary>Internal (rather than private) so tests can drive it directly with a controlled fake consumer.</summary>
        internal async Task RunFanOutConsumerLoopAsync(IConsumer<string, string> consumer, Func<ClusterFanOutMessage, CancellationToken, Task> onMessage, CancellationToken cancellationToken)
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
                        Log.FanOutConsumeFailed(_logger, exception);
                        continue;
                    }

                    ClusterFanOutMessage? message;
                    try
                    {
                        message = result?.Message?.Value?.FromNJson<ClusterFanOutMessage>();
                    }
                    catch (Exception exception)
                    {
                        // A single malformed message must not take down an otherwise-healthy
                        // consumer loop — log and move on to the next message.
                        Log.FanOutMessageUnparseable(_logger, exception);
                        continue;
                    }

                    if (message is null || message.OriginId == _selfId)
                        continue;

                    try
                    {
                        await onMessage(message, cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception exception)
                    {
                        Log.FanOutHandlerFaulted(_logger, exception);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Expected on shutdown/unsubscribe.
            }
        }

        private sealed class FanOutSubscription : IAsyncDisposable
        {
            private readonly ConcurrentDictionary<Guid, FanOutSubscription> _subscriptions;
            private readonly Guid _channelKey;
            private readonly IConsumer<string, string> _consumer;
            private readonly CancellationTokenSource _loopCts;
            private readonly Task _loopTask;

            internal FanOutSubscription(
                ConcurrentDictionary<Guid, FanOutSubscription> subscriptions,
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
            [LoggerMessage(EventId = 9510, Level = LogLevel.Warning,
                Message = "[Cluster] Kafka fan-out publish to topic '{Topic}' failed.")]
            public static partial void FanOutPublishFailed(ILogger logger, Exception exception, string topic);

            [LoggerMessage(EventId = 9511, Level = LogLevel.Warning,
                Message = "[Cluster] Kafka fan-out consume failed.")]
            public static partial void FanOutConsumeFailed(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 9513, Level = LogLevel.Warning,
                Message = "[Cluster] Kafka fan-out message could not be parsed; skipping it.")]
            public static partial void FanOutMessageUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 9512, Level = LogLevel.Error,
                Message = "[Cluster] Kafka fan-out handler faulted.")]
            public static partial void FanOutHandlerFaulted(ILogger logger, Exception exception);
        }
    }
}
