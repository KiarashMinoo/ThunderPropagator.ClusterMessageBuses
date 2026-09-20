using System.Collections.Concurrent;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.Kafka
{
    /// <summary>
    /// Byte-oriented counterpart to <c>KafkaClusterMessageBus.FanOut.cs</c> /
    /// <c>.RequestReply.cs</c> -- see <see cref="ClusterByteMessage"/>'s own doc comment for why
    /// this surface exists alongside, not instead of, the <c>ClusterFanOutMessage</c>-based one.
    /// Fan-out uses its own per-channel topic (<see cref="KafkaTopicNaming.ByteFanOutTopic"/>);
    /// the snapshot pull reuses the existing broker-native request/reply plumbing in
    /// <c>KafkaClusterMessageBus.RequestReply.cs</c> via a new <see cref="ClusterRequestKind.PullSnapshotBytes"/>
    /// kind, exactly like <c>RestoreFromLeaderAsync</c> reuses it for <see cref="ClusterRequestKind.RestoreSnapshot"/>.
    /// </summary>
    internal sealed partial class KafkaClusterMessageBus
    {
        public override async Task PublishAsync(Guid channelKey, ClusterByteMessage message, CancellationToken cancellationToken = default)
        {
            var stamped = message with { OriginId = _selfId };
            var topic = KafkaTopicNaming.ByteFanOutTopic(_options.TopicPrefix, channelKey);

            try
            {
                await _producer.ProduceAsync(topic, new Message<string, string> { Value = stamped.ToNJson() }, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (ProduceException<string, string> exception)
            {
                Log.ByteFanOutPublishFailed(_logger, exception, topic);
            }
        }

        public override Task<IAsyncDisposable> SubscribeAsync(Guid channelKey, Func<ClusterByteMessage, CancellationToken, Task> onMessage, CancellationToken cancellationToken = default)
        {
            var topic = KafkaTopicNaming.ByteFanOutTopic(_options.TopicPrefix, channelKey);
            var consumer = _consumerFactory(BuildConsumerConfig($"{_options.ConsumerGroupPrefix}-bytefanout-{Guid.NewGuid():N}"));
            consumer.Subscribe(topic);

            var loopCts = CancellationTokenSource.CreateLinkedTokenSource(_lifetimeCts.Token);
            var loopTask = Task.Run(() => RunByteFanOutConsumerLoopAsync(consumer, onMessage, loopCts.Token), loopCts.Token);

            var subscription = new ByteFanOutSubscription(_byteFanOutSubscriptions, channelKey, consumer, loopCts, loopTask);
            _byteFanOutSubscriptions[channelKey] = subscription;

            return Task.FromResult<IAsyncDisposable>(subscription);
        }

        /// <summary>Internal (rather than private) so tests can drive it directly with a controlled fake consumer.</summary>
        internal async Task RunByteFanOutConsumerLoopAsync(IConsumer<string, string> consumer, Func<ClusterByteMessage, CancellationToken, Task> onMessage, CancellationToken cancellationToken)
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
                        Log.ByteFanOutConsumeFailed(_logger, exception);
                        continue;
                    }

                    ClusterByteMessage? message;
                    try
                    {
                        message = result?.Message?.Value?.FromNJson<ClusterByteMessage>();
                    }
                    catch (Exception exception)
                    {
                        Log.ByteFanOutMessageUnparseable(_logger, exception);
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
                        Log.ByteFanOutHandlerFaulted(_logger, exception);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Expected on shutdown/unsubscribe.
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
            private readonly IConsumer<string, string> _consumer;
            private readonly CancellationTokenSource _loopCts;
            private readonly Task _loopTask;

            internal ByteFanOutSubscription(
                ConcurrentDictionary<Guid, ByteFanOutSubscription> subscriptions,
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
            [LoggerMessage(EventId = 9550, Level = LogLevel.Warning,
                Message = "[Cluster] Kafka byte fan-out publish to topic '{Topic}' failed.")]
            public static partial void ByteFanOutPublishFailed(ILogger logger, Exception exception, string topic);

            [LoggerMessage(EventId = 9551, Level = LogLevel.Warning,
                Message = "[Cluster] Kafka byte fan-out consume failed.")]
            public static partial void ByteFanOutConsumeFailed(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 9552, Level = LogLevel.Warning,
                Message = "[Cluster] Kafka byte fan-out message could not be parsed; skipping it.")]
            public static partial void ByteFanOutMessageUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 9553, Level = LogLevel.Error,
                Message = "[Cluster] Kafka byte fan-out handler faulted.")]
            public static partial void ByteFanOutHandlerFaulted(ILogger logger, Exception exception);
        }
    }
}
