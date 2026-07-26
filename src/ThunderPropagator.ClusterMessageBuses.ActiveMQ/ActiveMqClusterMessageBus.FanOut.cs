using System.Collections.Concurrent;
using Apache.NMS;
using Microsoft.Extensions.Logging;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.BuildingBlocks.Application.Helpers;

namespace ThunderPropagator.ClusterMessageBuses.ActiveMQ
{
    internal sealed partial class ActiveMqClusterMessageBus
    {
        public override async Task PublishAsync(Guid channelKey, ClusterFanOutMessage message, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var stamped = message with { OriginId = _selfId };
            var topicName = ActiveMqTopicNaming.FanOutTopic(_options.TopicPrefix, channelKey);

            await _publishLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                var topic = await _publishSession!.GetTopicAsync(topicName).ConfigureAwait(false);
                var textMessage = await _publishSession.CreateTextMessageAsync(stamped.ToNJson()).ConfigureAwait(false);
                await _publishProducer!.SendAsync(topic, textMessage).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Log.FanOutPublishFailed(_logger, exception, topicName);
            }
            finally
            {
                _publishLock.Release();
            }
        }

        public override async Task<IAsyncDisposable> SubscribeAsync(Guid channelKey, Func<ClusterFanOutMessage, CancellationToken, Task> onMessage, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var topicName = ActiveMqTopicNaming.FanOutTopic(_options.TopicPrefix, channelKey);
            var session = await _connection!.CreateSessionAsync().ConfigureAwait(false);
            var topic = await session.GetTopicAsync(topicName).ConfigureAwait(false);
            var consumer = await session.CreateConsumerAsync(topic).ConfigureAwait(false);

            consumer.AsyncListener += (message, ct) => HandleFanOutDeliveryAsync(ReadText(message), onMessage, ct);

            var subscription = new FanOutSubscription(_fanOutSubscriptions, channelKey, session, consumer);
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
            private readonly ISession _session;
            private readonly IMessageConsumer _consumer;

            internal FanOutSubscription(
                ConcurrentDictionary<Guid, FanOutSubscription> subscriptions,
                Guid channelKey,
                ISession session,
                IMessageConsumer consumer)
            {
                _subscriptions = subscriptions;
                _channelKey = channelKey;
                _session = session;
                _consumer = consumer;
            }

            public async ValueTask DisposeAsync()
            {
                _subscriptions.TryRemove(_channelKey, out _);

                try
                {
                    await _consumer.CloseAsync().ConfigureAwait(false);
                }
                catch (Exception)
                {
                    // Best-effort: the session is closed/disposed regardless below (it may already
                    // be broken if the connection dropped).
                }

                _consumer.Dispose();

                try
                {
                    await _session.CloseAsync().ConfigureAwait(false);
                }
                catch (Exception)
                {
                    // Same best-effort reasoning as above.
                }

                _session.Dispose();
            }
        }

        private static partial class Log
        {
            [LoggerMessage(EventId = 91010, Level = LogLevel.Warning,
                Message = "[Cluster] ActiveMQ fan-out publish to topic '{Topic}' failed.")]
            public static partial void FanOutPublishFailed(ILogger logger, Exception exception, string topic);

            [LoggerMessage(EventId = 91011, Level = LogLevel.Warning,
                Message = "[Cluster] ActiveMQ fan-out message could not be parsed; skipping it.")]
            public static partial void FanOutMessageUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91012, Level = LogLevel.Error,
                Message = "[Cluster] ActiveMQ fan-out handler faulted.")]
            public static partial void FanOutHandlerFaulted(ILogger logger, Exception exception);
        }
    }
}
