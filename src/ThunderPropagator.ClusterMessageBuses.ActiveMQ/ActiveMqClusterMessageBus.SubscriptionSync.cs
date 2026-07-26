using System.Collections.Concurrent;
using Apache.NMS;
using Microsoft.Extensions.Logging;
using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.BuildingBlocks.Application.Helpers;

namespace ThunderPropagator.ClusterMessageBuses.ActiveMQ
{
    internal sealed partial class ActiveMqClusterMessageBus
    {
        public override async Task PublishAsync(Guid channelKey, ClusterSubscriptionEvent subscriptionEvent, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var stamped = subscriptionEvent with { OriginId = _selfId };
            var topicName = ActiveMqTopicNaming.SubscriptionEventTopic(_options.TopicPrefix, channelKey);

            await _publishLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                var topic = await _publishSession!.GetTopicAsync(topicName).ConfigureAwait(false);
                var textMessage = await _publishSession.CreateTextMessageAsync(stamped.ToNJson()).ConfigureAwait(false);
                await _publishProducer!.SendAsync(topic, textMessage).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Log.SubscriptionEventPublishFailed(_logger, exception, topicName);
            }
            finally
            {
                _publishLock.Release();
            }
        }

        public override async Task<IAsyncDisposable> SubscribeAsync(Guid channelKey, Func<ClusterSubscriptionEvent, CancellationToken, Task> onEvent, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var topicName = ActiveMqTopicNaming.SubscriptionEventTopic(_options.TopicPrefix, channelKey);
            var session = await _connection!.CreateSessionAsync().ConfigureAwait(false);
            var topic = await session.GetTopicAsync(topicName).ConfigureAwait(false);
            var consumer = await session.CreateConsumerAsync(topic).ConfigureAwait(false);

            consumer.AsyncListener += (message, ct) => HandleSubscriptionEventDeliveryAsync(ReadText(message), onEvent, ct);

            var subscription = new SubscriptionEventSubscription(_subscriptionEventSubscriptions, channelKey, session, consumer);
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
            private readonly ISession _session;
            private readonly IMessageConsumer _consumer;

            internal SubscriptionEventSubscription(
                ConcurrentDictionary<Guid, SubscriptionEventSubscription> subscriptions,
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
                    // Best-effort: the session is closed/disposed regardless below.
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
            [LoggerMessage(EventId = 91020, Level = LogLevel.Warning,
                Message = "[Cluster] ActiveMQ subscription-event publish to topic '{Topic}' failed.")]
            public static partial void SubscriptionEventPublishFailed(ILogger logger, Exception exception, string topic);

            [LoggerMessage(EventId = 91021, Level = LogLevel.Warning,
                Message = "[Cluster] ActiveMQ subscription event could not be parsed; skipping it.")]
            public static partial void SubscriptionEventUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91022, Level = LogLevel.Error,
                Message = "[Cluster] ActiveMQ subscription-event handler faulted.")]
            public static partial void SubscriptionEventHandlerFaulted(ILogger logger, Exception exception);
        }
    }
}
