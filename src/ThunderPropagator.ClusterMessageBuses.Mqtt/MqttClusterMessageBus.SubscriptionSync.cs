using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.BuildingBlocks.Application.Helpers;

namespace ThunderPropagator.ClusterMessageBuses.Mqtt
{
    internal sealed partial class MqttClusterMessageBus
    {
        public override async Task PublishAsync(Guid channelKey, ClusterSubscriptionEvent subscriptionEvent, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var stamped = subscriptionEvent with { OriginId = _selfId };
            var topic = MqttTopicNaming.SubscriptionEventTopic(_options.TopicPrefix, channelKey);

            try
            {
                await _transport.PublishAsync(topic, stamped.ToNJson(), cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Log.SubscriptionEventPublishFailed(_logger, exception, topic);
            }
        }

        public override async Task<IAsyncDisposable> SubscribeAsync(Guid channelKey, Func<ClusterSubscriptionEvent, CancellationToken, Task> onEvent, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var topic = MqttTopicNaming.SubscriptionEventTopic(_options.TopicPrefix, channelKey);
            var loopCts = CancellationTokenSource.CreateLinkedTokenSource(_lifetimeCts.Token);
            var loopTask = Task.Run(() => RunSubscriptionEventSubscriptionLoopAsync(topic, onEvent, loopCts.Token), loopCts.Token);

            var subscription = new SubscriptionEventSubscription(_subscriptionEventSubscriptions, channelKey, loopCts, loopTask);
            _subscriptionEventSubscriptions[channelKey] = subscription;

            return subscription;
        }

        /// <summary>Internal (rather than private) so tests can drive it directly against a fake transport.</summary>
        internal async Task RunSubscriptionEventSubscriptionLoopAsync(string topic, Func<ClusterSubscriptionEvent, CancellationToken, Task> onEvent, CancellationToken cancellationToken)
        {
            try
            {
                await foreach (var payload in _transport.SubscribeAsync(topic, cancellationToken).ConfigureAwait(false))
                {
                    await HandleSubscriptionEventDeliveryAsync(payload, onEvent, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
                // Expected on shutdown/unsubscribe.
            }
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
            private readonly CancellationTokenSource _loopCts;
            private readonly Task _loopTask;

            internal SubscriptionEventSubscription(
                ConcurrentDictionary<Guid, SubscriptionEventSubscription> subscriptions,
                Guid channelKey,
                CancellationTokenSource loopCts,
                Task loopTask)
            {
                _subscriptions = subscriptions;
                _channelKey = channelKey;
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
                    // Best-effort: the loop is cancelled regardless.
                }

                _loopCts.Dispose();
            }
        }

        private static partial class Log
        {
            [LoggerMessage(EventId = 9920, Level = LogLevel.Warning,
                Message = "[Cluster] MQTT subscription-event publish to topic '{Topic}' failed.")]
            public static partial void SubscriptionEventPublishFailed(ILogger logger, Exception exception, string topic);

            [LoggerMessage(EventId = 9921, Level = LogLevel.Warning,
                Message = "[Cluster] MQTT subscription event could not be parsed; skipping it.")]
            public static partial void SubscriptionEventUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 9922, Level = LogLevel.Error,
                Message = "[Cluster] MQTT subscription-event handler faulted.")]
            public static partial void SubscriptionEventHandlerFaulted(ILogger logger, Exception exception);
        }
    }
}
