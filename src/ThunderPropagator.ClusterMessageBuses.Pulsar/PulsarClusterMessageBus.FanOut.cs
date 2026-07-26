using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.BuildingBlocks.Application.Helpers;

namespace ThunderPropagator.ClusterMessageBuses.Pulsar
{
    internal sealed partial class PulsarClusterMessageBus
    {
        public override async Task PublishAsync(Guid channelKey, ClusterFanOutMessage message, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var stamped = message with { OriginId = _selfId };
            var topic = PulsarTopicNaming.FanOutTopic(_options.TopicPrefix, channelKey);

            try
            {
                await _transport.PublishAsync(topic, stamped.ToNJson(), cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Log.FanOutPublishFailed(_logger, exception, topic);
            }
        }

        public override async Task<IAsyncDisposable> SubscribeAsync(Guid channelKey, Func<ClusterFanOutMessage, CancellationToken, Task> onMessage, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var topic = PulsarTopicNaming.FanOutTopic(_options.TopicPrefix, channelKey);
            var subscriptionName = $"{_options.SubscriptionPrefix}-fanout-{_subscriptionSuffix}";
            var loopCts = CancellationTokenSource.CreateLinkedTokenSource(_lifetimeCts.Token);
            var loopTask = Task.Run(() => RunFanOutSubscriptionLoopAsync(topic, subscriptionName, onMessage, loopCts.Token), loopCts.Token);

            var subscription = new FanOutSubscription(_fanOutSubscriptions, channelKey, loopCts, loopTask);
            _fanOutSubscriptions[channelKey] = subscription;

            return subscription;
        }

        /// <summary>Internal (rather than private) so tests can drive it directly against a fake transport.</summary>
        internal async Task RunFanOutSubscriptionLoopAsync(string topic, string subscriptionName, Func<ClusterFanOutMessage, CancellationToken, Task> onMessage, CancellationToken cancellationToken)
        {
            try
            {
                await foreach (var payload in _transport.SubscribeAsync(topic, subscriptionName, cancellationToken).ConfigureAwait(false))
                {
                    await HandleFanOutDeliveryAsync(payload, onMessage, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
                // Expected on shutdown/unsubscribe.
            }
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
            private readonly CancellationTokenSource _loopCts;
            private readonly Task _loopTask;

            internal FanOutSubscription(
                ConcurrentDictionary<Guid, FanOutSubscription> subscriptions,
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
            [LoggerMessage(EventId = 9810, Level = LogLevel.Warning,
                Message = "[Cluster] Pulsar fan-out publish to topic '{Topic}' failed.")]
            public static partial void FanOutPublishFailed(ILogger logger, Exception exception, string topic);

            [LoggerMessage(EventId = 9811, Level = LogLevel.Warning,
                Message = "[Cluster] Pulsar fan-out message could not be parsed; skipping it.")]
            public static partial void FanOutMessageUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 9812, Level = LogLevel.Error,
                Message = "[Cluster] Pulsar fan-out handler faulted.")]
            public static partial void FanOutHandlerFaulted(ILogger logger, Exception exception);
        }
    }
}
