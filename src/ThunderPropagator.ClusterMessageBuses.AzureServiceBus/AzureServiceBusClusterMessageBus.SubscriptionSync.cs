using System.Collections.Concurrent;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.BuildingBlocks.Application.Helpers;

namespace ThunderPropagator.ClusterMessageBuses.AzureServiceBus
{
    internal sealed partial class AzureServiceBusClusterMessageBus
    {
        public override async Task PublishAsync(Guid channelKey, ClusterSubscriptionEvent subscriptionEvent, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var stamped = subscriptionEvent with { OriginId = _selfId };
            var topicName = AzureServiceBusResourceNaming.SubscriptionEventTopic(_options.ResourcePrefix, channelKey);

            try
            {
                await EnsureTopicAsync(topicName, cancellationToken).ConfigureAwait(false);
                await GetSender(topicName).SendMessageAsync(new ServiceBusMessage(stamped.ToNJson()), cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Log.SubscriptionEventPublishFailed(_logger, exception, topicName);
            }
        }

        public override async Task<IAsyncDisposable> SubscribeAsync(Guid channelKey, Func<ClusterSubscriptionEvent, CancellationToken, Task> onEvent, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var topicName = AzureServiceBusResourceNaming.SubscriptionEventTopic(_options.ResourcePrefix, channelKey);
            var subscriptionName = AzureServiceBusResourceNaming.SubscriptionEventSubscription(_nodeEndpoint, channelKey);

            await EnsureTopicAsync(topicName, cancellationToken).ConfigureAwait(false);
            await EnsureSubscriptionAsync(topicName, subscriptionName, cancellationToken).ConfigureAwait(false);

            var receiver = _client!.CreateReceiver(topicName, subscriptionName);

            var subscriptionCts = CancellationTokenSource.CreateLinkedTokenSource(_lifetimeCts.Token);
            var pollTask = RunReceiverPollLoopAsync(receiver, (body, ct) => HandleSubscriptionEventDeliveryAsync(body, onEvent, ct), subscriptionCts.Token);
            _backgroundTasks.Add(pollTask);

            var subscription = new SubscriptionEventSubscription(_subscriptionEventSubscriptions, channelKey, this, topicName, subscriptionName, receiver, subscriptionCts, pollTask);
            _subscriptionEventSubscriptions[channelKey] = subscription;

            return subscription;
        }

        /// <summary>Internal (rather than private) so tests can drive it directly with a raw message body.</summary>
        internal async Task HandleSubscriptionEventDeliveryAsync(string body, Func<ClusterSubscriptionEvent, CancellationToken, Task> onEvent, CancellationToken cancellationToken)
        {
            ClusterSubscriptionEvent? subscriptionEvent;
            try
            {
                subscriptionEvent = body.FromNJson<ClusterSubscriptionEvent>();
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
            private readonly AzureServiceBusClusterMessageBus _bus;
            private readonly string _topicName;
            private readonly string _subscriptionName;
            private readonly ServiceBusReceiver _receiver;
            private readonly CancellationTokenSource _subscriptionCts;
            private readonly Task _pollTask;

            internal SubscriptionEventSubscription(
                ConcurrentDictionary<Guid, SubscriptionEventSubscription> subscriptions,
                Guid channelKey,
                AzureServiceBusClusterMessageBus bus,
                string topicName,
                string subscriptionName,
                ServiceBusReceiver receiver,
                CancellationTokenSource subscriptionCts,
                Task pollTask)
            {
                _subscriptions = subscriptions;
                _channelKey = channelKey;
                _bus = bus;
                _topicName = topicName;
                _subscriptionName = subscriptionName;
                _receiver = receiver;
                _subscriptionCts = subscriptionCts;
                _pollTask = pollTask;
            }

            public async ValueTask DisposeAsync()
            {
                _subscriptions.TryRemove(_channelKey, out _);

                await _subscriptionCts.CancelAsync().ConfigureAwait(false);
                try
                {
                    await _pollTask.ConfigureAwait(false);
                }
                catch (Exception)
                {
                    // Best-effort — the receiver/subscription are torn down regardless below.
                }

                try
                {
                    await _receiver.DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception)
                {
                    // Best-effort.
                }

                try
                {
                    await _bus._admin!.DeleteSubscriptionAsync(_topicName, _subscriptionName).ConfigureAwait(false);
                }
                catch (Exception)
                {
                    // Best-effort: the subscription may already be gone if the topic was deleted
                    // out-of-band.
                }

                _subscriptionCts.Dispose();
            }
        }

        private static partial class Log
        {
            [LoggerMessage(EventId = 9820, Level = LogLevel.Warning,
                Message = "[Cluster] AzureServiceBus subscription-event publish to topic '{Topic}' failed.")]
            public static partial void SubscriptionEventPublishFailed(ILogger logger, Exception exception, string topic);

            [LoggerMessage(EventId = 9821, Level = LogLevel.Warning,
                Message = "[Cluster] AzureServiceBus subscription event could not be parsed; skipping it.")]
            public static partial void SubscriptionEventUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 9822, Level = LogLevel.Error,
                Message = "[Cluster] AzureServiceBus subscription-event handler faulted.")]
            public static partial void SubscriptionEventHandlerFaulted(ILogger logger, Exception exception);
        }
    }
}
