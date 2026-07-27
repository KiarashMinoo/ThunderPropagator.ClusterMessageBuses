using System.Collections.Concurrent;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.BuildingBlocks.Application.Helpers;

namespace ThunderPropagator.ClusterMessageBuses.AzureServiceBus
{
    internal sealed partial class AzureServiceBusClusterMessageBus
    {
        public override async Task PublishAsync(Guid channelKey, ClusterFanOutMessage message, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var stamped = message with { OriginId = _selfId };
            var topicName = AzureServiceBusResourceNaming.FanOutTopic(_options.ResourcePrefix, channelKey);

            try
            {
                await EnsureTopicAsync(topicName, cancellationToken).ConfigureAwait(false);
                await GetSender(topicName).SendMessageAsync(new ServiceBusMessage(stamped.ToNJson()), cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Log.FanOutPublishFailed(_logger, exception, topicName);
            }
        }

        public override async Task<IAsyncDisposable> SubscribeAsync(Guid channelKey, Func<ClusterFanOutMessage, CancellationToken, Task> onMessage, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var topicName = AzureServiceBusResourceNaming.FanOutTopic(_options.ResourcePrefix, channelKey);
            var subscriptionName = AzureServiceBusResourceNaming.FanOutSubscription(_nodeEndpoint, channelKey);

            await EnsureTopicAsync(topicName, cancellationToken).ConfigureAwait(false);
            await EnsureSubscriptionAsync(topicName, subscriptionName, cancellationToken).ConfigureAwait(false);

            var receiver = _client!.CreateReceiver(topicName, subscriptionName);

            var subscriptionCts = CancellationTokenSource.CreateLinkedTokenSource(_lifetimeCts.Token);
            var pollTask = RunReceiverPollLoopAsync(receiver, (body, ct) => HandleFanOutDeliveryAsync(body, onMessage, ct), subscriptionCts.Token);
            _backgroundTasks.Add(pollTask);

            var subscription = new FanOutSubscription(_fanOutSubscriptions, channelKey, this, topicName, subscriptionName, receiver, subscriptionCts, pollTask);
            _fanOutSubscriptions[channelKey] = subscription;

            return subscription;
        }

        /// <summary>Internal (rather than private) so tests can drive it directly with a raw message body.</summary>
        internal async Task HandleFanOutDeliveryAsync(string body, Func<ClusterFanOutMessage, CancellationToken, Task> onMessage, CancellationToken cancellationToken)
        {
            ClusterFanOutMessage? message;
            try
            {
                message = body.FromNJson<ClusterFanOutMessage>();
            }
            catch (Exception exception)
            {
                // A single malformed delivery must not take down an otherwise-healthy subscription.
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
            private readonly AzureServiceBusClusterMessageBus _bus;
            private readonly string _topicName;
            private readonly string _subscriptionName;
            private readonly ServiceBusReceiver _receiver;
            private readonly CancellationTokenSource _subscriptionCts;
            private readonly Task _pollTask;

            internal FanOutSubscription(
                ConcurrentDictionary<Guid, FanOutSubscription> subscriptions,
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
            [LoggerMessage(EventId = 9810, Level = LogLevel.Warning,
                Message = "[Cluster] AzureServiceBus fan-out publish to topic '{Topic}' failed.")]
            public static partial void FanOutPublishFailed(ILogger logger, Exception exception, string topic);

            [LoggerMessage(EventId = 9811, Level = LogLevel.Warning,
                Message = "[Cluster] AzureServiceBus fan-out message could not be parsed; skipping it.")]
            public static partial void FanOutMessageUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 9812, Level = LogLevel.Error,
                Message = "[Cluster] AzureServiceBus fan-out handler faulted.")]
            public static partial void FanOutHandlerFaulted(ILogger logger, Exception exception);
        }
    }
}
