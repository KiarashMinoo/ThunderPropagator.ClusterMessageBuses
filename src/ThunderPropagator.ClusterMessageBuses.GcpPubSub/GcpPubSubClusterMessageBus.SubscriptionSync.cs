using System.Collections.Concurrent;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.BuildingBlocks.Application.Helpers;

namespace ThunderPropagator.ClusterMessageBuses.GcpPubSub
{
    internal sealed partial class GcpPubSubClusterMessageBus
    {
        public override async Task PublishAsync(Guid channelKey, ClusterSubscriptionEvent subscriptionEvent, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var stamped = subscriptionEvent with { OriginId = _selfId };
            var topicName = new TopicName(_options.ProjectId, GcpPubSubResourceNaming.SubscriptionEventTopicId(_options.ResourcePrefix, channelKey));

            try
            {
                await EnsureTopicAsync(topicName, cancellationToken).ConfigureAwait(false);
                var pubsubMessage = new PubsubMessage { Data = ByteString.CopyFromUtf8(stamped.ToNJson()) };
                await _publisher!.PublishAsync(topicName, [pubsubMessage], cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Log.SubscriptionEventPublishFailed(_logger, exception, topicName.TopicId);
            }
        }

        public override async Task<IAsyncDisposable> SubscribeAsync(Guid channelKey, Func<ClusterSubscriptionEvent, CancellationToken, Task> onEvent, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var topicName = new TopicName(_options.ProjectId, GcpPubSubResourceNaming.SubscriptionEventTopicId(_options.ResourcePrefix, channelKey));
            var subscriptionName = new SubscriptionName(_options.ProjectId, GcpPubSubResourceNaming.SubscriptionEventSubscriptionId(_options.ResourcePrefix, _nodeEndpoint, channelKey));

            await EnsureTopicAsync(topicName, cancellationToken).ConfigureAwait(false);
            await EnsureSubscriptionAsync(subscriptionName, topicName, cancellationToken).ConfigureAwait(false);

            var subscriptionCts = CancellationTokenSource.CreateLinkedTokenSource(_lifetimeCts.Token);
            var pollTask = RunSubscriptionPollLoopAsync(subscriptionName, (body, ct) => HandleSubscriptionEventDeliveryAsync(body, onEvent, ct), subscriptionCts.Token);
            _backgroundTasks.Add(pollTask);

            var subscription = new SubscriptionEventSubscription(_subscriptionEventSubscriptions, channelKey, this, subscriptionName, subscriptionCts, pollTask);
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
            private readonly GcpPubSubClusterMessageBus _bus;
            private readonly SubscriptionName _subscriptionName;
            private readonly CancellationTokenSource _subscriptionCts;
            private readonly Task _pollTask;

            internal SubscriptionEventSubscription(
                ConcurrentDictionary<Guid, SubscriptionEventSubscription> subscriptions,
                Guid channelKey,
                GcpPubSubClusterMessageBus bus,
                SubscriptionName subscriptionName,
                CancellationTokenSource subscriptionCts,
                Task pollTask)
            {
                _subscriptions = subscriptions;
                _channelKey = channelKey;
                _bus = bus;
                _subscriptionName = subscriptionName;
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
                    // Best-effort — the subscription is torn down regardless below.
                }

                try
                {
                    await _bus._subscriber!.DeleteSubscriptionAsync(_subscriptionName).ConfigureAwait(false);
                }
                catch (Exception)
                {
                    // Same best-effort reasoning as above.
                }

                _subscriptionCts.Dispose();
            }
        }

        private static partial class Log
        {
            [LoggerMessage(EventId = 9820, Level = LogLevel.Warning,
                Message = "[Cluster] GcpPubSub subscription-event publish to topic '{Topic}' failed.")]
            public static partial void SubscriptionEventPublishFailed(ILogger logger, Exception exception, string topic);

            [LoggerMessage(EventId = 9821, Level = LogLevel.Warning,
                Message = "[Cluster] GcpPubSub subscription event could not be parsed; skipping it.")]
            public static partial void SubscriptionEventUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 9822, Level = LogLevel.Error,
                Message = "[Cluster] GcpPubSub subscription-event handler faulted.")]
            public static partial void SubscriptionEventHandlerFaulted(ILogger logger, Exception exception);
        }
    }
}
