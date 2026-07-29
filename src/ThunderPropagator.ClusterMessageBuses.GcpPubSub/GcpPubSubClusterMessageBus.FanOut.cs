using System.Collections.Concurrent;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.BuildingBlocks.Application.Helpers;

namespace ThunderPropagator.ClusterMessageBuses.GcpPubSub
{
    internal sealed partial class GcpPubSubClusterMessageBus
    {
        public override async Task PublishAsync(Guid channelKey, ClusterFanOutMessage message, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var stamped = message with { OriginId = _selfId };
            var topicName = new TopicName(_options.ProjectId, GcpPubSubResourceNaming.FanOutTopicId(_options.ResourcePrefix, channelKey));

            try
            {
                await EnsureTopicAsync(topicName, cancellationToken).ConfigureAwait(false);
                var pubsubMessage = new PubsubMessage { Data = ByteString.CopyFromUtf8(stamped.ToNJson()) };
                await _publisher!.PublishAsync(topicName, [pubsubMessage], cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Log.FanOutPublishFailed(_logger, exception, topicName.TopicId);
            }
        }

        public override async Task<IAsyncDisposable> SubscribeAsync(Guid channelKey, Func<ClusterFanOutMessage, CancellationToken, Task> onMessage, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var topicName = new TopicName(_options.ProjectId, GcpPubSubResourceNaming.FanOutTopicId(_options.ResourcePrefix, channelKey));
            var subscriptionName = new SubscriptionName(_options.ProjectId, GcpPubSubResourceNaming.FanOutSubscriptionId(_options.ResourcePrefix, _nodeEndpoint, channelKey));

            await EnsureTopicAsync(topicName, cancellationToken).ConfigureAwait(false);
            await EnsureSubscriptionAsync(subscriptionName, topicName, cancellationToken).ConfigureAwait(false);

            var subscriptionCts = CancellationTokenSource.CreateLinkedTokenSource(_lifetimeCts.Token);
            var pollTask = RunSubscriptionPollLoopAsync(subscriptionName, (body, ct) => HandleFanOutDeliveryAsync(body, onMessage, ct), subscriptionCts.Token);
            _backgroundTasks.Add(pollTask);

            var subscription = new FanOutSubscription(_fanOutSubscriptions, channelKey, this, subscriptionName, subscriptionCts, pollTask);
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
            private readonly GcpPubSubClusterMessageBus _bus;
            private readonly SubscriptionName _subscriptionName;
            private readonly CancellationTokenSource _subscriptionCts;
            private readonly Task _pollTask;

            internal FanOutSubscription(
                ConcurrentDictionary<Guid, FanOutSubscription> subscriptions,
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
                    // Best-effort: the subscription may already be gone if the topic was deleted
                    // out-of-band.
                }

                _subscriptionCts.Dispose();
            }
        }

        private static partial class Log
        {
            [LoggerMessage(EventId = 9810, Level = LogLevel.Warning,
                Message = "[Cluster] GcpPubSub fan-out publish to topic '{Topic}' failed.")]
            public static partial void FanOutPublishFailed(ILogger logger, Exception exception, string topic);

            [LoggerMessage(EventId = 9811, Level = LogLevel.Warning,
                Message = "[Cluster] GcpPubSub fan-out message could not be parsed; skipping it.")]
            public static partial void FanOutMessageUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 9812, Level = LogLevel.Error,
                Message = "[Cluster] GcpPubSub fan-out handler faulted.")]
            public static partial void FanOutHandlerFaulted(ILogger logger, Exception exception);
        }
    }
}
