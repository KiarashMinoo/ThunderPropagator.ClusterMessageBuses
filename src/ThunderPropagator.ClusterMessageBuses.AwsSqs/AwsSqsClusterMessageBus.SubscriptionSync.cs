using System.Collections.Concurrent;
using Amazon.SimpleNotificationService.Model;
using Microsoft.Extensions.Logging;
using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.BuildingBlocks.Application.Helpers;

namespace ThunderPropagator.ClusterMessageBuses.AwsSqs
{
    internal sealed partial class AwsSqsClusterMessageBus
    {
        public override async Task PublishAsync(Guid channelKey, ClusterSubscriptionEvent subscriptionEvent, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var stamped = subscriptionEvent with { OriginId = _selfId };
            var topicName = AwsSqsResourceNaming.SubscriptionEventTopic(_options.ResourcePrefix, channelKey);

            try
            {
                var topicArn = await EnsureTopicAsync(topicName, cancellationToken).ConfigureAwait(false);
                await _sns!.PublishAsync(new PublishRequest { TopicArn = topicArn, Message = stamped.ToNJson() }, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Log.SubscriptionEventPublishFailed(_logger, exception, topicName);
            }
        }

        public override async Task<IAsyncDisposable> SubscribeAsync(Guid channelKey, Func<ClusterSubscriptionEvent, CancellationToken, Task> onEvent, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var topicName = AwsSqsResourceNaming.SubscriptionEventTopic(_options.ResourcePrefix, channelKey);
            var queueName = AwsSqsResourceNaming.SubscriptionEventQueue(_options.ResourcePrefix, _nodeEndpoint, channelKey);

            var topicArn = await EnsureTopicAsync(topicName, cancellationToken).ConfigureAwait(false);
            var queueUrl = await EnsureQueueAsync(queueName, cancellationToken).ConfigureAwait(false);
            var queueArn = await GetQueueArnAsync(queueUrl, cancellationToken).ConfigureAwait(false);

            await GrantTopicPublishToQueueAsync(queueUrl, queueArn, topicArn, cancellationToken).ConfigureAwait(false);
            var subscriptionArn = await SubscribeQueueToTopicAsync(topicArn, queueArn, cancellationToken).ConfigureAwait(false);

            var subscriptionCts = CancellationTokenSource.CreateLinkedTokenSource(_lifetimeCts.Token);
            var pollTask = RunQueuePollLoopAsync(queueUrl, (body, ct) => HandleSubscriptionEventDeliveryAsync(body, onEvent, ct), subscriptionCts.Token);
            _backgroundTasks.Add(pollTask);

            var subscription = new SubscriptionEventSubscription(_subscriptionEventSubscriptions, channelKey, this, queueUrl, subscriptionArn, subscriptionCts, pollTask);
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
            private readonly AwsSqsClusterMessageBus _bus;
            private readonly string _queueUrl;
            private readonly string _subscriptionArn;
            private readonly CancellationTokenSource _subscriptionCts;
            private readonly Task _pollTask;

            internal SubscriptionEventSubscription(
                ConcurrentDictionary<Guid, SubscriptionEventSubscription> subscriptions,
                Guid channelKey,
                AwsSqsClusterMessageBus bus,
                string queueUrl,
                string subscriptionArn,
                CancellationTokenSource subscriptionCts,
                Task pollTask)
            {
                _subscriptions = subscriptions;
                _channelKey = channelKey;
                _bus = bus;
                _queueUrl = queueUrl;
                _subscriptionArn = subscriptionArn;
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
                    // Best-effort — the queue/subscription are torn down regardless below.
                }

                try
                {
                    await _bus._sns!.UnsubscribeAsync(new UnsubscribeRequest { SubscriptionArn = _subscriptionArn }).ConfigureAwait(false);
                }
                catch (Exception)
                {
                    // Best-effort: the SNS subscription may already be gone if the topic/queue was
                    // deleted out-of-band.
                }

                try
                {
                    await _bus._sqs!.DeleteQueueAsync(new Amazon.SQS.Model.DeleteQueueRequest { QueueUrl = _queueUrl }).ConfigureAwait(false);
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
            [LoggerMessage(EventId = 9720, Level = LogLevel.Warning,
                Message = "[Cluster] AwsSqs subscription-event publish to topic '{Topic}' failed.")]
            public static partial void SubscriptionEventPublishFailed(ILogger logger, Exception exception, string topic);

            [LoggerMessage(EventId = 9721, Level = LogLevel.Warning,
                Message = "[Cluster] AwsSqs subscription event could not be parsed; skipping it.")]
            public static partial void SubscriptionEventUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 9722, Level = LogLevel.Error,
                Message = "[Cluster] AwsSqs subscription-event handler faulted.")]
            public static partial void SubscriptionEventHandlerFaulted(ILogger logger, Exception exception);
        }
    }
}
