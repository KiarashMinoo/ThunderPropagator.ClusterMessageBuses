using System.Collections.Concurrent;
using Amazon.SimpleNotificationService.Model;
using Microsoft.Extensions.Logging;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.BuildingBlocks.Application.Helpers;

namespace ThunderPropagator.ClusterMessageBuses.AwsSqs
{
    internal sealed partial class AwsSqsClusterMessageBus
    {
        public override async Task PublishAsync(Guid channelKey, ClusterFanOutMessage message, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var stamped = message with { OriginId = _selfId };
            var topicName = AwsSqsResourceNaming.FanOutTopic(_options.ResourcePrefix, channelKey);

            try
            {
                var topicArn = await EnsureTopicAsync(topicName, cancellationToken).ConfigureAwait(false);
                await _sns!.PublishAsync(new PublishRequest { TopicArn = topicArn, Message = stamped.ToNJson() }, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Log.FanOutPublishFailed(_logger, exception, topicName);
            }
        }

        public override async Task<IAsyncDisposable> SubscribeAsync(Guid channelKey, Func<ClusterFanOutMessage, CancellationToken, Task> onMessage, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var topicName = AwsSqsResourceNaming.FanOutTopic(_options.ResourcePrefix, channelKey);
            var queueName = AwsSqsResourceNaming.FanOutQueue(_options.ResourcePrefix, _nodeEndpoint, channelKey);

            var topicArn = await EnsureTopicAsync(topicName, cancellationToken).ConfigureAwait(false);
            var queueUrl = await EnsureQueueAsync(queueName, cancellationToken).ConfigureAwait(false);
            var queueArn = await GetQueueArnAsync(queueUrl, cancellationToken).ConfigureAwait(false);

            await GrantTopicPublishToQueueAsync(queueUrl, queueArn, topicArn, cancellationToken).ConfigureAwait(false);
            var subscriptionArn = await SubscribeQueueToTopicAsync(topicArn, queueArn, cancellationToken).ConfigureAwait(false);

            var subscriptionCts = CancellationTokenSource.CreateLinkedTokenSource(_lifetimeCts.Token);
            var pollTask = RunQueuePollLoopAsync(queueUrl, (body, ct) => HandleFanOutDeliveryAsync(body, onMessage, ct), subscriptionCts.Token);
            _backgroundTasks.Add(pollTask);

            var subscription = new FanOutSubscription(_fanOutSubscriptions, channelKey, this, queueUrl, subscriptionArn, subscriptionCts, pollTask);
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
            private readonly AwsSqsClusterMessageBus _bus;
            private readonly string _queueUrl;
            private readonly string _subscriptionArn;
            private readonly CancellationTokenSource _subscriptionCts;
            private readonly Task _pollTask;

            internal FanOutSubscription(
                ConcurrentDictionary<Guid, FanOutSubscription> subscriptions,
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
            [LoggerMessage(EventId = 9710, Level = LogLevel.Warning,
                Message = "[Cluster] AwsSqs fan-out publish to topic '{Topic}' failed.")]
            public static partial void FanOutPublishFailed(ILogger logger, Exception exception, string topic);

            [LoggerMessage(EventId = 9711, Level = LogLevel.Warning,
                Message = "[Cluster] AwsSqs fan-out message could not be parsed; skipping it.")]
            public static partial void FanOutMessageUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 9712, Level = LogLevel.Error,
                Message = "[Cluster] AwsSqs fan-out handler faulted.")]
            public static partial void FanOutHandlerFaulted(ILogger logger, Exception exception);
        }
    }
}
