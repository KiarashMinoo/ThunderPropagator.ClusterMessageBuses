using System.Collections.Concurrent;
using Amazon.SimpleNotificationService.Model;
using Microsoft.Extensions.Logging;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.AwsSqs
{
    /// <summary>
    /// Byte-oriented counterpart to <c>AwsSqsClusterMessageBus.FanOut.cs</c> /
    /// <c>.RequestReply.cs</c> -- see <see cref="ClusterByteMessage"/>'s own doc comment for why
    /// this surface exists alongside, not instead of, the <c>ClusterFanOutMessage</c>-based one.
    /// Fan-out follows the identical SNS-topic-plus-per-node-exclusive-SQS-queue operational
    /// pattern as <c>AwsSqsClusterMessageBus.FanOut.cs</c>, just under its own resource-name
    /// namespace (<see cref="AwsSqsResourceNaming.ByteFanOutTopic"/> / <see cref="AwsSqsResourceNaming.ByteFanOutQueue"/>)
    /// so the two payload shapes never collide on the wire. The snapshot pull reuses the existing
    /// broker-native request/reply plumbing in <c>AwsSqsClusterMessageBus.RequestReply.cs</c> via a
    /// new <see cref="ClusterRequestKind.PullSnapshotBytes"/> kind, exactly like
    /// <c>RestoreFromLeaderAsync</c> reuses it for <see cref="ClusterRequestKind.RestoreSnapshot"/>.
    /// </summary>
    internal sealed partial class AwsSqsClusterMessageBus
    {
        public override async Task PublishAsync(Guid channelKey, ClusterByteMessage message, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var stamped = message with { OriginId = _selfId };
            var topicName = AwsSqsResourceNaming.ByteFanOutTopic(_options.ResourcePrefix, channelKey);

            try
            {
                var topicArn = await EnsureTopicAsync(topicName, cancellationToken).ConfigureAwait(false);
                await _sns!.PublishAsync(new PublishRequest { TopicArn = topicArn, Message = stamped.ToNJson() }, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Log.ByteFanOutPublishFailed(_logger, exception, topicName);
            }
        }

        public override async Task<IAsyncDisposable> SubscribeAsync(Guid channelKey, Func<ClusterByteMessage, CancellationToken, Task> onMessage, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var topicName = AwsSqsResourceNaming.ByteFanOutTopic(_options.ResourcePrefix, channelKey);
            var queueName = AwsSqsResourceNaming.ByteFanOutQueue(_options.ResourcePrefix, _nodeEndpoint, channelKey);

            var topicArn = await EnsureTopicAsync(topicName, cancellationToken).ConfigureAwait(false);
            var queueUrl = await EnsureQueueAsync(queueName, cancellationToken).ConfigureAwait(false);
            var queueArn = await GetQueueArnAsync(queueUrl, cancellationToken).ConfigureAwait(false);

            await GrantTopicPublishToQueueAsync(queueUrl, queueArn, topicArn, cancellationToken).ConfigureAwait(false);
            var subscriptionArn = await SubscribeQueueToTopicAsync(topicArn, queueArn, cancellationToken).ConfigureAwait(false);

            var subscriptionCts = CancellationTokenSource.CreateLinkedTokenSource(_lifetimeCts.Token);
            var pollTask = RunQueuePollLoopAsync(queueUrl, (body, ct) => HandleByteFanOutDeliveryAsync(body, onMessage, ct), subscriptionCts.Token);
            _backgroundTasks.Add(pollTask);

            var subscription = new ByteFanOutSubscription(_byteFanOutSubscriptions, channelKey, this, queueUrl, subscriptionArn, subscriptionCts, pollTask);
            _byteFanOutSubscriptions[channelKey] = subscription;

            return subscription;
        }

        /// <summary>Internal (rather than private) so tests can drive it directly with a raw message body.</summary>
        internal async Task HandleByteFanOutDeliveryAsync(string body, Func<ClusterByteMessage, CancellationToken, Task> onMessage, CancellationToken cancellationToken)
        {
            ClusterByteMessage? message;
            try
            {
                message = body.FromNJson<ClusterByteMessage>();
            }
            catch (Exception exception)
            {
                // A single malformed delivery must not take down an otherwise-healthy subscription.
                Log.ByteFanOutMessageUnparseable(_logger, exception);
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
                Log.ByteFanOutHandlerFaulted(_logger, exception);
            }
        }

        public override async Task<byte[]?> PullSnapshotAsync(Uri leaderEndpoint, Guid channelKey, CancellationToken cancellationToken = default)
        {
            var response = await SendRequestAsync(
                leaderEndpoint, ClusterRequestKind.PullSnapshotBytes, channelName: null, channelKey, sinceTicks: null, cancellationToken)
                .ConfigureAwait(false);

            return response.PayloadJson?.FromNJson<byte[]?>();
        }

        /// <summary>
        /// Answering side of <see cref="PullSnapshotAsync"/>. Returns a successful response with a
        /// JSON-null payload when no <see cref="IClusterByteSnapshotProvider"/> is registered, or it
        /// has nothing to offer yet -- mirrored by <see cref="PullSnapshotAsync"/> deserializing
        /// that back to a null byte[], exactly like <c>BuildRestoreSnapshotResponseAsync</c>
        /// answers with an empty array rather than a failure when a channel has nothing to restore.
        /// </summary>
        private async Task<ClusterResponseEnvelope> BuildPullSnapshotBytesResponseAsync(ClusterRequestEnvelope request, CancellationToken cancellationToken)
        {
            if (_byteSnapshotProvider is null)
                return new ClusterResponseEnvelope(request.CorrelationId, true, null, "null");

            var snapshot = await _byteSnapshotProvider.GetSnapshotAsync(request.ChannelKey!.Value, cancellationToken).ConfigureAwait(false);
            return new ClusterResponseEnvelope(request.CorrelationId, true, null, snapshot.ToNJson());
        }

        private sealed class ByteFanOutSubscription : IAsyncDisposable
        {
            private readonly ConcurrentDictionary<Guid, ByteFanOutSubscription> _subscriptions;
            private readonly Guid _channelKey;
            private readonly AwsSqsClusterMessageBus _bus;
            private readonly string _queueUrl;
            private readonly string _subscriptionArn;
            private readonly CancellationTokenSource _subscriptionCts;
            private readonly Task _pollTask;

            internal ByteFanOutSubscription(
                ConcurrentDictionary<Guid, ByteFanOutSubscription> subscriptions,
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
            [LoggerMessage(EventId = 9760, Level = LogLevel.Warning,
                Message = "[Cluster] AwsSqs byte fan-out publish to topic '{Topic}' failed.")]
            public static partial void ByteFanOutPublishFailed(ILogger logger, Exception exception, string topic);

            [LoggerMessage(EventId = 9761, Level = LogLevel.Warning,
                Message = "[Cluster] AwsSqs byte fan-out message could not be parsed; skipping it.")]
            public static partial void ByteFanOutMessageUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 9762, Level = LogLevel.Error,
                Message = "[Cluster] AwsSqs byte fan-out handler faulted.")]
            public static partial void ByteFanOutHandlerFaulted(ILogger logger, Exception exception);
        }
    }
}
