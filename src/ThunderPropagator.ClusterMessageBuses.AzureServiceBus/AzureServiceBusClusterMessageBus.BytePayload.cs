using System.Collections.Concurrent;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.AzureServiceBus
{
    /// <summary>
    /// Byte-oriented counterpart to <c>AzureServiceBusClusterMessageBus.FanOut.cs</c> /
    /// <c>.RequestReply.cs</c> -- see <see cref="ClusterByteMessage"/>'s own doc comment for why
    /// this surface exists alongside, not instead of, the <c>ClusterFanOutMessage</c>-based one.
    /// Fan-out uses its own per-channel topic (<see cref="AzureServiceBusResourceNaming.ByteFanOutTopic"/>)
    /// with, like <c>FanOut.cs</c>, every node creating its own exclusive topic subscription
    /// (<see cref="AzureServiceBusResourceNaming.ByteFanOutSubscription"/>); the snapshot pull
    /// reuses the existing broker-native request/reply plumbing in
    /// <c>AzureServiceBusClusterMessageBus.RequestReply.cs</c> via a new
    /// <see cref="ClusterRequestKind.PullSnapshotBytes"/> kind, exactly like
    /// <c>RestoreFromLeaderAsync</c> reuses it for <see cref="ClusterRequestKind.RestoreSnapshot"/>.
    /// </summary>
    internal sealed partial class AzureServiceBusClusterMessageBus
    {
        public override async Task PublishAsync(Guid channelKey, ClusterByteMessage message, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var stamped = message with { OriginId = _selfId };
            var topicName = AzureServiceBusResourceNaming.ByteFanOutTopic(_options.ResourcePrefix, channelKey);

            try
            {
                await EnsureTopicAsync(topicName, cancellationToken).ConfigureAwait(false);
                await GetSender(topicName).SendMessageAsync(new ServiceBusMessage(stamped.ToNJson()), cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Log.ByteFanOutPublishFailed(_logger, exception, topicName);
            }
        }

        public override async Task<IAsyncDisposable> SubscribeAsync(Guid channelKey, Func<ClusterByteMessage, CancellationToken, Task> onMessage, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var topicName = AzureServiceBusResourceNaming.ByteFanOutTopic(_options.ResourcePrefix, channelKey);
            var subscriptionName = AzureServiceBusResourceNaming.ByteFanOutSubscription(_nodeEndpoint, channelKey);

            await EnsureTopicAsync(topicName, cancellationToken).ConfigureAwait(false);
            await EnsureSubscriptionAsync(topicName, subscriptionName, cancellationToken).ConfigureAwait(false);

            var receiver = _client!.CreateReceiver(topicName, subscriptionName);

            var subscriptionCts = CancellationTokenSource.CreateLinkedTokenSource(_lifetimeCts.Token);
            var pollTask = RunReceiverPollLoopAsync(receiver, (body, ct) => HandleByteFanOutDeliveryAsync(body, onMessage, ct), subscriptionCts.Token);
            _backgroundTasks.Add(pollTask);

            var subscription = new ByteFanOutSubscription(_byteFanOutSubscriptions, channelKey, this, topicName, subscriptionName, receiver, subscriptionCts, pollTask);
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
            private readonly AzureServiceBusClusterMessageBus _bus;
            private readonly string _topicName;
            private readonly string _subscriptionName;
            private readonly ServiceBusReceiver _receiver;
            private readonly CancellationTokenSource _subscriptionCts;
            private readonly Task _pollTask;

            internal ByteFanOutSubscription(
                ConcurrentDictionary<Guid, ByteFanOutSubscription> subscriptions,
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
            [LoggerMessage(EventId = 9860, Level = LogLevel.Warning,
                Message = "[Cluster] AzureServiceBus byte fan-out publish to topic '{Topic}' failed.")]
            public static partial void ByteFanOutPublishFailed(ILogger logger, Exception exception, string topic);

            [LoggerMessage(EventId = 9861, Level = LogLevel.Warning,
                Message = "[Cluster] AzureServiceBus byte fan-out message could not be parsed; skipping it.")]
            public static partial void ByteFanOutMessageUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 9862, Level = LogLevel.Error,
                Message = "[Cluster] AzureServiceBus byte fan-out handler faulted.")]
            public static partial void ByteFanOutHandlerFaulted(ILogger logger, Exception exception);
        }
    }
}
