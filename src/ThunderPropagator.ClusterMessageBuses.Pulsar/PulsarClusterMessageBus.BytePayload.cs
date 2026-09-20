using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.Pulsar
{
    /// <summary>
    /// Byte-oriented counterpart to <c>PulsarClusterMessageBus.FanOut.cs</c> /
    /// <c>.RequestReply.cs</c> -- see <see cref="ClusterByteMessage"/>'s own doc comment for why
    /// this surface exists alongside, not instead of, the <c>ClusterFanOutMessage</c>-based one.
    /// Fan-out uses its own per-channel topic (<see cref="PulsarTopicNaming.ByteFanOutTopic"/>);
    /// the snapshot pull reuses the existing hand-rolled request/reply plumbing in
    /// <c>PulsarClusterMessageBus.RequestReply.cs</c> via a new <see cref="ClusterRequestKind.PullSnapshotBytes"/>
    /// kind, exactly like <c>RestoreFromLeaderAsync</c> reuses it for <see cref="ClusterRequestKind.RestoreSnapshot"/>.
    /// </summary>
    internal sealed partial class PulsarClusterMessageBus
    {
        public override async Task PublishAsync(Guid channelKey, ClusterByteMessage message, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var stamped = message with { OriginId = _selfId };
            var topic = PulsarTopicNaming.ByteFanOutTopic(_options.TopicPrefix, channelKey);

            try
            {
                await _transport.PublishAsync(topic, stamped.ToNJson(), cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Log.ByteFanOutPublishFailed(_logger, exception, topic);
            }
        }

        public override async Task<IAsyncDisposable> SubscribeAsync(Guid channelKey, Func<ClusterByteMessage, CancellationToken, Task> onMessage, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var topic = PulsarTopicNaming.ByteFanOutTopic(_options.TopicPrefix, channelKey);
            var subscriptionName = $"{_options.SubscriptionPrefix}-bytefanout-{_subscriptionSuffix}";
            var loopCts = CancellationTokenSource.CreateLinkedTokenSource(_lifetimeCts.Token);
            var loopTask = Task.Run(() => RunByteFanOutSubscriptionLoopAsync(topic, subscriptionName, onMessage, loopCts.Token), loopCts.Token);

            var subscription = new ByteFanOutSubscription(_byteFanOutSubscriptions, channelKey, loopCts, loopTask);
            _byteFanOutSubscriptions[channelKey] = subscription;

            return subscription;
        }

        /// <summary>Internal (rather than private) so tests can drive it directly against a fake transport.</summary>
        internal async Task RunByteFanOutSubscriptionLoopAsync(string topic, string subscriptionName, Func<ClusterByteMessage, CancellationToken, Task> onMessage, CancellationToken cancellationToken)
        {
            try
            {
                await foreach (var payload in _transport.SubscribeAsync(topic, subscriptionName, cancellationToken).ConfigureAwait(false))
                {
                    await HandleByteFanOutDeliveryAsync(payload, onMessage, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
                // Expected on shutdown/unsubscribe.
            }
        }

        /// <summary>Internal (rather than private) so tests can drive it directly with a raw payload.</summary>
        internal async Task HandleByteFanOutDeliveryAsync(string payload, Func<ClusterByteMessage, CancellationToken, Task> onMessage, CancellationToken cancellationToken)
        {
            ClusterByteMessage? message;
            try
            {
                message = payload.FromNJson<ClusterByteMessage>();
            }
            catch (Exception exception)
            {
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
            private readonly CancellationTokenSource _loopCts;
            private readonly Task _loopTask;

            internal ByteFanOutSubscription(
                ConcurrentDictionary<Guid, ByteFanOutSubscription> subscriptions,
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
            [LoggerMessage(EventId = 9860, Level = LogLevel.Warning,
                Message = "[Cluster] Pulsar byte fan-out publish to topic '{Topic}' failed.")]
            public static partial void ByteFanOutPublishFailed(ILogger logger, Exception exception, string topic);

            [LoggerMessage(EventId = 9861, Level = LogLevel.Warning,
                Message = "[Cluster] Pulsar byte fan-out message could not be parsed; skipping it.")]
            public static partial void ByteFanOutMessageUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 9862, Level = LogLevel.Error,
                Message = "[Cluster] Pulsar byte fan-out handler faulted.")]
            public static partial void ByteFanOutHandlerFaulted(ILogger logger, Exception exception);
        }
    }
}
