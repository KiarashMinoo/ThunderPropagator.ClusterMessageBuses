using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.NATS
{
    /// <summary>
    /// Byte-oriented counterpart to <c>NatsClusterMessageBus.FanOut.cs</c> /
    /// <c>.RequestReply.cs</c> -- see <see cref="ClusterByteMessage"/>'s own doc comment for why
    /// this surface exists alongside, not instead of, the <c>ClusterFanOutMessage</c>-based one.
    /// Fan-out uses its own per-channel subject (<see cref="NatsSubjectNaming.ByteFanOutSubject"/>);
    /// the snapshot pull reuses the existing native-request/reply plumbing in
    /// <c>NatsClusterMessageBus.RequestReply.cs</c> via a new
    /// <see cref="ClusterRequestKind.PullSnapshotBytes"/> kind, exactly like
    /// <c>RestoreFromLeaderAsync</c> reuses it for <see cref="ClusterRequestKind.RestoreSnapshot"/>.
    /// </summary>
    internal sealed partial class NatsClusterMessageBus
    {
        public override async Task PublishAsync(Guid channelKey, ClusterByteMessage message, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var stamped = message with { OriginId = _selfId };
            var subject = NatsSubjectNaming.ByteFanOutSubject(_options.SubjectPrefix, channelKey);

            try
            {
                await _transport.PublishAsync(subject, stamped.ToNJson(), cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Log.ByteFanOutPublishFailed(_logger, exception, subject);
            }
        }

        public override async Task<IAsyncDisposable> SubscribeAsync(Guid channelKey, Func<ClusterByteMessage, CancellationToken, Task> onMessage, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var subject = NatsSubjectNaming.ByteFanOutSubject(_options.SubjectPrefix, channelKey);
            var loopCts = CancellationTokenSource.CreateLinkedTokenSource(_lifetimeCts.Token);
            var loopTask = Task.Run(() => RunByteFanOutSubscriptionLoopAsync(subject, onMessage, loopCts.Token), loopCts.Token);

            var subscription = new ByteFanOutSubscription(_byteFanOutSubscriptions, channelKey, loopCts, loopTask);
            _byteFanOutSubscriptions[channelKey] = subscription;

            return subscription;
        }

        /// <summary>Internal (rather than private) so tests can drive it directly against a fake transport.</summary>
        internal async Task RunByteFanOutSubscriptionLoopAsync(string subject, Func<ClusterByteMessage, CancellationToken, Task> onMessage, CancellationToken cancellationToken)
        {
            try
            {
                await foreach (var delivery in _transport.SubscribeAsync(subject, cancellationToken).ConfigureAwait(false))
                {
                    await HandleByteFanOutDeliveryAsync(delivery.Payload, onMessage, cancellationToken).ConfigureAwait(false);
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
        /// has nothing to offer yet -- mirrored by <see cref="PullSnapshotAsync"/> deserializing that
        /// back to a null byte[], exactly like <c>BuildRestoreSnapshotResponseAsync</c> answers with
        /// an empty array rather than a failure when a channel has nothing to restore.
        /// </summary>
        private async Task<NatsClusterResponseEnvelope> BuildPullSnapshotBytesResponseAsync(NatsClusterRequestEnvelope request, CancellationToken cancellationToken)
        {
            if (_byteSnapshotProvider is null)
                return new NatsClusterResponseEnvelope(true, null, "null");

            var snapshot = await _byteSnapshotProvider.GetSnapshotAsync(request.ChannelKey!.Value, cancellationToken).ConfigureAwait(false);
            return new NatsClusterResponseEnvelope(true, null, snapshot.ToNJson());
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
            [LoggerMessage(EventId = 9760, Level = LogLevel.Warning,
                Message = "[Cluster] NATS byte fan-out publish to subject '{Subject}' failed.")]
            public static partial void ByteFanOutPublishFailed(ILogger logger, Exception exception, string subject);

            [LoggerMessage(EventId = 9761, Level = LogLevel.Warning,
                Message = "[Cluster] NATS byte fan-out message could not be parsed; skipping it.")]
            public static partial void ByteFanOutMessageUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 9762, Level = LogLevel.Error,
                Message = "[Cluster] NATS byte fan-out handler faulted.")]
            public static partial void ByteFanOutHandlerFaulted(ILogger logger, Exception exception);
        }
    }
}
