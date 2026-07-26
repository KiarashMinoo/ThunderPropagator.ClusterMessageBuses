using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.BuildingBlocks.Application.Helpers;

namespace ThunderPropagator.ClusterMessageBuses.NATS
{
    internal sealed partial class NatsClusterMessageBus
    {
        public override async Task PublishAsync(Guid channelKey, ClusterFanOutMessage message, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var stamped = message with { OriginId = _selfId };
            var subject = NatsSubjectNaming.FanOutSubject(_options.SubjectPrefix, channelKey);

            try
            {
                await _transport.PublishAsync(subject, stamped.ToNJson(), cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Log.FanOutPublishFailed(_logger, exception, subject);
            }
        }

        public override async Task<IAsyncDisposable> SubscribeAsync(Guid channelKey, Func<ClusterFanOutMessage, CancellationToken, Task> onMessage, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var subject = NatsSubjectNaming.FanOutSubject(_options.SubjectPrefix, channelKey);
            var loopCts = CancellationTokenSource.CreateLinkedTokenSource(_lifetimeCts.Token);
            var loopTask = Task.Run(() => RunFanOutSubscriptionLoopAsync(subject, onMessage, loopCts.Token), loopCts.Token);

            var subscription = new FanOutSubscription(_fanOutSubscriptions, channelKey, loopCts, loopTask);
            _fanOutSubscriptions[channelKey] = subscription;

            return subscription;
        }

        /// <summary>Internal (rather than private) so tests can drive it directly against a fake transport.</summary>
        internal async Task RunFanOutSubscriptionLoopAsync(string subject, Func<ClusterFanOutMessage, CancellationToken, Task> onMessage, CancellationToken cancellationToken)
        {
            try
            {
                await foreach (var delivery in _transport.SubscribeAsync(subject, cancellationToken).ConfigureAwait(false))
                {
                    await HandleFanOutDeliveryAsync(delivery.Payload, onMessage, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
                // Expected on shutdown/unsubscribe.
            }
        }

        /// <summary>Internal (rather than private) so tests can drive it directly with a raw payload.</summary>
        internal async Task HandleFanOutDeliveryAsync(string payload, Func<ClusterFanOutMessage, CancellationToken, Task> onMessage, CancellationToken cancellationToken)
        {
            ClusterFanOutMessage? message;
            try
            {
                message = payload.FromNJson<ClusterFanOutMessage>();
            }
            catch (Exception exception)
            {
                // A single malformed delivery must not take down an otherwise-healthy subscription
                // — log and move on (mirrors the fix applied to KafkaClusterMessageBus's consume
                // loops after a dedicated test proved the unguarded deserialize could crash them).
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
            private readonly CancellationTokenSource _loopCts;
            private readonly Task _loopTask;

            internal FanOutSubscription(
                ConcurrentDictionary<Guid, FanOutSubscription> subscriptions,
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
            [LoggerMessage(EventId = 9710, Level = LogLevel.Warning,
                Message = "[Cluster] NATS fan-out publish to subject '{Subject}' failed.")]
            public static partial void FanOutPublishFailed(ILogger logger, Exception exception, string subject);

            [LoggerMessage(EventId = 9711, Level = LogLevel.Warning,
                Message = "[Cluster] NATS fan-out message could not be parsed; skipping it.")]
            public static partial void FanOutMessageUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 9712, Level = LogLevel.Error,
                Message = "[Cluster] NATS fan-out handler faulted.")]
            public static partial void FanOutHandlerFaulted(ILogger logger, Exception exception);
        }
    }
}
