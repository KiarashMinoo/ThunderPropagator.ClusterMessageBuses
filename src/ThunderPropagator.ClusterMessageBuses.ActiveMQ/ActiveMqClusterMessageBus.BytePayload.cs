using System.Collections.Concurrent;
using Apache.NMS;
using Microsoft.Extensions.Logging;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.ActiveMQ
{
    /// <summary>
    /// Byte-oriented counterpart to <c>ActiveMqClusterMessageBus.FanOut.cs</c> /
    /// <c>.RequestReply.cs</c> -- see <see cref="ClusterByteMessage"/>'s own doc comment for why
    /// this surface exists alongside, not instead of, the <c>ClusterFanOutMessage</c>-based one.
    /// Fan-out uses its own per-channel topic (<see cref="ActiveMqTopicNaming.ByteFanOutTopic"/>),
    /// with the same push-based, non-durable <see cref="IMessageConsumer.AsyncListener"/> shape as
    /// <c>.FanOut.cs</c>; the snapshot pull reuses the existing hand-rolled request/reply plumbing
    /// in <c>ActiveMqClusterMessageBus.RequestReply.cs</c> via a new
    /// <see cref="ClusterRequestKind.PullSnapshotBytes"/> kind, exactly like
    /// <c>RestoreFromLeaderAsync</c> reuses it for <see cref="ClusterRequestKind.RestoreSnapshot"/>.
    /// </summary>
    internal sealed partial class ActiveMqClusterMessageBus
    {
        /// <inheritdoc />
        public override async Task PublishAsync(Guid channelKey, ClusterByteMessage message, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var stamped = message with { OriginId = _selfId };
            var topicName = ActiveMqTopicNaming.ByteFanOutTopic(_options.TopicPrefix, channelKey);

            await _publishLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                var topic = await _publishSession!.GetTopicAsync(topicName).ConfigureAwait(false);
                var textMessage = await _publishSession.CreateTextMessageAsync(stamped.ToNJson()).ConfigureAwait(false);
                await _publishProducer!.SendAsync(topic, textMessage).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Log.ByteFanOutPublishFailed(_logger, exception, topicName);
            }
            finally
            {
                _publishLock.Release();
            }
        }

        /// <inheritdoc />
        public override async Task<IAsyncDisposable> SubscribeAsync(Guid channelKey, Func<ClusterByteMessage, CancellationToken, Task> onMessage, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var topicName = ActiveMqTopicNaming.ByteFanOutTopic(_options.TopicPrefix, channelKey);
            var session = await _connection!.CreateSessionAsync().ConfigureAwait(false);
            var topic = await session.GetTopicAsync(topicName).ConfigureAwait(false);
            var consumer = await session.CreateConsumerAsync(topic).ConfigureAwait(false);

            consumer.AsyncListener += (message, ct) => HandleByteFanOutDeliveryAsync(ReadText(message), onMessage, ct);

            var subscription = new ByteFanOutSubscription(_byteFanOutSubscriptions, channelKey, session, consumer);
            _byteFanOutSubscriptions[channelKey] = subscription;

            return subscription;
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
                // A single malformed delivery must not take down an otherwise-healthy subscription
                // — log and move on (mirrors the same discipline in HandleFanOutDeliveryAsync).
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

        /// <inheritdoc />
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
            private readonly ISession _session;
            private readonly IMessageConsumer _consumer;

            internal ByteFanOutSubscription(
                ConcurrentDictionary<Guid, ByteFanOutSubscription> subscriptions,
                Guid channelKey,
                ISession session,
                IMessageConsumer consumer)
            {
                _subscriptions = subscriptions;
                _channelKey = channelKey;
                _session = session;
                _consumer = consumer;
            }

            public async ValueTask DisposeAsync()
            {
                _subscriptions.TryRemove(_channelKey, out _);

                try
                {
                    await _consumer.CloseAsync().ConfigureAwait(false);
                }
                catch (Exception)
                {
                    // Best-effort: the session is closed/disposed regardless below (it may already
                    // be broken if the connection dropped).
                }

                _consumer.Dispose();

                try
                {
                    await _session.CloseAsync().ConfigureAwait(false);
                }
                catch (Exception)
                {
                    // Same best-effort reasoning as above.
                }

                _session.Dispose();
            }
        }

        private static partial class Log
        {
            [LoggerMessage(EventId = 91060, Level = LogLevel.Warning,
                Message = "[Cluster] ActiveMQ byte fan-out publish to topic '{Topic}' failed.")]
            public static partial void ByteFanOutPublishFailed(ILogger logger, Exception exception, string topic);

            [LoggerMessage(EventId = 91061, Level = LogLevel.Warning,
                Message = "[Cluster] ActiveMQ byte fan-out message could not be parsed; skipping it.")]
            public static partial void ByteFanOutMessageUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91062, Level = LogLevel.Error,
                Message = "[Cluster] ActiveMQ byte fan-out handler faulted.")]
            public static partial void ByteFanOutHandlerFaulted(ILogger logger, Exception exception);
        }
    }
}
