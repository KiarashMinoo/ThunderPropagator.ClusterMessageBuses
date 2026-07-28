using System.Collections.Concurrent;
using Apache.NMS;
using Apache.NMS.ActiveMQ;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using ThunderPropagator.Application.Channels.Cluster;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;

using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.ActiveMQ
{
    /// <summary>
    /// ActiveMQ-backed <see cref="IClusterMessageBus"/>: fan-out and subscription-event propagation
    /// each use one JMS topic per channel, with every node running its own non-durable
    /// <see cref="IMessageConsumer"/> against that topic (plain JMS topic pub/sub already delivers a
    /// copy of every message to every currently-active subscriber, so — like
    /// <c>NatsClusterMessageBus</c>/<c>MqttClusterMessageBus</c> and unlike the
    /// Kafka/RabbitMQ/Pulsar transports — no consumer-group/exclusive-queue/unique-subscription-name
    /// trick is needed; durable subscriptions, which would instead persist missed messages for an
    /// offline node, are deliberately not used here since a fan-out broadcast that a temporarily
    /// disconnected node misses is expected to be replayed via <see cref="SyncDeltaFromLeaderAsync"/>
    /// on reconnect, not redelivered from the broker).
    /// </summary>
    /// <remarks>
    /// <para>
    /// ActiveMQ/JMS has no native request/reply (unlike NATS), so the three leader/peer-pull
    /// operations (<see cref="RestoreFromLeaderAsync"/>, <see cref="SyncDeltaFromLeaderAsync"/>,
    /// <see cref="FetchPeerSubscriptionsAsync"/>) use one point-to-point JMS queue per node for
    /// requests and one for replies, combined with the same hand-rolled correlation-id scheme as
    /// <c>KafkaClusterMessageBus</c>/<c>PulsarClusterMessageBus</c>/<c>MqttClusterMessageBus</c>.
    /// </para>
    /// <para>
    /// <c>Apache.NMS</c>'s <c>IConnection</c>/<c>ISession</c>/<c>IMessageProducer</c>/
    /// <c>IMessageConsumer</c>/<c>ITopic</c>/<c>IQueue</c>/<c>ITextMessage</c> are all plain public
    /// interfaces (confirmed directly from the <c>Apache.NMS</c> API source), so — like
    /// <c>KafkaClusterMessageBus</c>'s <c>IProducer</c>/<c>IConsumer</c> and
    /// <c>RabbitMqClusterMessageBus</c>'s <c>IConnection</c>/<c>IChannel</c> — every test substitutes
    /// these real interfaces directly with NSubstitute rather than requiring a bespoke transport-seam
    /// interface the way the less-thoroughly-documented NATS.Net/DotPulsar/MQTTnet transports do.
    /// </para>
    /// <para>
    /// One shared session/producer pair (<see cref="_publishSession"/>/<see cref="_publishProducer"/>)
    /// is reused for every publish this transport performs, serialized behind
    /// <see cref="_publishLock"/> — a single JMS session is not safe for concurrent use by more than
    /// one thread. Each <see cref="SubscribeAsync(Guid, Func{ClusterFanOutMessage,CancellationToken,Task},CancellationToken)"/>
    /// call, and the request/reply listeners started during initialization, each get their own
    /// dedicated session instead.
    /// </para>
    /// </remarks>
    internal sealed partial class ActiveMqClusterMessageBus : AbstractClusterMessageBus, IAsyncDisposable
    {
        private readonly ActiveMqClusterMessageBusOptions _options;
        private readonly Uri _nodeEndpoint;
        private readonly IClusterChannelResolver _channelResolver;
        private readonly ILogger _logger;
        private readonly Guid _selfId = Guid.NewGuid();

        /// <summary>This node's process-lifetime self-echo identifier. Exposed for tests only.</summary>
        internal Guid SelfId => _selfId;

        private readonly Func<CancellationToken, Task<IConnection>> _connectionFactory;

        private readonly SemaphoreSlim _initLock = new(1, 1);
        private readonly SemaphoreSlim _publishLock = new(1, 1);
        private volatile bool _initialized;

        private IConnection? _connection;
        private ISession? _publishSession;
        private IMessageProducer? _publishProducer;
        private ISession? _requestListenerSession;
        private IMessageConsumer? _requestListenerConsumer;
        private ISession? _replyListenerSession;
        private IMessageConsumer? _replyListenerConsumer;

        private readonly ConcurrentDictionary<Guid, FanOutSubscription> _fanOutSubscriptions = new();
        private readonly ConcurrentDictionary<Guid, SubscriptionEventSubscription> _subscriptionEventSubscriptions = new();

        private readonly ConcurrentDictionary<Guid, TaskCompletionSource<ActiveMqClusterResponseEnvelope>> _pendingRequests = new();
        private readonly CancellationTokenSource _lifetimeCts = new();

        // Same constructor-injection-for-testability shape as KafkaClusterMessageBus/
        // RabbitMqClusterMessageBus: connectionFactory defaults to the real ConnectionFactory-based
        // builder, but tests substitute an NSubstitute-backed IConnection instead of requiring a live
        // broker.
        public ActiveMqClusterMessageBus(
            IOptions<ActiveMqClusterMessageBusOptions> options,
            ClusterConfiguration clusterConfiguration,
            IClusterChannelResolver channelResolver,
            ILoggerFactory loggerFactory,
            Func<CancellationToken, Task<IConnection>>? connectionFactory = null)
        {
            _options = options.Value;
            _nodeEndpoint = clusterConfiguration.NodeEndpoint
                ?? throw new InvalidOperationException(
                    $"{nameof(ClusterConfiguration)}.{nameof(ClusterConfiguration.NodeEndpoint)} must be set for " +
                    $"{nameof(ActiveMqClusterMessageBus)} to identify this node's request/reply queues.");
            _channelResolver = channelResolver;
            _logger = loggerFactory.CreateLogger<ActiveMqClusterMessageBus>();
            _connectionFactory = connectionFactory ?? DefaultConnectionFactoryAsync;

            Log.Constructed(_logger, _nodeEndpoint.Host);
        }

        private async Task<IConnection> DefaultConnectionFactoryAsync(CancellationToken cancellationToken)
        {
            var factory = new ConnectionFactory(new Uri(_options.BrokerUri));
            _options.ConfigureConnectionFactory?.Invoke(factory);

            // Apache.NMS's documented connect flow requires an explicit Start() call before a
            // connection will dispatch messages to consumers — CreateConnectionAsync alone leaves it
            // idle. cancellationToken is not threaded into either call since neither
            // IConnectionFactory.CreateConnectionAsync() nor IStartable.StartAsync() accept one; it is
            // still honored by every awaiter around this method.
            var connection = await factory.CreateConnectionAsync().ConfigureAwait(false);
            await connection.StartAsync().ConfigureAwait(false);

            return connection;
        }

        /// <summary>
        /// Lazily opens the connection and starts this node's request/reply listeners exactly once.
        /// Internal (rather than private) so tests can await it directly to force initialization
        /// against a substitute <see cref="IConnection"/> before exercising the bus.
        /// </summary>
        internal async Task EnsureInitializedAsync(CancellationToken cancellationToken)
        {
            if (_initialized)
                return;

            await _initLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                if (_initialized)
                    return;

                _connection = await _connectionFactory(cancellationToken).ConfigureAwait(false);

                _publishSession = await _connection.CreateSessionAsync().ConfigureAwait(false);
                _publishProducer = await _publishSession.CreateProducerAsync().ConfigureAwait(false);

                var requestQueueName = ActiveMqTopicNaming.RequestQueue(_options.TopicPrefix, _nodeEndpoint);
                _requestListenerSession = await _connection.CreateSessionAsync().ConfigureAwait(false);
                var requestQueue = await _requestListenerSession.GetQueueAsync(requestQueueName).ConfigureAwait(false);
                _requestListenerConsumer = await _requestListenerSession.CreateConsumerAsync(requestQueue).ConfigureAwait(false);
                _requestListenerConsumer.AsyncListener += (message, ct) => HandleRequestDeliveryAsync(ReadText(message), ct);

                var replyQueueName = ActiveMqTopicNaming.ReplyQueue(_options.TopicPrefix, _nodeEndpoint);
                _replyListenerSession = await _connection.CreateSessionAsync().ConfigureAwait(false);
                var replyQueue = await _replyListenerSession.GetQueueAsync(replyQueueName).ConfigureAwait(false);
                _replyListenerConsumer = await _replyListenerSession.CreateConsumerAsync(replyQueue).ConfigureAwait(false);
                _replyListenerConsumer.AsyncListener += (message, ct) => HandleReplyDeliveryAsync(ReadText(message), ct);

                _initialized = true;
                Log.Started(_logger, _nodeEndpoint.Host);
            }
            finally
            {
                _initLock.Release();
            }
        }

        /// <summary>
        /// Reads the string payload from a received <see cref="IMessage"/>, tolerating anything that
        /// is not an <see cref="ITextMessage"/> (this transport only ever sends text messages itself,
        /// but a foreign publisher on the same broker could put something else on these
        /// destinations) by treating it as an empty, deliberately-unparseable payload rather than
        /// throwing — the same malformed-payload-skip discipline used by every prior transport.
        /// </summary>
        private static string ReadText(IMessage message) => message is ITextMessage textMessage ? textMessage.Text ?? string.Empty : string.Empty;

        public async ValueTask DisposeAsync()
        {
            await _lifetimeCts.CancelAsync().ConfigureAwait(false);

            foreach (var pending in _pendingRequests.Values)
            {
                pending.TrySetCanceled();
            }

            // Callers are expected to dispose each SubscribeAsync handle themselves as channels
            // unsubscribe — but if the whole bus is torn down first (e.g. host shutdown) without
            // that happening, every still-open per-channel consumer must still be closed here
            // rather than leaked (same fix applied to every other broker transport in this repo
            // after a dedicated regression test found it there first for Kafka).
            foreach (var fanOutSubscription in _fanOutSubscriptions.Values.ToArray())
            {
                await fanOutSubscription.DisposeAsync().ConfigureAwait(false);
            }

            foreach (var subscriptionEventSubscription in _subscriptionEventSubscriptions.Values.ToArray())
            {
                await subscriptionEventSubscription.DisposeAsync().ConfigureAwait(false);
            }

            foreach (var session in new[] { _requestListenerSession, _replyListenerSession, _publishSession })
            {
                if (session is null)
                    continue;

                try
                {
                    await session.CloseAsync().ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    Log.SessionCloseFailed(_logger, exception);
                }

                session.Dispose();
            }

            if (_connection is not null)
            {
                try
                {
                    await _connection.CloseAsync().ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    Log.ConnectionCloseFailed(_logger, exception);
                }

                _connection.Dispose();
            }

            _initLock.Dispose();
            _publishLock.Dispose();
            _lifetimeCts.Dispose();
        }

        private static partial class Log
        {
            [LoggerMessage(EventId = 91000, Level = LogLevel.Debug,
                Message = "[Cluster] ActiveMQ message bus constructed for node '{Host}'; connection is opened lazily on first use.")]
            public static partial void Constructed(ILogger logger, string host);

            [LoggerMessage(EventId = 91001, Level = LogLevel.Information,
                Message = "[Cluster] ActiveMQ message bus started for node '{Host}'.")]
            public static partial void Started(ILogger logger, string host);

            [LoggerMessage(EventId = 91002, Level = LogLevel.Warning,
                Message = "[Cluster] ActiveMQ session close failed during dispose.")]
            public static partial void SessionCloseFailed(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91003, Level = LogLevel.Warning,
                Message = "[Cluster] ActiveMQ connection close failed during dispose.")]
            public static partial void ConnectionCloseFailed(ILogger logger, Exception exception);
        }
    }
}
