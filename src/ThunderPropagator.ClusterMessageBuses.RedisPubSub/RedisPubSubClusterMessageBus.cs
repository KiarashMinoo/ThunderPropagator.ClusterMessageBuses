using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using ThunderPropagator.Application.Channels.Cluster;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;

using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.RedisPubSub
{
    /// <summary>
    /// Redis-pub/sub-backed <see cref="IClusterMessageBus"/>: fan-out and subscription-event
    /// propagation each use one Redis channel per ThunderPropagator channel, with every node
    /// subscribing independently (plain Redis pub/sub already delivers a copy of every published
    /// message to every subscribing client, so — like <c>NatsClusterMessageBus</c>/
    /// <c>MqttClusterMessageBus</c>/<c>ActiveMqClusterMessageBus</c>'s topics and unlike the
    /// Kafka/RabbitMQ/Pulsar transports — no consumer-group/exclusive-queue/unique-subscription-name
    /// trick is needed). Redis pub/sub has no native request/reply (unlike NATS), so the three
    /// leader/peer-pull operations (<see cref="RestoreFromLeaderAsync"/>,
    /// <see cref="SyncDeltaFromLeaderAsync"/>, <see cref="FetchPeerSubscriptionsAsync"/>) use the same
    /// hand-rolled correlation-id/reply-channel scheme as <c>KafkaClusterMessageBus</c>/
    /// <c>PulsarClusterMessageBus</c>/<c>MqttClusterMessageBus</c>/<c>ActiveMqClusterMessageBus</c>:
    /// every node subscribes to its own request channel and its own reply channel (both named from
    /// its own <see cref="ClusterConfiguration.NodeEndpoint"/>) and answers by resolving the
    /// requested channel locally, replying on the requester's own reply channel.
    /// </summary>
    /// <remarks>
    /// <c>StackExchange.Redis</c>'s <c>IConnectionMultiplexer</c> and <c>ISubscriber</c> are both
    /// plain public interfaces (confirmed directly from the <c>StackExchange.Redis</c> API source),
    /// so — like <c>KafkaClusterMessageBus</c>'s <c>IProducer</c>/<c>IConsumer</c>,
    /// <c>RabbitMqClusterMessageBus</c>'s <c>IConnection</c>/<c>IChannel</c>, and
    /// <c>ActiveMqClusterMessageBus</c>'s <c>IConnection</c>/<c>ISession</c> — every test substitutes
    /// these real interfaces directly with NSubstitute. This transport deliberately uses
    /// <c>ISubscriber</c>'s delegate-based <c>SubscribeAsync(RedisChannel, Action&lt;RedisChannel,RedisValue&gt;)</c>
    /// overload rather than the <c>ChannelMessageQueue</c>-returning overload, since
    /// <c>ChannelMessageQueue</c> is a sealed concrete class (not an interface) and so cannot be
    /// substituted directly; the handler delegate is necessarily synchronous, so delivery handling is
    /// dispatched fire-and-forget (<c>_ = HandleXAsync(...)</c>) from inside it, the same trade-off
    /// documented on <see cref="FanOutSubscription"/>/<see cref="SubscriptionEventSubscription"/>.
    /// </remarks>
    internal sealed partial class RedisPubSubClusterMessageBus : AbstractClusterMessageBus, IAsyncDisposable
    {
        private readonly RedisPubSubClusterMessageBusOptions _options;
        private readonly Uri _nodeEndpoint;
        private readonly IClusterChannelResolver _channelResolver;
        private readonly ILogger _logger;
        private readonly Guid _selfId = Guid.NewGuid();

        /// <summary>This node's process-lifetime self-echo identifier. Exposed for tests only.</summary>
        internal Guid SelfId => _selfId;

        private readonly Func<CancellationToken, Task<IConnectionMultiplexer>> _connectionFactory;

        private readonly SemaphoreSlim _initLock = new(1, 1);
        private volatile bool _initialized;

        private IConnectionMultiplexer? _connection;
        private ISubscriber? _subscriber;
        private Action<RedisChannel, RedisValue>? _requestHandler;
        private Action<RedisChannel, RedisValue>? _replyHandler;

        private readonly ConcurrentDictionary<Guid, FanOutSubscription> _fanOutSubscriptions = new();
        private readonly ConcurrentDictionary<Guid, SubscriptionEventSubscription> _subscriptionEventSubscriptions = new();

        private readonly ConcurrentDictionary<Guid, TaskCompletionSource<RedisPubSubClusterResponseEnvelope>> _pendingRequests = new();
        private readonly CancellationTokenSource _lifetimeCts = new();

        // Same constructor-injection-for-testability shape as every other broker transport in this
        // repo: connectionFactory defaults to the real ConnectionMultiplexer-based builder, but tests
        // substitute an NSubstitute-backed IConnectionMultiplexer instead of requiring a live server.
        public RedisPubSubClusterMessageBus(
            IOptions<RedisPubSubClusterMessageBusOptions> options,
            ClusterConfiguration clusterConfiguration,
            IClusterChannelResolver channelResolver,
            ILoggerFactory loggerFactory,
            Func<CancellationToken, Task<IConnectionMultiplexer>>? connectionFactory = null)
        {
            _options = options.Value;
            _nodeEndpoint = clusterConfiguration.NodeEndpoint
                ?? throw new InvalidOperationException(
                    $"{nameof(ClusterConfiguration)}.{nameof(ClusterConfiguration.NodeEndpoint)} must be set for " +
                    $"{nameof(RedisPubSubClusterMessageBus)} to identify this node's request/reply channels.");
            _channelResolver = channelResolver;
            _logger = loggerFactory.CreateLogger<RedisPubSubClusterMessageBus>();
            _connectionFactory = connectionFactory ?? DefaultConnectionFactoryAsync;

            Log.Constructed(_logger, _nodeEndpoint.Host);
        }

        private async Task<IConnectionMultiplexer> DefaultConnectionFactoryAsync(CancellationToken cancellationToken)
        {
            var configurationOptions = ConfigurationOptions.Parse(_options.ConnectionString);
            _options.ConfigureOptions?.Invoke(configurationOptions);

            // ConnectionMultiplexer.ConnectAsync does not accept a CancellationToken; cancellation of
            // the connection attempt itself is not honored here, but cancellationToken is still
            // honored by every awaiter around this method.
            return await ConnectionMultiplexer.ConnectAsync(configurationOptions).ConfigureAwait(false);
        }

        /// <summary>
        /// Lazily opens the connection and subscribes this node's request and reply channels exactly
        /// once. Internal (rather than private) so tests can await it directly to force
        /// initialization against a substitute <see cref="IConnectionMultiplexer"/> before exercising
        /// the bus.
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
                _subscriber = _connection.GetSubscriber();

                var requestChannel = RedisChannelNaming.RequestChannel(_options.ChannelPrefix, _nodeEndpoint);
                _requestHandler = (_, value) => _ = HandleRequestDeliveryAsync(value.ToString() ?? string.Empty, _lifetimeCts.Token);
                await _subscriber.SubscribeAsync(RedisChannel.Literal(requestChannel), _requestHandler).ConfigureAwait(false);

                var replyChannel = RedisChannelNaming.ReplyChannel(_options.ChannelPrefix, _nodeEndpoint);
                _replyHandler = (_, value) => _ = HandleReplyDeliveryAsync(value.ToString() ?? string.Empty, _lifetimeCts.Token);
                await _subscriber.SubscribeAsync(RedisChannel.Literal(replyChannel), _replyHandler).ConfigureAwait(false);

                _initialized = true;
                Log.Started(_logger, _nodeEndpoint.Host);
            }
            finally
            {
                _initLock.Release();
            }
        }

        public async ValueTask DisposeAsync()
        {
            await _lifetimeCts.CancelAsync().ConfigureAwait(false);

            foreach (var pending in _pendingRequests.Values)
            {
                pending.TrySetCanceled();
            }

            // Callers are expected to dispose each SubscribeAsync handle themselves as channels
            // unsubscribe — but if the whole bus is torn down first (e.g. host shutdown) without
            // that happening, every still-open per-channel subscription must still be stopped here
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

            if (_subscriber is not null)
            {
                if (_requestHandler is not null)
                {
                    var requestChannel = RedisChannelNaming.RequestChannel(_options.ChannelPrefix, _nodeEndpoint);
                    try
                    {
                        await _subscriber.UnsubscribeAsync(RedisChannel.Literal(requestChannel), _requestHandler).ConfigureAwait(false);
                    }
                    catch (Exception exception)
                    {
                        Log.UnsubscribeFailed(_logger, exception, requestChannel);
                    }
                }

                if (_replyHandler is not null)
                {
                    var replyChannel = RedisChannelNaming.ReplyChannel(_options.ChannelPrefix, _nodeEndpoint);
                    try
                    {
                        await _subscriber.UnsubscribeAsync(RedisChannel.Literal(replyChannel), _replyHandler).ConfigureAwait(false);
                    }
                    catch (Exception exception)
                    {
                        Log.UnsubscribeFailed(_logger, exception, replyChannel);
                    }
                }
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

                await _connection.DisposeAsync().ConfigureAwait(false);
            }

            _initLock.Dispose();
            _lifetimeCts.Dispose();
        }

        private static partial class Log
        {
            [LoggerMessage(EventId = 91100, Level = LogLevel.Debug,
                Message = "[Cluster] Redis pub/sub message bus constructed for node '{Host}'; connection is opened lazily on first use.")]
            public static partial void Constructed(ILogger logger, string host);

            [LoggerMessage(EventId = 91101, Level = LogLevel.Information,
                Message = "[Cluster] Redis pub/sub message bus started for node '{Host}'.")]
            public static partial void Started(ILogger logger, string host);

            [LoggerMessage(EventId = 91102, Level = LogLevel.Warning,
                Message = "[Cluster] Redis pub/sub unsubscribe from channel '{Channel}' failed during dispose.")]
            public static partial void UnsubscribeFailed(ILogger logger, Exception exception, string channel);

            [LoggerMessage(EventId = 91103, Level = LogLevel.Warning,
                Message = "[Cluster] Redis pub/sub connection close failed during dispose.")]
            public static partial void ConnectionCloseFailed(ILogger logger, Exception exception);
        }
    }
}
