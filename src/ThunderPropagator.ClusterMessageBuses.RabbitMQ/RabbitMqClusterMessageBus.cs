using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using ThunderPropagator.Application.Channels.Cluster;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;

using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.RabbitMQ
{
    /// <summary>
    /// RabbitMQ-backed <see cref="IClusterMessageBus"/>: fan-out and subscription-event propagation
    /// each use one "fanout"-type exchange per channel, with every node binding its own exclusive,
    /// auto-delete queue to that exchange (so every node gets its own full copy of the broadcast,
    /// mirroring <c>ThunderPropagator.ClusterMessageBuses.Kafka</c>'s one-consumer-group-per-node
    /// design). The three leader/peer-pull operations (<see cref="RestoreFromLeaderAsync"/>,
    /// <see cref="SyncDeltaFromLeaderAsync"/>, <see cref="FetchPeerSubscriptionsAsync"/>) use a
    /// broker-native request/reply scheme: every node declares its own exclusive request queue and
    /// reply queue (named from its own <see cref="ClusterConfiguration.NodeEndpoint"/>) and publishes
    /// to a peer's request/reply queue via RabbitMQ's default exchange (routing key = queue name).
    /// </summary>
    /// <remarks>
    /// <para>
    /// Unlike Confluent.Kafka's synchronous builder pattern, every RabbitMQ.Client v7 connection/
    /// channel/topology operation is asynchronous, so none of it can happen inside this class's
    /// constructor. Instead, every public method (and <see cref="SendRequestAsync"/>) begins by
    /// awaiting <see cref="EnsureInitializedAsync"/>, which lazily opens the connection, declares
    /// this node's request/reply queues, and starts their consumers exactly once (guarded by
    /// <see cref="_initLock"/>, double-checked against <see cref="_initialized"/>).
    /// </para>
    /// <para>
    /// One shared channel (<see cref="_publishChannel"/>) is reused for every publish this transport
    /// performs, serialized behind <see cref="_publishLock"/> — the RabbitMQ .NET client explicitly
    /// requires that a single <c>IChannel</c> never be used concurrently by more than one thread for
    /// publishing. Each <see cref="SubscribeAsync(Guid, Func{ThunderPropagator.Application.Channels.Cluster.ClusterFanOutMessage,CancellationToken,Task},CancellationToken)"/>
    /// call, and the two listener loops started during initialization, each get their own dedicated
    /// channel instead, so one subscriber's consumer dispatch can never block another's.
    /// </para>
    /// <para>
    /// <see cref="ClusterConfiguration.NodeEndpoint"/> must be set for this transport to work, even
    /// though nothing here is actually reached over HTTP — it's reused purely as this node's stable
    /// identity for queue naming (<see cref="RabbitMqTopicNaming.Slugify"/>), exactly as
    /// <c>HttpClusterMessageBus</c> reuses it as a literal base URI and
    /// <c>KafkaClusterMessageBus</c> reuses it for topic naming.
    /// </para>
    /// </remarks>
    internal sealed partial class RabbitMqClusterMessageBus : AbstractClusterMessageBus, IAsyncDisposable
    {
        private readonly RabbitMqClusterMessageBusOptions _options;
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
        private IChannel? _publishChannel;
        private IChannel? _requestListenerChannel;
        private IChannel? _replyListenerChannel;

        private readonly ConcurrentDictionary<Guid, FanOutSubscription> _fanOutSubscriptions = new();
        private readonly ConcurrentDictionary<Guid, SubscriptionEventSubscription> _subscriptionEventSubscriptions = new();

        private readonly ConcurrentDictionary<Guid, TaskCompletionSource<RabbitMqClusterResponseEnvelope>> _pendingRequests = new();
        private readonly CancellationTokenSource _lifetimeCts = new();

        // Same constructor-injection-for-testability shape as KafkaClusterMessageBus/
        // HttpClusterMessageBus: connectionFactory defaults to the real ConnectionFactory-based
        // builder, but tests substitute an NSubstitute-backed IConnection/IChannel instead of
        // requiring a live broker.
        public RabbitMqClusterMessageBus(
            IOptions<RabbitMqClusterMessageBusOptions> options,
            ClusterConfiguration clusterConfiguration,
            IClusterChannelResolver channelResolver,
            ILoggerFactory loggerFactory,
            Func<CancellationToken, Task<IConnection>>? connectionFactory = null)
        {
            _options = options.Value;
            _nodeEndpoint = clusterConfiguration.NodeEndpoint
                ?? throw new InvalidOperationException(
                    $"{nameof(ClusterConfiguration)}.{nameof(ClusterConfiguration.NodeEndpoint)} must be set for " +
                    $"{nameof(RabbitMqClusterMessageBus)} to identify this node's request/reply queues.");
            _channelResolver = channelResolver;
            _logger = loggerFactory.CreateLogger<RabbitMqClusterMessageBus>();
            _connectionFactory = connectionFactory ?? DefaultConnectionFactoryAsync;

            Log.Constructed(_logger, _nodeEndpoint.Host);
        }

        private async Task<IConnection> DefaultConnectionFactoryAsync(CancellationToken cancellationToken)
        {
            var factory = new ConnectionFactory { Uri = new Uri(_options.ConnectionString) };
            _options.ConfigureConnectionFactory?.Invoke(factory);

            // The RabbitMQ.Client v7 API guide's documented CreateConnectionAsync overloads do not
            // include a bare CancellationToken parameter alongside a Uri-configured factory with no
            // endpoint list, so cancellation of the connection attempt itself is not honored here;
            // cancellationToken is still honored by every awaiter around this call.
            return await factory.CreateConnectionAsync().ConfigureAwait(false);
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
                _publishChannel = await _connection.CreateChannelAsync().ConfigureAwait(false);

                var requestQueue = RabbitMqTopicNaming.RequestQueue(_options.ExchangePrefix, _nodeEndpoint);
                _requestListenerChannel = await _connection.CreateChannelAsync().ConfigureAwait(false);
                await _requestListenerChannel.QueueDeclareAsync(requestQueue, false, true, true, null).ConfigureAwait(false);
                var requestConsumer = new AsyncEventingBasicConsumer(_requestListenerChannel);
                requestConsumer.ReceivedAsync += async (_, delivery) =>
                    await HandleRequestDeliveryAsync(delivery.Body.ToArray(), _lifetimeCts.Token).ConfigureAwait(false);
                await _requestListenerChannel.BasicConsumeAsync(requestQueue, true, requestConsumer).ConfigureAwait(false);

                var replyQueue = RabbitMqTopicNaming.ReplyQueue(_options.ExchangePrefix, _nodeEndpoint);
                _replyListenerChannel = await _connection.CreateChannelAsync().ConfigureAwait(false);
                await _replyListenerChannel.QueueDeclareAsync(replyQueue, false, true, true, null).ConfigureAwait(false);
                var replyConsumer = new AsyncEventingBasicConsumer(_replyListenerChannel);
                replyConsumer.ReceivedAsync += async (_, delivery) =>
                    await HandleReplyDeliveryAsync(delivery.Body.ToArray(), _lifetimeCts.Token).ConfigureAwait(false);
                await _replyListenerChannel.BasicConsumeAsync(replyQueue, true, replyConsumer).ConfigureAwait(false);

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
            // that happening, every still-open per-channel consumer must still be closed here
            // rather than leaked (same fix applied to KafkaClusterMessageBus after it was found
            // there via a dedicated regression test).
            foreach (var fanOutSubscription in _fanOutSubscriptions.Values.ToArray())
            {
                await fanOutSubscription.DisposeAsync().ConfigureAwait(false);
            }

            foreach (var subscriptionEventSubscription in _subscriptionEventSubscriptions.Values.ToArray())
            {
                await subscriptionEventSubscription.DisposeAsync().ConfigureAwait(false);
            }

            foreach (var channel in new[] { _requestListenerChannel, _replyListenerChannel, _publishChannel })
            {
                if (channel is null)
                    continue;

                try
                {
                    await channel.CloseAsync().ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    Log.ChannelCloseFailed(_logger, exception);
                }

                await channel.DisposeAsync().ConfigureAwait(false);
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
            _publishLock.Dispose();
            _lifetimeCts.Dispose();
        }

        private static partial class Log
        {
            [LoggerMessage(EventId = 9600, Level = LogLevel.Debug,
                Message = "[Cluster] RabbitMQ message bus constructed for node '{Host}'; connection is opened lazily on first use.")]
            public static partial void Constructed(ILogger logger, string host);

            [LoggerMessage(EventId = 9601, Level = LogLevel.Information,
                Message = "[Cluster] RabbitMQ message bus started for node '{Host}'.")]
            public static partial void Started(ILogger logger, string host);

            [LoggerMessage(EventId = 9602, Level = LogLevel.Warning,
                Message = "[Cluster] RabbitMQ channel close failed during dispose.")]
            public static partial void ChannelCloseFailed(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 9603, Level = LogLevel.Warning,
                Message = "[Cluster] RabbitMQ connection close failed during dispose.")]
            public static partial void ConnectionCloseFailed(ILogger logger, Exception exception);
        }
    }
}
