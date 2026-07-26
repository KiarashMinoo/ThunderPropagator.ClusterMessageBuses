using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using ThunderPropagator.Application.Channels.Cluster;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;

using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.NATS
{
    /// <summary>
    /// NATS-backed <see cref="IClusterMessageBus"/>: fan-out and subscription-event propagation each
    /// use one subject per channel, with every node running its own independent subscription (Core
    /// NATS pub/sub delivers a copy of every message to every distinct subscriber, so no consumer-
    /// group-style configuration is needed the way Kafka/RabbitMQ require — this is the simplest of
    /// the broker-based transports' fan-out models). The three leader/peer-pull operations
    /// (<see cref="RestoreFromLeaderAsync"/>, <see cref="SyncDeltaFromLeaderAsync"/>,
    /// <see cref="FetchPeerSubscriptionsAsync"/>) use NATS's native request/reply: every node
    /// subscribes to its own request subject (named from its own
    /// <see cref="ClusterConfiguration.NodeEndpoint"/>) and answers by publishing to the reply-to
    /// inbox subject NATS itself attaches to each request — unlike Kafka/RabbitMQ, this transport
    /// never has to name, own, or correlate a reply topic/queue by hand.
    /// </summary>
    /// <remarks>
    /// All real <c>NATS.Net</c> client usage is isolated behind <see cref="INatsClusterTransport"/>
    /// (see <see cref="NatsClusterTransport"/> for the real implementation) so this class — and its
    /// tests — never depend on <c>NATS.Net</c>'s own concrete types.
    /// </remarks>
    internal sealed partial class NatsClusterMessageBus : AbstractClusterMessageBus, IAsyncDisposable
    {
        private readonly NatsClusterMessageBusOptions _options;
        private readonly Uri _nodeEndpoint;
        private readonly IClusterChannelResolver _channelResolver;
        private readonly ILogger _logger;
        private readonly Guid _selfId = Guid.NewGuid();

        /// <summary>This node's process-lifetime self-echo identifier. Exposed for tests only.</summary>
        internal Guid SelfId => _selfId;

        private readonly INatsClusterTransport _transport;

        private readonly SemaphoreSlim _initLock = new(1, 1);
        private volatile bool _initialized;
        private Task? _requestListenerTask;

        private readonly ConcurrentDictionary<Guid, FanOutSubscription> _fanOutSubscriptions = new();
        private readonly ConcurrentDictionary<Guid, SubscriptionEventSubscription> _subscriptionEventSubscriptions = new();

        private readonly CancellationTokenSource _lifetimeCts = new();

        // Same constructor-injection-for-testability shape as KafkaClusterMessageBus/
        // RabbitMqClusterMessageBus: transport defaults to the real NatsClusterTransport, but tests
        // substitute an NSubstitute-backed INatsClusterTransport instead of requiring a live server.
        public NatsClusterMessageBus(
            IOptions<NatsClusterMessageBusOptions> options,
            ClusterConfiguration clusterConfiguration,
            IClusterChannelResolver channelResolver,
            ILoggerFactory loggerFactory,
            INatsClusterTransport? transport = null)
        {
            _options = options.Value;
            _nodeEndpoint = clusterConfiguration.NodeEndpoint
                ?? throw new InvalidOperationException(
                    $"{nameof(ClusterConfiguration)}.{nameof(ClusterConfiguration.NodeEndpoint)} must be set for " +
                    $"{nameof(NatsClusterMessageBus)} to identify this node's request subject.");
            _channelResolver = channelResolver;
            _logger = loggerFactory.CreateLogger<NatsClusterMessageBus>();
            _transport = transport ?? new NatsClusterTransport(_options.Url);

            Log.Constructed(_logger, _nodeEndpoint.Host);
        }

        /// <summary>
        /// Lazily starts this node's request-listener loop exactly once. Internal (rather than
        /// private) so tests can await it directly before exercising the bus. Kept Task-returning
        /// (rather than fully synchronous) for consistency with the other transports' lazy-init seam
        /// even though, unlike RabbitMQ, nothing here actually needs to await real I/O to begin.
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

                _requestListenerTask = Task.Run(() => RunRequestListenerLoopAsync(_lifetimeCts.Token));

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

            if (_requestListenerTask is not null)
            {
                try
                {
                    await _requestListenerTask.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
                }
                catch (Exception exception) when (exception is TimeoutException or OperationCanceledException)
                {
                    Log.ListenerLoopDidNotStopInTime(_logger, exception);
                }
            }

            // Callers are expected to dispose each SubscribeAsync handle themselves as channels
            // unsubscribe — but if the whole bus is torn down first (e.g. host shutdown) without
            // that happening, every still-running per-channel subscription loop must still be
            // stopped here rather than leaked (same fix applied to KafkaClusterMessageBus /
            // RabbitMqClusterMessageBus after a dedicated regression test found it there first).
            foreach (var fanOutSubscription in _fanOutSubscriptions.Values.ToArray())
            {
                await fanOutSubscription.DisposeAsync().ConfigureAwait(false);
            }

            foreach (var subscriptionEventSubscription in _subscriptionEventSubscriptions.Values.ToArray())
            {
                await subscriptionEventSubscription.DisposeAsync().ConfigureAwait(false);
            }

            await _transport.DisposeAsync().ConfigureAwait(false);

            _initLock.Dispose();
            _lifetimeCts.Dispose();
        }

        private static partial class Log
        {
            [LoggerMessage(EventId = 9700, Level = LogLevel.Debug,
                Message = "[Cluster] NATS message bus constructed for node '{Host}'; the request listener is started lazily on first use.")]
            public static partial void Constructed(ILogger logger, string host);

            [LoggerMessage(EventId = 9701, Level = LogLevel.Information,
                Message = "[Cluster] NATS message bus started for node '{Host}'.")]
            public static partial void Started(ILogger logger, string host);

            [LoggerMessage(EventId = 9702, Level = LogLevel.Warning,
                Message = "[Cluster] NATS listener loop did not stop within the shutdown grace period.")]
            public static partial void ListenerLoopDidNotStopInTime(ILogger logger, Exception exception);
        }
    }
}
