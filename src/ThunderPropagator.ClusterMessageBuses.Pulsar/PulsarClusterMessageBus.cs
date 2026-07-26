using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using ThunderPropagator.Application.Channels.Cluster;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;

using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.Pulsar
{
    /// <summary>
    /// Pulsar-backed <see cref="IClusterMessageBus"/>: fan-out and subscription-event propagation
    /// each use one topic per channel, with every node consuming through its own unique-per-process
    /// subscription name (Pulsar delivers a full, independent copy of a topic's messages to each
    /// distinct subscription name, mirroring <c>KafkaClusterMessageBus</c>'s one-consumer-group-per-
    /// node design — a shared subscription name would instead split messages across nodes, which is
    /// wrong for a fan-out bus). Pulsar has no native request/reply (unlike NATS), so the three
    /// leader/peer-pull operations (<see cref="RestoreFromLeaderAsync"/>,
    /// <see cref="SyncDeltaFromLeaderAsync"/>, <see cref="FetchPeerSubscriptionsAsync"/>) use the same
    /// hand-rolled correlation-id/reply-topic scheme as <c>KafkaClusterMessageBus"/>: every node
    /// listens on its own request topic (named from its own
    /// <see cref="ClusterConfiguration.NodeEndpoint"/>) and answers by resolving the requested
    /// channel locally, replying on the requester's own reply topic.
    /// </summary>
    /// <remarks>
    /// All real <c>DotPulsar</c> client usage is isolated behind <see cref="IPulsarClusterTransport"/>
    /// (see <see cref="PulsarClusterTransport"/> for the real implementation) so this class — and its
    /// tests — never depend on <c>DotPulsar</c>'s own concrete types.
    /// </remarks>
    internal sealed partial class PulsarClusterMessageBus : AbstractClusterMessageBus, IAsyncDisposable
    {
        private readonly PulsarClusterMessageBusOptions _options;
        private readonly Uri _nodeEndpoint;
        private readonly IClusterChannelResolver _channelResolver;
        private readonly ILogger _logger;
        private readonly Guid _selfId = Guid.NewGuid();

        /// <summary>This node's process-lifetime self-echo identifier. Exposed for tests only.</summary>
        internal Guid SelfId => _selfId;

        private readonly IPulsarClusterTransport _transport;

        /// <summary>
        /// Unique-per-process suffix appended to every subscription name this bus creates, so this
        /// node always gets its own independent copy of every topic it consumes rather than
        /// competing with other nodes (or other runs of this same node) for messages.
        /// </summary>
        private readonly string _subscriptionSuffix = Guid.NewGuid().ToString("N");

        private readonly SemaphoreSlim _initLock = new(1, 1);
        private volatile bool _initialized;
        private Task? _requestListenerTask;
        private Task? _replyListenerTask;

        private readonly ConcurrentDictionary<Guid, FanOutSubscription> _fanOutSubscriptions = new();
        private readonly ConcurrentDictionary<Guid, SubscriptionEventSubscription> _subscriptionEventSubscriptions = new();
        private readonly ConcurrentDictionary<Guid, TaskCompletionSource<PulsarClusterResponseEnvelope>> _pendingRequests = new();

        private readonly CancellationTokenSource _lifetimeCts = new();

        // Same constructor-injection-for-testability shape as the other broker transports:
        // transport defaults to the real PulsarClusterTransport, but tests substitute an
        // NSubstitute-backed IPulsarClusterTransport instead of requiring a live Pulsar broker.
        public PulsarClusterMessageBus(
            IOptions<PulsarClusterMessageBusOptions> options,
            ClusterConfiguration clusterConfiguration,
            IClusterChannelResolver channelResolver,
            ILoggerFactory loggerFactory,
            IPulsarClusterTransport? transport = null)
        {
            _options = options.Value;
            _nodeEndpoint = clusterConfiguration.NodeEndpoint
                ?? throw new InvalidOperationException(
                    $"{nameof(ClusterConfiguration)}.{nameof(ClusterConfiguration.NodeEndpoint)} must be set for " +
                    $"{nameof(PulsarClusterMessageBus)} to identify this node's request/reply topics.");
            _channelResolver = channelResolver;
            _logger = loggerFactory.CreateLogger<PulsarClusterMessageBus>();
            _transport = transport ?? new PulsarClusterTransport(_options.ServiceUrl);

            Log.Constructed(_logger, _nodeEndpoint.Host);
        }

        /// <summary>
        /// Lazily starts this node's request and reply listener loops exactly once. Internal (rather
        /// than private) so tests can await it directly before exercising the bus.
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

                var requestTopic = PulsarTopicNaming.RequestTopic(_options.TopicPrefix, _nodeEndpoint);
                var requestSubscription = $"{_options.SubscriptionPrefix}-requests-{_subscriptionSuffix}";
                _requestListenerTask = Task.Run(() => RunRequestListenerLoopAsync(requestTopic, requestSubscription, _lifetimeCts.Token));

                var replyTopic = PulsarTopicNaming.ReplyTopic(_options.TopicPrefix, _nodeEndpoint);
                var replySubscription = $"{_options.SubscriptionPrefix}-replies-{_subscriptionSuffix}";
                _replyListenerTask = Task.Run(() => RunReplyListenerLoopAsync(replyTopic, replySubscription, _lifetimeCts.Token));

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

            foreach (var loop in new[] { _requestListenerTask, _replyListenerTask })
            {
                if (loop is null)
                    continue;

                try
                {
                    await loop.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
                }
                catch (Exception exception) when (exception is TimeoutException or OperationCanceledException)
                {
                    Log.ListenerLoopDidNotStopInTime(_logger, exception);
                }
            }

            foreach (var pending in _pendingRequests.Values)
            {
                pending.TrySetCanceled();
            }

            // Callers are expected to dispose each SubscribeAsync handle themselves as channels
            // unsubscribe — but if the whole bus is torn down first (e.g. host shutdown) without
            // that happening, every still-running per-channel subscription loop must still be
            // stopped here rather than leaked (same fix applied to every other broker transport in
            // this repo after a dedicated regression test found it first for Kafka).
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
            [LoggerMessage(EventId = 9800, Level = LogLevel.Debug,
                Message = "[Cluster] Pulsar message bus constructed for node '{Host}'; the request/reply listeners are started lazily on first use.")]
            public static partial void Constructed(ILogger logger, string host);

            [LoggerMessage(EventId = 9801, Level = LogLevel.Information,
                Message = "[Cluster] Pulsar message bus started for node '{Host}'.")]
            public static partial void Started(ILogger logger, string host);

            [LoggerMessage(EventId = 9802, Level = LogLevel.Warning,
                Message = "[Cluster] Pulsar listener loop did not stop within the shutdown grace period.")]
            public static partial void ListenerLoopDidNotStopInTime(ILogger logger, Exception exception);
        }
    }
}
