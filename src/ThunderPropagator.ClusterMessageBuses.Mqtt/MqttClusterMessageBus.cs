using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using ThunderPropagator.Application.Channels.Cluster;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;

using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.Mqtt
{
    /// <summary>
    /// MQTT-backed <see cref="IClusterMessageBus"/>: fan-out and subscription-event propagation each
    /// use one topic per channel, with every node running its own independent MQTT client connection
    /// subscribed to that topic (plain MQTT pub/sub delivers a copy of every published message to
    /// every distinct subscribing client, so — like <c>NatsClusterMessageBus</c> and unlike
    /// <c>KafkaClusterMessageBus</c>/<c>RabbitMqClusterMessageBus</c>/<c>PulsarClusterMessageBus</c> —
    /// no consumer-group/exclusive-queue/unique-subscription-name trick is needed to avoid competing
    /// for messages). MQTT has no native request/reply (unlike NATS), so the three leader/peer-pull
    /// operations (<see cref="RestoreFromLeaderAsync"/>, <see cref="SyncDeltaFromLeaderAsync"/>,
    /// <see cref="FetchPeerSubscriptionsAsync"/>) use the same hand-rolled correlation-id/reply-topic
    /// scheme as <c>KafkaClusterMessageBus</c>/<c>PulsarClusterMessageBus</c>: every node subscribes to
    /// its own request topic and its own reply topic (both named from its own
    /// <see cref="ClusterConfiguration.NodeEndpoint"/>) and answers by resolving the requested channel
    /// locally, replying on the requester's own reply topic.
    /// </summary>
    /// <remarks>
    /// All real <c>MQTTnet</c> client usage is isolated behind <see cref="IMqttClusterTransport"/>
    /// (see <see cref="MqttClusterTransport"/> for the real implementation) so this class — and its
    /// tests — never depend on <c>MQTTnet</c>'s own concrete/event-based types.
    /// </remarks>
    internal sealed partial class MqttClusterMessageBus : AbstractClusterMessageBus, IAsyncDisposable
    {
        private readonly MqttClusterMessageBusOptions _options;
        private readonly Uri _nodeEndpoint;
        private readonly IClusterChannelResolver _channelResolver;
        private readonly ILogger _logger;
        private readonly Guid _selfId = Guid.NewGuid();

        /// <summary>This node's process-lifetime self-echo identifier. Exposed for tests only.</summary>
        internal Guid SelfId => _selfId;

        private readonly IMqttClusterTransport _transport;

        private readonly SemaphoreSlim _initLock = new(1, 1);
        private volatile bool _initialized;
        private Task? _requestListenerTask;
        private Task? _replyListenerTask;

        private readonly ConcurrentDictionary<Guid, FanOutSubscription> _fanOutSubscriptions = new();
        private readonly ConcurrentDictionary<Guid, SubscriptionEventSubscription> _subscriptionEventSubscriptions = new();
        private readonly ConcurrentDictionary<Guid, TaskCompletionSource<MqttClusterResponseEnvelope>> _pendingRequests = new();

        private readonly CancellationTokenSource _lifetimeCts = new();

        // Same constructor-injection-for-testability shape as every other broker transport in this
        // repo: transport defaults to the real MqttClusterTransport, but tests substitute an
        // NSubstitute-backed IMqttClusterTransport instead of requiring a live MQTT broker.
        public MqttClusterMessageBus(
            IOptions<MqttClusterMessageBusOptions> options,
            ClusterConfiguration clusterConfiguration,
            IClusterChannelResolver channelResolver,
            ILoggerFactory loggerFactory,
            IMqttClusterTransport? transport = null)
        {
            _options = options.Value;
            _nodeEndpoint = clusterConfiguration.NodeEndpoint
                ?? throw new InvalidOperationException(
                    $"{nameof(ClusterConfiguration)}.{nameof(ClusterConfiguration.NodeEndpoint)} must be set for " +
                    $"{nameof(MqttClusterMessageBus)} to identify this node's request/reply topics.");
            _channelResolver = channelResolver;
            _logger = loggerFactory.CreateLogger<MqttClusterMessageBus>();
            _transport = transport ?? new MqttClusterTransport(_options.Host, _options.Port, _options.ClientId);

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

                var requestTopic = MqttTopicNaming.RequestTopic(_options.TopicPrefix, _nodeEndpoint);
                _requestListenerTask = Task.Run(() => RunRequestListenerLoopAsync(requestTopic, _lifetimeCts.Token));

                var replyTopic = MqttTopicNaming.ReplyTopic(_options.TopicPrefix, _nodeEndpoint);
                _replyListenerTask = Task.Run(() => RunReplyListenerLoopAsync(replyTopic, _lifetimeCts.Token));

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
            [LoggerMessage(EventId = 9900, Level = LogLevel.Debug,
                Message = "[Cluster] MQTT message bus constructed for node '{Host}'; the request/reply listeners are started lazily on first use.")]
            public static partial void Constructed(ILogger logger, string host);

            [LoggerMessage(EventId = 9901, Level = LogLevel.Information,
                Message = "[Cluster] MQTT message bus started for node '{Host}'.")]
            public static partial void Started(ILogger logger, string host);

            [LoggerMessage(EventId = 9902, Level = LogLevel.Warning,
                Message = "[Cluster] MQTT listener loop did not stop within the shutdown grace period.")]
            public static partial void ListenerLoopDidNotStopInTime(ILogger logger, Exception exception);
        }
    }
}
