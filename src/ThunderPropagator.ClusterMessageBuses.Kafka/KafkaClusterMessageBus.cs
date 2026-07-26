using System.Collections.Concurrent;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using ThunderPropagator.Application.Channels.Cluster;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;

using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.Kafka
{
    /// <summary>
    /// Kafka-backed <see cref="IClusterMessageBus"/>: fan-out and subscription-event propagation
    /// use one topic per channel, consumed by every node through its own ephemeral,
    /// unique-per-process consumer group (so every node gets its own full copy of the broadcast —
    /// a shared/competing-consumer group would split messages across nodes instead of delivering
    /// them to all of them, which is wrong for a fan-out bus). The three leader/peer-pull
    /// operations (<see cref="RestoreFromLeaderAsync"/>, <see cref="SyncDeltaFromLeaderAsync"/>,
    /// <see cref="FetchPeerSubscriptionsAsync"/>) use a broker-native request/reply scheme: every
    /// node listens on its own request topic (named from its own
    /// <see cref="ClusterConfiguration.NodeEndpoint"/>) and answers by resolving the requested
    /// channel locally via <see cref="IClusterChannelResolver"/> and the protected members exposed by
    /// <see cref="AbstractClusterMessageBus"/>, replying on the requester's own reply topic.
    /// </summary>
    /// <remarks>
    /// <see cref="ClusterConfiguration.NodeEndpoint"/> must be set for this transport to work, even
    /// though nothing here is actually reached over HTTP — it's reused purely as this node's stable
    /// identity for topic naming (<see cref="KafkaTopicNaming.Slugify"/>), exactly as
    /// <c>HttpClusterMessageBus</c> reuses it as a literal base URI.
    /// </remarks>
    internal sealed partial class KafkaClusterMessageBus : AbstractClusterMessageBus, IAsyncDisposable
    {
        private readonly KafkaClusterMessageBusOptions _options;
        private readonly Uri _nodeEndpoint;
        private readonly IClusterChannelResolver _channelResolver;
        private readonly ILogger _logger;
        private readonly Guid _selfId = Guid.NewGuid();

        /// <summary>This node's process-lifetime self-echo identifier. Exposed for tests only.</summary>
        internal Guid SelfId => _selfId;

        private readonly IProducer<string, string> _producer;
        private readonly Func<ConsumerConfig, IConsumer<string, string>> _consumerFactory;

        private readonly ConcurrentDictionary<Guid, FanOutSubscription> _fanOutSubscriptions = new();
        private readonly ConcurrentDictionary<Guid, SubscriptionEventSubscription> _subscriptionEventSubscriptions = new();

        private readonly ConcurrentDictionary<Guid, TaskCompletionSource<KafkaClusterResponseEnvelope>> _pendingRequests = new();
        private readonly CancellationTokenSource _lifetimeCts = new();
        private readonly Task _requestListenerLoop;
        private readonly Task _replyListenerLoop;
        private readonly IConsumer<string, string> _requestConsumer;
        private readonly IConsumer<string, string> _replyConsumer;

        // Same constructor-injection-for-testability shape as HttpClusterMessageBus (which takes an
        // HttpClient backed by a fake handler in tests): producerFactory/consumerFactory default to
        // the real Confluent.Kafka builders but tests substitute NSubstitute-backed IProducer/IConsumer
        // instances instead of requiring a live broker.
        public KafkaClusterMessageBus(
            IOptions<KafkaClusterMessageBusOptions> options,
            ClusterConfiguration clusterConfiguration,
            IClusterChannelResolver channelResolver,
            ILoggerFactory loggerFactory,
            Func<ProducerConfig, IProducer<string, string>>? producerFactory = null,
            Func<ConsumerConfig, IConsumer<string, string>>? consumerFactory = null)
        {
            _options = options.Value;
            _nodeEndpoint = clusterConfiguration.NodeEndpoint
                ?? throw new InvalidOperationException(
                    $"{nameof(ClusterConfiguration)}.{nameof(ClusterConfiguration.NodeEndpoint)} must be set for " +
                    $"{nameof(KafkaClusterMessageBus)} to identify this node's request/reply topics.");
            _channelResolver = channelResolver;
            _logger = loggerFactory.CreateLogger<KafkaClusterMessageBus>();

            var producerConfig = new ProducerConfig { BootstrapServers = _options.BootstrapServers };
            _options.ConfigureProducer?.Invoke(producerConfig);
            _producer = (producerFactory ?? (config => new ProducerBuilder<string, string>(config).Build()))(producerConfig);

            _consumerFactory = consumerFactory ?? (config => new ConsumerBuilder<string, string>(config).Build());

            _requestConsumer = _consumerFactory(BuildConsumerConfig($"{_options.ConsumerGroupPrefix}-requests-{Guid.NewGuid():N}"));
            _requestConsumer.Subscribe(KafkaTopicNaming.RequestTopic(_options.TopicPrefix, _nodeEndpoint));
            _requestListenerLoop = Task.Run(() => RunRequestListenerLoopAsync(_lifetimeCts.Token));

            _replyConsumer = _consumerFactory(BuildConsumerConfig($"{_options.ConsumerGroupPrefix}-replies-{Guid.NewGuid():N}"));
            _replyConsumer.Subscribe(KafkaTopicNaming.ReplyTopic(_options.TopicPrefix, _nodeEndpoint));
            _replyListenerLoop = Task.Run(() => RunReplyListenerLoopAsync(_lifetimeCts.Token));

            Log.Started(_logger, _nodeEndpoint.Host);
        }

        private ConsumerConfig BuildConsumerConfig(string groupId)
        {
            var config = new ConsumerConfig { BootstrapServers = _options.BootstrapServers };
            _options.ConfigureConsumer?.Invoke(config);

            // Enforced regardless of ConfigureConsumer: every consumer this transport creates needs
            // its own unique, never-committed group so it always reads only new messages published
            // from "now" on, matching HttpClusterMessageBus's real-time-only fan-out semantics (no
            // historical replay on (re)start).
            config.GroupId = groupId;
            config.AutoOffsetReset = AutoOffsetReset.Latest;
            config.EnableAutoCommit = false;

            return config;
        }

        public async ValueTask DisposeAsync()
        {
            await _lifetimeCts.CancelAsync().ConfigureAwait(false);

            foreach (var loop in new[] { _requestListenerLoop, _replyListenerLoop })
            {
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
            // that happening, every still-open per-channel consumer must still be closed here
            // rather than leaked. Each subscription's own loop already stops on its own (its
            // CancellationTokenSource is linked to _lifetimeCts, cancelled above); disposing it
            // here just closes/disposes its IConsumer and removes it from the dictionary.
            foreach (var fanOutSubscription in _fanOutSubscriptions.Values.ToArray())
            {
                await fanOutSubscription.DisposeAsync().ConfigureAwait(false);
            }

            foreach (var subscriptionEventSubscription in _subscriptionEventSubscriptions.Values.ToArray())
            {
                await subscriptionEventSubscription.DisposeAsync().ConfigureAwait(false);
            }

            _requestConsumer.Close();
            _requestConsumer.Dispose();
            _replyConsumer.Close();
            _replyConsumer.Dispose();
            _producer.Dispose();
            _lifetimeCts.Dispose();
        }

        private static partial class Log
        {
            [LoggerMessage(EventId = 9500, Level = LogLevel.Information,
                Message = "[Cluster] Kafka message bus started for node '{Host}'.")]
            public static partial void Started(ILogger logger, string host);

            [LoggerMessage(EventId = 9501, Level = LogLevel.Warning,
                Message = "[Cluster] Kafka listener loop did not stop within the shutdown grace period.")]
            public static partial void ListenerLoopDidNotStopInTime(ILogger logger, Exception exception);
        }
    }
}
