using System.Collections.Concurrent;
using Google.Cloud.PubSub.V1;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using ThunderPropagator.Application.Channels.Cluster;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.GcpPubSub
{
    /// <summary>
    /// GCP Pub/Sub-backed <see cref="IClusterMessageBus"/>: fan-out and subscription-event
    /// propagation each use one topic per channel, with every node creating its own exclusive
    /// subscription on that topic — mirroring
    /// <c>ThunderPropagator.ClusterMessageBuses.AwsSqs</c>'s one-topic-per-channel,
    /// one-exclusive-receiver-per-node design almost exactly, with a Pub/Sub subscription standing
    /// in for an SQS queue. Unlike AwsSqs, though, Pub/Sub has no separate queue primitive at all —
    /// so the three leader/peer-pull operations (<see cref="RestoreFromLeaderAsync"/>,
    /// <see cref="SyncDeltaFromLeaderAsync"/>, <see cref="FetchPeerSubscriptionsAsync"/>) also go
    /// through a topic + single-subscription pair per node (this node's own request topic/subscription,
    /// and its own reply topic/subscription) rather than a real queue, matching CLAUDE.md's
    /// "brokers with only topics/subscriptions" pattern almost exactly.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Neither <see cref="PublisherServiceApiClient"/> nor <see cref="SubscriberServiceApiClient"/>
    /// expose a push/streaming receive API in the shape this repo's other transports use — this
    /// transport polls <c>PullAsync</c> in a loop itself for every subscription it owns (this node's
    /// request subscription, reply subscription, and one subscription per active fan-out/
    /// subscription-sync subscription), acknowledging each message via <c>AcknowledgeAsync</c> once
    /// handled, the same shape as <c>AwsSqsClusterMessageBus</c>'s <c>ReceiveMessageAsync</c>/
    /// <c>DeleteMessageAsync</c> poll loop.
    /// </para>
    /// <para>
    /// Both Pub/Sub clients are GAPIC-generated classes with a protected parameterless constructor
    /// and virtual members specifically designed for direct substitution in tests (Google's own
    /// documented pattern, the same reason <c>Amazon.SQS.IAmazonSQS</c>/<c>IAmazonSimpleNotificationService</c>
    /// need no custom wrapper interface either) — so, like those, no custom wrapper interface is
    /// needed here.
    /// </para>
    /// <para>
    /// <see cref="ClusterConfiguration.NodeEndpoint"/> must be set for this transport to work, even
    /// though nothing here is actually reached over HTTP — it's reused purely as this node's stable
    /// identity for topic/subscription naming (<see cref="GcpPubSubResourceNaming.Slugify"/>), exactly
    /// as <c>RabbitMqClusterMessageBus</c>/<c>KafkaClusterMessageBus</c>/<c>AwsSqsClusterMessageBus</c>
    /// reuse it.
    /// </para>
    /// </remarks>
    internal sealed partial class GcpPubSubClusterMessageBus : AbstractClusterMessageBus, IAsyncDisposable
    {
        private readonly GcpPubSubClusterMessageBusOptions _options;
        private readonly Uri _nodeEndpoint;
        private readonly IClusterChannelResolver _channelResolver;
        private readonly ILogger _logger;
        private readonly Guid _selfId = Guid.NewGuid();

        /// <summary>This node's process-lifetime self-echo identifier. Exposed for tests only.</summary>
        internal Guid SelfId => _selfId;

        private readonly Func<CancellationToken, Task<PublisherServiceApiClient>> _publisherFactory;
        private readonly Func<CancellationToken, Task<SubscriberServiceApiClient>> _subscriberFactory;

        private readonly SemaphoreSlim _initLock = new(1, 1);
        private volatile bool _initialized;

        private PublisherServiceApiClient? _publisher;
        private SubscriberServiceApiClient? _subscriber;
        private SubscriptionName? _requestSubscriptionName;
        private TopicName? _requestTopicName;

        private readonly ConcurrentDictionary<Guid, FanOutSubscription> _fanOutSubscriptions = new();
        private readonly ConcurrentDictionary<Guid, SubscriptionEventSubscription> _subscriptionEventSubscriptions = new();

        private readonly ConcurrentDictionary<Guid, TaskCompletionSource<GcpPubSubClusterResponseEnvelope>> _pendingRequests = new();

        private readonly ConcurrentBag<Task> _backgroundTasks = new();
        private readonly CancellationTokenSource _lifetimeCts = new();

        // Same constructor-injection-for-testability shape as AwsSqsClusterMessageBus/
        // RabbitMqClusterMessageBus: the two client factories default to real GAPIC client builders,
        // but tests substitute NSubstitute-backed PublisherServiceApiClient/SubscriberServiceApiClient
        // instead of requiring a live GCP project.
        public GcpPubSubClusterMessageBus(
            IOptions<GcpPubSubClusterMessageBusOptions> options,
            ClusterConfiguration clusterConfiguration,
            IClusterChannelResolver channelResolver,
            ILoggerFactory loggerFactory,
            Func<CancellationToken, Task<PublisherServiceApiClient>>? publisherFactory = null,
            Func<CancellationToken, Task<SubscriberServiceApiClient>>? subscriberFactory = null)
        {
            _options = options.Value;
            if (string.IsNullOrWhiteSpace(_options.ProjectId))
            {
                throw new InvalidOperationException(
                    $"{nameof(GcpPubSubClusterMessageBusOptions)}.{nameof(GcpPubSubClusterMessageBusOptions.ProjectId)} must be set for " +
                    $"{nameof(GcpPubSubClusterMessageBus)} to name/create its topics and subscriptions.");
            }

            _nodeEndpoint = clusterConfiguration.NodeEndpoint
                ?? throw new InvalidOperationException(
                    $"{nameof(ClusterConfiguration)}.{nameof(ClusterConfiguration.NodeEndpoint)} must be set for " +
                    $"{nameof(GcpPubSubClusterMessageBus)} to identify this node's request/reply topics.");

            _channelResolver = channelResolver;
            _logger = loggerFactory.CreateLogger<GcpPubSubClusterMessageBus>();
            _publisherFactory = publisherFactory ?? DefaultPublisherFactoryAsync;
            _subscriberFactory = subscriberFactory ?? DefaultSubscriberFactoryAsync;

            Log.Constructed(_logger, _nodeEndpoint.Host);
        }

        private async Task<PublisherServiceApiClient> DefaultPublisherFactoryAsync(CancellationToken cancellationToken)
        {
            var builder = new PublisherServiceApiClientBuilder();
            _options.ConfigurePublisherClient?.Invoke(builder);
            return await builder.BuildAsync(cancellationToken).ConfigureAwait(false);
        }

        private async Task<SubscriberServiceApiClient> DefaultSubscriberFactoryAsync(CancellationToken cancellationToken)
        {
            var builder = new SubscriberServiceApiClientBuilder();
            _options.ConfigureSubscriberClient?.Invoke(builder);
            return await builder.BuildAsync(cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Lazily builds both Pub/Sub clients and this node's own request/reply topics and
        /// subscriptions, and starts the request-subscription poller, exactly once. Internal (rather
        /// than private) so tests can await it directly to force initialization against substitute
        /// clients before exercising the bus.
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

                _publisher = await _publisherFactory(cancellationToken).ConfigureAwait(false);
                _subscriber = await _subscriberFactory(cancellationToken).ConfigureAwait(false);

                _requestTopicName = new TopicName(_options.ProjectId, GcpPubSubResourceNaming.RequestTopicId(_options.ResourcePrefix, _nodeEndpoint));
                _requestSubscriptionName = new SubscriptionName(_options.ProjectId, GcpPubSubResourceNaming.RequestSubscriptionId(_options.ResourcePrefix, _nodeEndpoint));
                var replyTopicName = new TopicName(_options.ProjectId, GcpPubSubResourceNaming.ReplyTopicId(_options.ResourcePrefix, _nodeEndpoint));
                var replySubscriptionName = new SubscriptionName(_options.ProjectId, GcpPubSubResourceNaming.ReplySubscriptionId(_options.ResourcePrefix, _nodeEndpoint));

                await EnsureTopicAsync(_requestTopicName, cancellationToken).ConfigureAwait(false);
                await EnsureSubscriptionAsync(_requestSubscriptionName, _requestTopicName, cancellationToken).ConfigureAwait(false);
                await EnsureTopicAsync(replyTopicName, cancellationToken).ConfigureAwait(false);
                await EnsureSubscriptionAsync(replySubscriptionName, replyTopicName, cancellationToken).ConfigureAwait(false);

                _backgroundTasks.Add(RunSubscriptionPollLoopAsync(_requestSubscriptionName, HandleRequestDeliveryAsync, _lifetimeCts.Token));
                _backgroundTasks.Add(RunSubscriptionPollLoopAsync(replySubscriptionName, HandleReplyDeliveryAsync, _lifetimeCts.Token));

                _initialized = true;
                Log.Started(_logger, _nodeEndpoint.Host);
            }
            finally
            {
                _initLock.Release();
            }
        }

        /// <summary>
        /// Creates <paramref name="topicName"/> if it doesn't already exist. GAPIC's <c>CreateTopicAsync</c>
        /// is not itself idempotent (it throws on an existing topic), unlike SQS's <c>CreateQueueAsync</c>
        /// — catching the resulting <see cref="StatusCode.AlreadyExists"/> here is what makes this
        /// idempotent instead. Internal (rather than private) so tests can drive it directly.
        /// </summary>
        internal async Task EnsureTopicAsync(TopicName topicName, CancellationToken cancellationToken)
        {
            try
            {
                await _publisher!.CreateTopicAsync(topicName, cancellationToken).ConfigureAwait(false);
            }
            catch (RpcException exception) when (exception.StatusCode == StatusCode.AlreadyExists)
            {
                // Already there — exactly what we want.
            }
        }

        /// <summary>
        /// Creates <paramref name="subscriptionName"/> on <paramref name="topicName"/> if it doesn't
        /// already exist (idempotent, same reasoning as <see cref="EnsureTopicAsync"/>).
        /// </summary>
        internal async Task EnsureSubscriptionAsync(SubscriptionName subscriptionName, TopicName topicName, CancellationToken cancellationToken)
        {
            try
            {
                await _subscriber!.CreateSubscriptionAsync(
                    subscriptionName, topicName, pushConfig: null, ackDeadlineSeconds: _options.AckDeadlineSeconds, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (RpcException exception) when (exception.StatusCode == StatusCode.AlreadyExists)
            {
                // Already there — exactly what we want.
            }
        }

        /// <summary>
        /// Repeatedly pulls <paramref name="subscriptionName"/>, invoking <paramref name="handleBody"/>
        /// for each message's UTF-8 payload and acknowledging the whole batch afterward regardless of
        /// whether any individual handler succeeded or faulted (a single poisoned message must not be
        /// redelivered forever — the handler itself is responsible for logging/swallowing its own
        /// failures, matching every other transport's "malformed delivery must not crash the loop"
        /// rule). Internal (rather than private) so tests can drive a single iteration directly
        /// against a substitute client.
        /// </summary>
        internal async Task RunSubscriptionPollLoopAsync(SubscriptionName subscriptionName, Func<string, CancellationToken, Task> handleBody, CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                PullResponse response;
                try
                {
                    response = await _subscriber!.PullAsync(subscriptionName, maxMessages: _options.PullMaxMessages, cancellationToken)
                        .ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception exception)
                {
                    Log.PullFailed(_logger, exception, subscriptionName.SubscriptionId);
                    continue;
                }

                if (response.ReceivedMessages.Count == 0)
                {
                    try
                    {
                        await Task.Delay(_options.EmptyPollDelay, cancellationToken).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        return;
                    }

                    continue;
                }

                foreach (var received in response.ReceivedMessages)
                {
                    try
                    {
                        await handleBody(received.Message.Data.ToStringUtf8(), cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception exception)
                    {
                        Log.MessageHandlingFaulted(_logger, exception, subscriptionName.SubscriptionId);
                    }
                }

                try
                {
                    await _subscriber!.AcknowledgeAsync(
                        subscriptionName, response.ReceivedMessages.Select(m => m.AckId), cancellationToken)
                        .ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    Log.AcknowledgeFailed(_logger, exception, subscriptionName.SubscriptionId);
                }
            }
        }

        public async ValueTask DisposeAsync()
        {
            await _lifetimeCts.CancelAsync().ConfigureAwait(false);

            foreach (var pending in _pendingRequests.Values)
            {
                pending.TrySetCanceled();
            }

            // Callers are expected to dispose each SubscribeAsync handle themselves — but if the
            // whole bus is torn down first without that happening, every still-open subscription
            // must still be cleaned up here rather than leaked (same fix already applied to every
            // broker-style transport in this repo after it was first found via a dedicated
            // regression test on KafkaClusterMessageBus).
            foreach (var fanOutSubscription in _fanOutSubscriptions.Values.ToArray())
            {
                await fanOutSubscription.DisposeAsync().ConfigureAwait(false);
            }

            foreach (var subscriptionEventSubscription in _subscriptionEventSubscriptions.Values.ToArray())
            {
                await subscriptionEventSubscription.DisposeAsync().ConfigureAwait(false);
            }

            try
            {
                await Task.WhenAll(_backgroundTasks.ToArray()).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected — every poll loop observes _lifetimeCts.
            }
            catch (Exception exception)
            {
                Log.BackgroundTaskFaultedDuringDispose(_logger, exception);
            }

            (_publisher as IDisposable)?.Dispose();
            (_subscriber as IDisposable)?.Dispose();

            _initLock.Dispose();
            _lifetimeCts.Dispose();
        }

        private static partial class Log
        {
            [LoggerMessage(EventId = 9800, Level = LogLevel.Debug,
                Message = "[Cluster] GcpPubSub message bus constructed for node '{Host}'; clients/topics/subscriptions are built lazily on first use.")]
            public static partial void Constructed(ILogger logger, string host);

            [LoggerMessage(EventId = 9801, Level = LogLevel.Information,
                Message = "[Cluster] GcpPubSub message bus started for node '{Host}'.")]
            public static partial void Started(ILogger logger, string host);

            [LoggerMessage(EventId = 9802, Level = LogLevel.Warning,
                Message = "[Cluster] GcpPubSub Pull against subscription '{Subscription}' failed; retrying.")]
            public static partial void PullFailed(ILogger logger, Exception exception, string subscription);

            [LoggerMessage(EventId = 9803, Level = LogLevel.Error,
                Message = "[Cluster] GcpPubSub message handling faulted for subscription '{Subscription}'.")]
            public static partial void MessageHandlingFaulted(ILogger logger, Exception exception, string subscription);

            [LoggerMessage(EventId = 9804, Level = LogLevel.Warning,
                Message = "[Cluster] GcpPubSub Acknowledge against subscription '{Subscription}' failed.")]
            public static partial void AcknowledgeFailed(ILogger logger, Exception exception, string subscription);

            [LoggerMessage(EventId = 9805, Level = LogLevel.Warning,
                Message = "[Cluster] A background GcpPubSub task faulted during dispose.")]
            public static partial void BackgroundTaskFaultedDuringDispose(ILogger logger, Exception exception);
        }
    }
}
