using System.Collections.Concurrent;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using ThunderPropagator.Application.Channels.Cluster;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.AzureServiceBus
{
    /// <summary>
    /// Azure Service Bus-backed <see cref="IClusterMessageBus"/>: fan-out and subscription-event
    /// propagation each use one topic per channel, with every node creating its own exclusive topic
    /// <em>subscription</em> (a first-class, directly receivable Service Bus entity — unlike
    /// <c>ThunderPropagator.ClusterMessageBuses.AwsSqs</c>'s SNS topics, no separate queue, IAM
    /// policy, or explicit "subscribe queue to topic" call is needed). The three leader/peer-pull
    /// operations (<see cref="RestoreFromLeaderAsync"/>, <see cref="SyncDeltaFromLeaderAsync"/>,
    /// <see cref="FetchPeerSubscriptionsAsync"/>) use a broker-native request/reply scheme identical
    /// in shape to AwsSqs's: every node creates its own request queue and reply queue (named from
    /// its own <see cref="ClusterConfiguration.NodeEndpoint"/>) and sends directly to a peer's
    /// request/reply queue by name.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Like AwsSqs, Service Bus's client SDK is pull-only for this transport's purposes — this class
    /// polls <c>ServiceBusReceiver.ReceiveMessageAsync</c> in a loop itself for every entity it owns
    /// (this node's request queue, reply queue, and one subscription per active fan-out/
    /// subscription-sync subscription) rather than using the event-driven <c>ServiceBusProcessor</c>,
    /// so its lifecycle (start/stop per <see cref="SubscribeAsync(Guid, Func{ClusterFanOutMessage, CancellationToken, Task}, CancellationToken)"/>
    /// call) matches every other broker-style transport in this repo.
    /// </para>
    /// <para>
    /// <see cref="ServiceBusClient"/>, <see cref="ServiceBusSender"/>, <see cref="ServiceBusReceiver"/>,
    /// and <see cref="ServiceBusAdministrationClient"/> are all non-sealed with a protected
    /// parameterless constructor and virtual members — explicitly designed by the Azure SDK team to
    /// be mocked — so, like AwsSqs's <c>IAmazonSQS</c>/<c>IAmazonSimpleNotificationService</c>, they
    /// are substituted directly with NSubstitute in tests; no custom wrapper interface is needed.
    /// </para>
    /// <para>
    /// Azure Service Bus's admin API is not idempotent: creating a topic/subscription/queue that
    /// already exists throws a <see cref="ServiceBusException"/> with
    /// <see cref="ServiceBusException.Reason"/> equal to <see cref="ServiceBusFailureReason.MessagingEntityAlreadyExists"/>,
    /// unlike AWS's create-or-return-existing semantics. Every <c>Ensure*Async</c> helper below
    /// therefore checks existence first and treats the already-exists exception on the subsequent
    /// create call as a benign race rather than a failure.
    /// </para>
    /// </remarks>
    internal sealed partial class AzureServiceBusClusterMessageBus : AbstractClusterMessageBus, IAsyncDisposable
    {
        private readonly AzureServiceBusClusterMessageBusOptions _options;
        private readonly Uri _nodeEndpoint;
        private readonly IClusterChannelResolver _channelResolver;
        private readonly ILogger _logger;
        private readonly Guid _selfId = Guid.NewGuid();

        /// <summary>This node's process-lifetime self-echo identifier. Exposed for tests only.</summary>
        internal Guid SelfId => _selfId;

        private readonly Func<CancellationToken, Task<ServiceBusClient>> _clientFactory;
        private readonly Func<CancellationToken, Task<ServiceBusAdministrationClient>> _adminFactory;

        private readonly SemaphoreSlim _initLock = new(1, 1);
        private volatile bool _initialized;

        private ServiceBusClient? _client;
        private ServiceBusAdministrationClient? _admin;
        private ServiceBusReceiver? _requestReceiver;
        private ServiceBusReceiver? _replyReceiver;

        private readonly ConcurrentDictionary<string, ServiceBusSender> _senderCache = new();

        private readonly ConcurrentDictionary<Guid, FanOutSubscription> _fanOutSubscriptions = new();
        private readonly ConcurrentDictionary<Guid, SubscriptionEventSubscription> _subscriptionEventSubscriptions = new();

        private readonly ConcurrentDictionary<Guid, TaskCompletionSource<AzureServiceBusClusterResponseEnvelope>> _pendingRequests = new();

        private readonly ConcurrentBag<Task> _backgroundTasks = new();
        private readonly CancellationTokenSource _lifetimeCts = new();

        // Same constructor-injection-for-testability shape as AwsSqsClusterMessageBus: the two
        // client factories default to real Azure SDK client builders, but tests substitute
        // NSubstitute-backed ServiceBusClient/ServiceBusAdministrationClient instead of requiring a
        // live Service Bus namespace.
        public AzureServiceBusClusterMessageBus(
            IOptions<AzureServiceBusClusterMessageBusOptions> options,
            ClusterConfiguration clusterConfiguration,
            IClusterChannelResolver channelResolver,
            ILoggerFactory loggerFactory,
            Func<CancellationToken, Task<ServiceBusClient>>? clientFactory = null,
            Func<CancellationToken, Task<ServiceBusAdministrationClient>>? adminFactory = null)
        {
            _options = options.Value;
            _nodeEndpoint = clusterConfiguration.NodeEndpoint
                ?? throw new InvalidOperationException(
                    $"{nameof(ClusterConfiguration)}.{nameof(ClusterConfiguration.NodeEndpoint)} must be set for " +
                    $"{nameof(AzureServiceBusClusterMessageBus)} to identify this node's request/reply queues.");
            _channelResolver = channelResolver;
            _logger = loggerFactory.CreateLogger<AzureServiceBusClusterMessageBus>();
            _clientFactory = clientFactory ?? DefaultClientFactoryAsync;
            _adminFactory = adminFactory ?? DefaultAdminFactoryAsync;

            Log.Constructed(_logger, _nodeEndpoint.Host);
        }

        private Task<ServiceBusClient> DefaultClientFactoryAsync(CancellationToken cancellationToken)
        {
            var clientOptions = new ServiceBusClientOptions();
            _options.ConfigureClientOptions?.Invoke(clientOptions);
            ServiceBusClient client = new ServiceBusClient(_options.ConnectionString, clientOptions);
            return Task.FromResult(client);
        }

        private Task<ServiceBusAdministrationClient> DefaultAdminFactoryAsync(CancellationToken cancellationToken)
        {
            ServiceBusAdministrationClient admin = new ServiceBusAdministrationClient(_options.ConnectionString);
            return Task.FromResult(admin);
        }

        /// <summary>
        /// Lazily builds both Service Bus clients and this node's own request/reply queues, and
        /// starts their pollers, exactly once. Internal (rather than private) so tests can await it
        /// directly to force initialization against substitute clients before exercising the bus.
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

                _client = await _clientFactory(cancellationToken).ConfigureAwait(false);
                _admin = await _adminFactory(cancellationToken).ConfigureAwait(false);

                var requestQueueName = AzureServiceBusResourceNaming.RequestQueue(_options.ResourcePrefix, _nodeEndpoint);
                var replyQueueName = AzureServiceBusResourceNaming.ReplyQueue(_options.ResourcePrefix, _nodeEndpoint);

                await EnsureQueueAsync(requestQueueName, cancellationToken).ConfigureAwait(false);
                await EnsureQueueAsync(replyQueueName, cancellationToken).ConfigureAwait(false);

                _requestReceiver = _client.CreateReceiver(requestQueueName);
                _replyReceiver = _client.CreateReceiver(replyQueueName);

                _backgroundTasks.Add(RunReceiverPollLoopAsync(_requestReceiver, HandleRequestDeliveryAsync, _lifetimeCts.Token));
                _backgroundTasks.Add(RunReceiverPollLoopAsync(_replyReceiver, HandleReplyDeliveryAsync, _lifetimeCts.Token));

                _initialized = true;
                Log.Started(_logger, _nodeEndpoint.Host);
            }
            finally
            {
                _initLock.Release();
            }
        }

        /// <summary>
        /// Creates the queue named <paramref name="queueName"/> if it doesn't already exist.
        /// Internal (rather than private) so tests can drive it directly.
        /// </summary>
        internal async Task EnsureQueueAsync(string queueName, CancellationToken cancellationToken)
        {
            var exists = await _admin!.QueueExistsAsync(queueName, cancellationToken).ConfigureAwait(false);
            if (exists.Value)
                return;

            try
            {
                await _admin!.CreateQueueAsync(queueName, cancellationToken).ConfigureAwait(false);
            }
            catch (ServiceBusException exception) when (exception.Reason == ServiceBusFailureReason.MessagingEntityAlreadyExists)
            {
                // Benign race: another node created it between our existence check and our create
                // call — the entity exists either way, which is all we need.
            }
        }

        /// <summary>Creates the topic named <paramref name="topicName"/> if it doesn't already exist. Internal so tests can drive it directly.</summary>
        internal async Task EnsureTopicAsync(string topicName, CancellationToken cancellationToken)
        {
            var exists = await _admin!.TopicExistsAsync(topicName, cancellationToken).ConfigureAwait(false);
            if (exists.Value)
                return;

            try
            {
                await _admin!.CreateTopicAsync(topicName, cancellationToken).ConfigureAwait(false);
            }
            catch (ServiceBusException exception) when (exception.Reason == ServiceBusFailureReason.MessagingEntityAlreadyExists)
            {
                // Same benign-race reasoning as EnsureQueueAsync.
            }
        }

        /// <summary>
        /// Creates this node's exclusive subscription <paramref name="subscriptionName"/> on
        /// <paramref name="topicName"/> if it doesn't already exist. Internal so tests can drive it
        /// directly. The default subscription rule (no filter configured) passes every message,
        /// behaving like a fan-out exchange out of the box.
        /// </summary>
        internal async Task EnsureSubscriptionAsync(string topicName, string subscriptionName, CancellationToken cancellationToken)
        {
            var exists = await _admin!.SubscriptionExistsAsync(topicName, subscriptionName, cancellationToken).ConfigureAwait(false);
            if (exists.Value)
                return;

            try
            {
                await _admin!.CreateSubscriptionAsync(topicName, subscriptionName, cancellationToken).ConfigureAwait(false);
            }
            catch (ServiceBusException exception) when (exception.Reason == ServiceBusFailureReason.MessagingEntityAlreadyExists)
            {
                // Same benign-race reasoning as EnsureQueueAsync.
            }
        }

        /// <summary>
        /// Returns a cached <see cref="ServiceBusSender"/> for <paramref name="entityName"/>,
        /// creating one on first use. Senders represent AMQP links and are deliberately reused rather
        /// than recreated per send.
        /// </summary>
        internal ServiceBusSender GetSender(string entityName)
            => _senderCache.GetOrAdd(entityName, name => _client!.CreateSender(name));

        /// <summary>
        /// Repeatedly polls <paramref name="receiver"/>, invoking <paramref name="handleBody"/> for
        /// each message's body (as text) and completing the message afterward regardless of whether
        /// the handler succeeded or faulted (a single poisoned message must not be redelivered
        /// forever — same "malformed delivery must not crash the loop" rule as every other transport
        /// in this repo). Internal (rather than private) so tests can drive a single iteration
        /// directly against a substitute receiver.
        /// </summary>
        internal async Task RunReceiverPollLoopAsync(ServiceBusReceiver receiver, Func<string, CancellationToken, Task> handleBody, CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                ServiceBusReceivedMessage? message;
                try
                {
                    message = await receiver.ReceiveMessageAsync(_options.ReceiveMaxWaitTime, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception exception)
                {
                    Log.ReceiveFailed(_logger, exception, receiver.EntityPath);
                    continue;
                }

                if (message is null)
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

                try
                {
                    await handleBody(message.Body.ToString(), cancellationToken).ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    Log.MessageHandlingFaulted(_logger, exception, receiver.EntityPath);
                }

                try
                {
                    await receiver.CompleteMessageAsync(message, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    Log.CompleteMessageFailed(_logger, exception, receiver.EntityPath);
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
            // must still be cleaned up here rather than leaked, same fix already applied to every
            // broker-style transport in this repo.
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

            if (_requestReceiver is not null)
                await _requestReceiver.DisposeAsync().ConfigureAwait(false);

            if (_replyReceiver is not null)
                await _replyReceiver.DisposeAsync().ConfigureAwait(false);

            foreach (var sender in _senderCache.Values)
            {
                await sender.DisposeAsync().ConfigureAwait(false);
            }

            if (_client is not null)
                await _client.DisposeAsync().ConfigureAwait(false);

            _initLock.Dispose();
            _lifetimeCts.Dispose();
        }

        private static partial class Log
        {
            [LoggerMessage(EventId = 9800, Level = LogLevel.Debug,
                Message = "[Cluster] AzureServiceBus message bus constructed for node '{Host}'; clients/queues are built lazily on first use.")]
            public static partial void Constructed(ILogger logger, string host);

            [LoggerMessage(EventId = 9801, Level = LogLevel.Information,
                Message = "[Cluster] AzureServiceBus message bus started for node '{Host}'.")]
            public static partial void Started(ILogger logger, string host);

            [LoggerMessage(EventId = 9802, Level = LogLevel.Warning,
                Message = "[Cluster] AzureServiceBus ReceiveMessage against entity '{EntityPath}' failed; retrying.")]
            public static partial void ReceiveFailed(ILogger logger, Exception exception, string entityPath);

            [LoggerMessage(EventId = 9803, Level = LogLevel.Error,
                Message = "[Cluster] AzureServiceBus message handling faulted for entity '{EntityPath}'.")]
            public static partial void MessageHandlingFaulted(ILogger logger, Exception exception, string entityPath);

            [LoggerMessage(EventId = 9804, Level = LogLevel.Warning,
                Message = "[Cluster] AzureServiceBus CompleteMessage against entity '{EntityPath}' failed.")]
            public static partial void CompleteMessageFailed(ILogger logger, Exception exception, string entityPath);

            [LoggerMessage(EventId = 9805, Level = LogLevel.Warning,
                Message = "[Cluster] A background AzureServiceBus task faulted during dispose.")]
            public static partial void BackgroundTaskFaultedDuringDispose(ILogger logger, Exception exception);
        }
    }
}
