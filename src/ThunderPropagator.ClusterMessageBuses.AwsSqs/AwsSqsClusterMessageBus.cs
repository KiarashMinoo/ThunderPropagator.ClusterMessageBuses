using System.Collections.Concurrent;
using Amazon.SimpleNotificationService;
using Amazon.SQS;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using ThunderPropagator.Application.Channels.Cluster;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.AwsSqs
{
    /// <summary>
    /// AWS SNS/SQS-backed <see cref="IClusterMessageBus"/>: fan-out and subscription-event
    /// propagation each use one SNS topic per channel, with every node creating its own exclusive
    /// SQS queue subscribed to that topic (raw message delivery enabled, so a queue message body is
    /// exactly the serialized payload with no SNS envelope to unwrap) — mirroring
    /// <c>ThunderPropagator.ClusterMessageBuses.RabbitMQ</c>'s one-exchange-per-channel,
    /// one-exclusive-queue-per-node design almost exactly, with SNS topics standing in for fanout
    /// exchanges. The three leader/peer-pull operations
    /// (<see cref="RestoreFromLeaderAsync"/>, <see cref="SyncDeltaFromLeaderAsync"/>,
    /// <see cref="FetchPeerSubscriptionsAsync"/>) use a broker-native request/reply scheme: every
    /// node creates its own request queue and reply queue (named from its own
    /// <see cref="ClusterConfiguration.NodeEndpoint"/>) and sends directly to a peer's request/reply
    /// queue (resolved via <c>GetQueueUrlAsync</c>).
    /// </summary>
    /// <remarks>
    /// <para>
    /// Neither <see cref="Amazon.SQS.IAmazonSQS"/> nor <see cref="Amazon.SimpleNotificationService.IAmazonSimpleNotificationService"/>
    /// expose a push/streaming receive API — unlike every message-broker client library used
    /// elsewhere in this repo (Confluent.Kafka's consumer loop, RabbitMQ.Client's async event
    /// consumer, MQTTnet's message-received event, etc.), SQS is pull-only: this transport must poll
    /// <c>ReceiveMessageAsync</c> in a loop itself for every queue it owns (this node's request
    /// queue, reply queue, and one queue per active fan-out/subscription-sync subscription), deleting
    /// each message via <c>DeleteMessageAsync</c> once handled. <see cref="AwsSqsClusterMessageBusOptions.ReceiveWaitTimeSeconds"/>
    /// enables SQS's own server-side long polling so this isn't a tight busy-loop in production.
    /// </para>
    /// <para>
    /// Both AWS clients are plain public interfaces with ordinary (non-extension-method) async
    /// members, so — like Confluent.Kafka's <c>IConsumer</c>/<c>IProducer</c> and RabbitMQ.Client's
    /// <c>IConnection</c>/<c>IChannel</c> — they're substituted directly with NSubstitute in tests;
    /// no custom wrapper interface is needed.
    /// </para>
    /// <para>
    /// <see cref="ClusterConfiguration.NodeEndpoint"/> must be set for this transport to work, even
    /// though nothing here is actually reached over HTTP — it's reused purely as this node's stable
    /// identity for queue naming (<see cref="AwsSqsResourceNaming.Slugify"/>), exactly as
    /// <c>RabbitMqClusterMessageBus</c>/<c>KafkaClusterMessageBus</c> reuse it.
    /// </para>
    /// </remarks>
    internal sealed partial class AwsSqsClusterMessageBus : AbstractClusterMessageBus, IAsyncDisposable
    {
        private readonly AwsSqsClusterMessageBusOptions _options;
        private readonly Uri _nodeEndpoint;
        private readonly IClusterChannelResolver _channelResolver;
        private readonly ILogger _logger;
        private readonly Guid _selfId = Guid.NewGuid();

        /// <summary>This node's process-lifetime self-echo identifier. Exposed for tests only.</summary>
        internal Guid SelfId => _selfId;

        private readonly Func<CancellationToken, Task<IAmazonSQS>> _sqsFactory;
        private readonly Func<CancellationToken, Task<IAmazonSimpleNotificationService>> _snsFactory;

        private readonly SemaphoreSlim _initLock = new(1, 1);
        private volatile bool _initialized;

        private IAmazonSQS? _sqs;
        private IAmazonSimpleNotificationService? _sns;
        private string? _requestQueueUrl;
        private string? _replyQueueUrl;

        private readonly ConcurrentDictionary<Guid, FanOutSubscription> _fanOutSubscriptions = new();
        private readonly ConcurrentDictionary<Guid, SubscriptionEventSubscription> _subscriptionEventSubscriptions = new();

        private readonly ConcurrentDictionary<Guid, TaskCompletionSource<AwsSqsClusterResponseEnvelope>> _pendingRequests = new();

        private readonly ConcurrentBag<Task> _backgroundTasks = new();
        private readonly CancellationTokenSource _lifetimeCts = new();

        // Same constructor-injection-for-testability shape as RabbitMqClusterMessageBus/
        // KafkaClusterMessageBus: the two client factories default to real AWSSDK client builders,
        // but tests substitute NSubstitute-backed IAmazonSQS/IAmazonSimpleNotificationService
        // instead of requiring live AWS resources.
        public AwsSqsClusterMessageBus(
            IOptions<AwsSqsClusterMessageBusOptions> options,
            ClusterConfiguration clusterConfiguration,
            IClusterChannelResolver channelResolver,
            ILoggerFactory loggerFactory,
            Func<CancellationToken, Task<IAmazonSQS>>? sqsFactory = null,
            Func<CancellationToken, Task<IAmazonSimpleNotificationService>>? snsFactory = null)
        {
            _options = options.Value;
            _nodeEndpoint = clusterConfiguration.NodeEndpoint
                ?? throw new InvalidOperationException(
                    $"{nameof(ClusterConfiguration)}.{nameof(ClusterConfiguration.NodeEndpoint)} must be set for " +
                    $"{nameof(AwsSqsClusterMessageBus)} to identify this node's request/reply queues.");
            _channelResolver = channelResolver;
            _logger = loggerFactory.CreateLogger<AwsSqsClusterMessageBus>();
            _sqsFactory = sqsFactory ?? DefaultSqsFactoryAsync;
            _snsFactory = snsFactory ?? DefaultSnsFactoryAsync;

            Log.Constructed(_logger, _nodeEndpoint.Host);
        }

        private Task<IAmazonSQS> DefaultSqsFactoryAsync(CancellationToken cancellationToken)
        {
            var config = new AmazonSQSConfig();
            _options.ConfigureSqsClient?.Invoke(config);
            IAmazonSQS client = new AmazonSQSClient(config);
            return Task.FromResult(client);
        }

        private Task<IAmazonSimpleNotificationService> DefaultSnsFactoryAsync(CancellationToken cancellationToken)
        {
            var config = new AmazonSimpleNotificationServiceConfig();
            _options.ConfigureSnsClient?.Invoke(config);
            IAmazonSimpleNotificationService client = new AmazonSimpleNotificationServiceClient(config);
            return Task.FromResult(client);
        }

        /// <summary>
        /// Lazily builds both AWS clients and this node's own request/reply queues, and starts their
        /// pollers, exactly once. Internal (rather than private) so tests can await it directly to
        /// force initialization against substitute clients before exercising the bus.
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

                _sqs = await _sqsFactory(cancellationToken).ConfigureAwait(false);
                _sns = await _snsFactory(cancellationToken).ConfigureAwait(false);

                _requestQueueUrl = await EnsureQueueAsync(AwsSqsResourceNaming.RequestQueue(_options.ResourcePrefix, _nodeEndpoint), cancellationToken).ConfigureAwait(false);
                _replyQueueUrl = await EnsureQueueAsync(AwsSqsResourceNaming.ReplyQueue(_options.ResourcePrefix, _nodeEndpoint), cancellationToken).ConfigureAwait(false);

                _backgroundTasks.Add(RunQueuePollLoopAsync(_requestQueueUrl, HandleRequestDeliveryAsync, _lifetimeCts.Token));
                _backgroundTasks.Add(RunQueuePollLoopAsync(_replyQueueUrl, HandleReplyDeliveryAsync, _lifetimeCts.Token));

                _initialized = true;
                Log.Started(_logger, _nodeEndpoint.Host);
            }
            finally
            {
                _initLock.Release();
            }
        }

        /// <summary>
        /// Creates <paramref name="queueName"/> if it doesn't already exist (idempotent — SQS
        /// returns the same queue's URL if one with this name and the same attributes already
        /// exists) and returns its URL. Internal (rather than private) so tests can drive it
        /// directly.
        /// </summary>
        internal async Task<string> EnsureQueueAsync(string queueName, CancellationToken cancellationToken)
        {
            var response = await _sqs!.CreateQueueAsync(new Amazon.SQS.Model.CreateQueueRequest
            {
                QueueName = queueName,
                Attributes = new Dictionary<string, string>
                {
                    ["VisibilityTimeout"] = _options.VisibilityTimeoutSeconds.ToString(System.Globalization.CultureInfo.InvariantCulture),
                },
            }, cancellationToken).ConfigureAwait(false);

            return response.QueueUrl;
        }

        /// <summary>Fetches a queue's ARN — needed to subscribe it to an SNS topic and to grant that topic publish permission.</summary>
        internal async Task<string> GetQueueArnAsync(string queueUrl, CancellationToken cancellationToken)
        {
            var response = await _sqs!.GetQueueAttributesAsync(new Amazon.SQS.Model.GetQueueAttributesRequest
            {
                QueueUrl = queueUrl,
                AttributeNames = ["QueueArn"],
            }, cancellationToken).ConfigureAwait(false);

            return response.Attributes["QueueArn"];
        }

        /// <summary>Creates <paramref name="topicName"/> if it doesn't already exist (idempotent, same as <see cref="EnsureQueueAsync"/>) and returns its ARN.</summary>
        internal async Task<string> EnsureTopicAsync(string topicName, CancellationToken cancellationToken)
        {
            var response = await _sns!.CreateTopicAsync(new Amazon.SimpleNotificationService.Model.CreateTopicRequest
            {
                Name = topicName,
            }, cancellationToken).ConfigureAwait(false);

            return response.TopicArn;
        }

        /// <summary>
        /// Grants <paramref name="topicArn"/> permission to deliver to <paramref name="queueUrl"/> —
        /// required before subscribing the queue to the topic, since SQS queues default to denying
        /// every principal but their own account/owner. See <see cref="AwsSqsQueuePolicy"/>.
        /// </summary>
        internal Task GrantTopicPublishToQueueAsync(string queueUrl, string queueArn, string topicArn, CancellationToken cancellationToken)
        {
            return _sqs!.SetQueueAttributesAsync(new Amazon.SQS.Model.SetQueueAttributesRequest
            {
                QueueUrl = queueUrl,
                Attributes = new Dictionary<string, string>
                {
                    ["Policy"] = AwsSqsQueuePolicy.AllowSnsPublish(queueArn, topicArn),
                },
            }, cancellationToken);
        }

        /// <summary>
        /// Subscribes <paramref name="queueArn"/> to <paramref name="topicArn"/> with raw message
        /// delivery enabled (so a delivered SQS message body is exactly the published payload, with
        /// no SNS envelope to unwrap) and returns the resulting subscription's ARN.
        /// </summary>
        internal async Task<string> SubscribeQueueToTopicAsync(string topicArn, string queueArn, CancellationToken cancellationToken)
        {
            var response = await _sns!.SubscribeAsync(new Amazon.SimpleNotificationService.Model.SubscribeRequest
            {
                TopicArn = topicArn,
                Protocol = "sqs",
                Endpoint = queueArn,
                Attributes = new Dictionary<string, string> { ["RawMessageDelivery"] = "true" },
            }, cancellationToken).ConfigureAwait(false);

            return response.SubscriptionArn;
        }

        /// <summary>
        /// Repeatedly long-polls <paramref name="queueUrl"/>, invoking <paramref name="handleBody"/>
        /// for each message's raw body and deleting the message afterward regardless of whether the
        /// handler succeeded or faulted (a single poisoned message must not be redelivered forever —
        /// the handler itself is responsible for logging/swallowing its own failures, matching every
        /// other transport's "malformed delivery must not crash the loop" rule). Internal (rather
        /// than private) so tests can drive a single iteration directly against a substitute client.
        /// </summary>
        internal async Task RunQueuePollLoopAsync(string queueUrl, Func<string, CancellationToken, Task> handleBody, CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                Amazon.SQS.Model.ReceiveMessageResponse response;
                try
                {
                    response = await _sqs!.ReceiveMessageAsync(new Amazon.SQS.Model.ReceiveMessageRequest
                    {
                        QueueUrl = queueUrl,
                        MaxNumberOfMessages = 10,
                        WaitTimeSeconds = _options.ReceiveWaitTimeSeconds,
                    }, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception exception)
                {
                    Log.ReceiveFailed(_logger, exception, queueUrl);
                    continue;
                }

                if (response.Messages.Count == 0)
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

                foreach (var message in response.Messages)
                {
                    try
                    {
                        await handleBody(message.Body, cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception exception)
                    {
                        Log.MessageHandlingFaulted(_logger, exception, queueUrl);
                    }

                    try
                    {
                        await _sqs!.DeleteMessageAsync(new Amazon.SQS.Model.DeleteMessageRequest
                        {
                            QueueUrl = queueUrl,
                            ReceiptHandle = message.ReceiptHandle,
                        }, cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception exception)
                    {
                        Log.DeleteMessageFailed(_logger, exception, queueUrl);
                    }
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

            _sqs?.Dispose();
            _sns?.Dispose();

            _initLock.Dispose();
            _lifetimeCts.Dispose();
        }

        private static partial class Log
        {
            [LoggerMessage(EventId = 9700, Level = LogLevel.Debug,
                Message = "[Cluster] AwsSqs message bus constructed for node '{Host}'; clients/queues are built lazily on first use.")]
            public static partial void Constructed(ILogger logger, string host);

            [LoggerMessage(EventId = 9701, Level = LogLevel.Information,
                Message = "[Cluster] AwsSqs message bus started for node '{Host}'.")]
            public static partial void Started(ILogger logger, string host);

            [LoggerMessage(EventId = 9702, Level = LogLevel.Warning,
                Message = "[Cluster] AwsSqs ReceiveMessage against queue '{QueueUrl}' failed; retrying.")]
            public static partial void ReceiveFailed(ILogger logger, Exception exception, string queueUrl);

            [LoggerMessage(EventId = 9703, Level = LogLevel.Error,
                Message = "[Cluster] AwsSqs message handling faulted for queue '{QueueUrl}'.")]
            public static partial void MessageHandlingFaulted(ILogger logger, Exception exception, string queueUrl);

            [LoggerMessage(EventId = 9704, Level = LogLevel.Warning,
                Message = "[Cluster] AwsSqs DeleteMessage against queue '{QueueUrl}' failed.")]
            public static partial void DeleteMessageFailed(ILogger logger, Exception exception, string queueUrl);

            [LoggerMessage(EventId = 9705, Level = LogLevel.Warning,
                Message = "[Cluster] A background AwsSqs task faulted during dispose.")]
            public static partial void BackgroundTaskFaultedDuringDispose(ILogger logger, Exception exception);
        }
    }
}
