using Amazon.SimpleNotificationService;
using Amazon.SQS;

namespace ThunderPropagator.ClusterMessageBuses.AwsSqs
{
    /// <summary>
    /// Configuration for <see cref="AwsSqsClusterMessageBus"/>.
    /// </summary>
    public sealed class AwsSqsClusterMessageBusOptions
    {
        /// <summary>
        /// Prefix applied to every SNS topic and SQS queue this transport creates, so multiple
        /// ThunderPropagator clusters (or environments) can share one AWS account/region without
        /// colliding. Default: <c>"tp-cluster"</c>.
        /// </summary>
        public string ResourcePrefix { get; set; } = "tp-cluster";

        /// <summary>
        /// How long <see cref="AwsSqsClusterMessageBus"/> waits for a broker-native request/reply
        /// round trip (<c>RestoreFromLeaderAsync</c>, <c>SyncDeltaFromLeaderAsync</c>,
        /// <c>FetchPeerSubscriptionsAsync</c>) to complete before giving up. Default: 30 seconds.
        /// </summary>
        public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(30);

        /// <summary>
        /// <c>WaitTimeSeconds</c> passed to every <c>ReceiveMessageAsync</c> long-poll call. AWS caps
        /// this at 20. Default: 20.
        /// </summary>
        public int ReceiveWaitTimeSeconds { get; set; } = 20;

        /// <summary>
        /// <c>VisibilityTimeout</c>, in seconds, applied to every queue this transport creates —
        /// how long a received-but-not-yet-deleted message stays invisible to other receivers.
        /// Default: 30 seconds.
        /// </summary>
        public int VisibilityTimeoutSeconds { get; set; } = 30;

        /// <summary>
        /// Defensive client-side delay before the next poll whenever a <c>ReceiveMessageAsync</c>
        /// call returns zero messages. Real AWS SQS long-polling already blocks server-side for up
        /// to <see cref="ReceiveWaitTimeSeconds"/>, but this guards against a client (production
        /// misconfiguration, or a test substitute) that returns empty results instantly, which would
        /// otherwise busy-loop. Default: 250 milliseconds.
        /// </summary>
        public TimeSpan EmptyPollDelay { get; set; } = TimeSpan.FromMilliseconds(250);

        /// <summary>Optional hook to fine-tune the <see cref="AmazonSQSConfig"/> used to build the default SQS client.</summary>
        public Action<AmazonSQSConfig>? ConfigureSqsClient { get; set; }

        /// <summary>Optional hook to fine-tune the <see cref="AmazonSimpleNotificationServiceConfig"/> used to build the default SNS client.</summary>
        public Action<AmazonSimpleNotificationServiceConfig>? ConfigureSnsClient { get; set; }
    }
}
