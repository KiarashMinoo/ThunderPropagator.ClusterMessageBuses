using Google.Cloud.PubSub.V1;

namespace ThunderPropagator.ClusterMessageBuses.GcpPubSub
{
    /// <summary>
    /// Configuration for <see cref="GcpPubSubClusterMessageBus"/>.
    /// </summary>
    public sealed class GcpPubSubClusterMessageBusOptions
    {
        /// <summary>
        /// The GCP project every topic/subscription this transport creates lives in. Required —
        /// <see cref="GcpPubSubClusterMessageBusExtensions.AddClusterGcpPubSubMessageBus"/> throws at
        /// construction time if left unset, the same way <c>AwsSqsClusterMessageBus</c> requires
        /// <see cref="ThunderPropagator.Application.Channels.Cluster.ClusterConfiguration.NodeEndpoint"/>.
        /// </summary>
        public string ProjectId { get; set; } = string.Empty;

        /// <summary>
        /// Prefix applied to every Pub/Sub topic and subscription this transport creates, so multiple
        /// ThunderPropagator clusters (or environments) can share one GCP project without colliding.
        /// Default: <c>"tp-cluster"</c>.
        /// </summary>
        public string ResourcePrefix { get; set; } = "tp-cluster";

        /// <summary>
        /// How long <see cref="GcpPubSubClusterMessageBus"/> waits for a broker-native request/reply
        /// round trip (<c>RestoreFromLeaderAsync</c>, <c>SyncDeltaFromLeaderAsync</c>,
        /// <c>FetchPeerSubscriptionsAsync</c>) to complete before giving up. Default: 30 seconds.
        /// </summary>
        public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(30);

        /// <summary><c>MaxMessages</c> passed to every <c>PullAsync</c> call. Default: 10.</summary>
        public int PullMaxMessages { get; set; } = 10;

        /// <summary>
        /// <c>AckDeadlineSeconds</c> applied to every subscription this transport creates — how long
        /// a pulled-but-not-yet-acknowledged message stays invisible to other pulls on the same
        /// subscription. Default: 30 seconds.
        /// </summary>
        public int AckDeadlineSeconds { get; set; } = 30;

        /// <summary>
        /// Defensive client-side delay before the next pull whenever a <c>PullAsync</c> call returns
        /// zero messages, so an idle subscription doesn't busy-loop. Default: 250 milliseconds.
        /// </summary>
        public TimeSpan EmptyPollDelay { get; set; } = TimeSpan.FromMilliseconds(250);

        /// <summary>
        /// Optional hook to fine-tune the <see cref="PublisherServiceApiClientBuilder"/> used to build
        /// the default publisher client (e.g. to point at the Pub/Sub emulator, or override
        /// credentials) — the default builder resolves Application Default Credentials and honors
        /// <c>PUBSUB_EMULATOR_HOST</c> automatically.
        /// </summary>
        public Action<PublisherServiceApiClientBuilder>? ConfigurePublisherClient { get; set; }

        /// <summary>Optional hook to fine-tune the <see cref="SubscriberServiceApiClientBuilder"/> used to build the default subscriber client. Same defaults/notes as <see cref="ConfigurePublisherClient"/>.</summary>
        public Action<SubscriberServiceApiClientBuilder>? ConfigureSubscriberClient { get; set; }
    }
}
