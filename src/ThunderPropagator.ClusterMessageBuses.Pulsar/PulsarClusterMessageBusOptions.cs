namespace ThunderPropagator.ClusterMessageBuses.Pulsar
{
    /// <summary>
    /// Configuration for <see cref="PulsarClusterMessageBus"/>.
    /// </summary>
    public sealed class PulsarClusterMessageBusOptions
    {
        /// <summary>
        /// Pulsar broker service URL, e.g. <c>"pulsar://localhost:6650"</c>. Required.
        /// </summary>
        public string ServiceUrl { get; set; } = "pulsar://localhost:6650";

        /// <summary>
        /// Fully-qualified Pulsar topic prefix (e.g. <c>"persistent://public/default/thunderpropagator-cluster"</c>)
        /// every topic this transport creates or consumes is built from, so multiple ThunderPropagator
        /// clusters (or environments/tenants/namespaces) can share one Pulsar cluster without colliding.
        /// </summary>
        public string TopicPrefix { get; set; } = "persistent://public/default/thunderpropagator-cluster";

        /// <summary>
        /// Prefix used when generating this node's unique-per-process subscription names. Every
        /// fan-out/subscription-event/request/reply topic this transport consumes uses its own
        /// subscription name distinct from every other node's — Pulsar delivers a full copy of a
        /// topic's messages to each distinct subscription name, mirroring
        /// <c>KafkaClusterMessageBus</c>'s one-consumer-group-per-node design (a shared subscription
        /// name would instead split messages across nodes, which is wrong for a fan-out bus).
        /// Default: <c>"thunderpropagator-cluster"</c>.
        /// </summary>
        public string SubscriptionPrefix { get; set; } = "thunderpropagator-cluster";

        /// <summary>
        /// How long <see cref="PulsarClusterMessageBus"/> waits for a request/reply round trip
        /// (<c>RestoreFromLeaderAsync</c>, <c>SyncDeltaFromLeaderAsync</c>,
        /// <c>FetchPeerSubscriptionsAsync</c>) to complete before giving up. Default: 30 seconds.
        /// </summary>
        public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(30);
    }
}
