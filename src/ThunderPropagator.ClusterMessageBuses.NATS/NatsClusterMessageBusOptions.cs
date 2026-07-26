namespace ThunderPropagator.ClusterMessageBuses.NATS
{
    /// <summary>
    /// Configuration for <see cref="NatsClusterMessageBus"/>.
    /// </summary>
    public sealed class NatsClusterMessageBusOptions
    {
        /// <summary>
        /// NATS server URL, e.g. <c>"nats://localhost:4222"</c>. Supports comma-separated seed
        /// servers per <c>NATS.Net</c>'s own connection option conventions. Required.
        /// </summary>
        public string Url { get; set; } = "nats://localhost:4222";

        /// <summary>
        /// Prefix applied to every subject this transport publishes/subscribes to, so multiple
        /// ThunderPropagator clusters (or environments) can share one NATS server without colliding.
        /// Default: <c>"thunderpropagator.cluster"</c>.
        /// </summary>
        public string SubjectPrefix { get; set; } = "thunderpropagator.cluster";

        /// <summary>
        /// How long <see cref="NatsClusterMessageBus"/> waits for a request/reply round trip
        /// (<c>RestoreFromLeaderAsync</c>, <c>SyncDeltaFromLeaderAsync</c>,
        /// <c>FetchPeerSubscriptionsAsync</c>) to complete before giving up. Default: 30 seconds.
        /// </summary>
        public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(30);
    }
}
