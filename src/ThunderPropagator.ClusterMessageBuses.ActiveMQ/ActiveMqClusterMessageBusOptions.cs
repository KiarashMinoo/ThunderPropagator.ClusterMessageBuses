using Apache.NMS.ActiveMQ;

namespace ThunderPropagator.ClusterMessageBuses.ActiveMQ
{
    /// <summary>
    /// Configuration for <see cref="ActiveMqClusterMessageBus"/>.
    /// </summary>
    public sealed class ActiveMqClusterMessageBusOptions
    {
        /// <summary>
        /// ActiveMQ broker connection URI, e.g. <c>"activemq:tcp://localhost:61616"</c>. Required.
        /// </summary>
        public string BrokerUri { get; set; } = "activemq:tcp://localhost:61616";

        /// <summary>
        /// Prefix applied to every topic/queue this transport declares, so multiple
        /// ThunderPropagator clusters (or environments) can share one ActiveMQ broker without
        /// colliding. Default: <c>"thunderpropagator.cluster"</c>.
        /// </summary>
        public string TopicPrefix { get; set; } = "thunderpropagator.cluster";

        /// <summary>
        /// How long <see cref="ActiveMqClusterMessageBus"/> waits for a request/reply round trip
        /// (<c>RestoreFromLeaderAsync</c>, <c>SyncDeltaFromLeaderAsync</c>,
        /// <c>FetchPeerSubscriptionsAsync</c>) to complete before giving up. Default: 30 seconds.
        /// </summary>
        public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(30);

        /// <summary>
        /// Optional hook to fine-tune the <see cref="ConnectionFactory"/> built from
        /// <see cref="BrokerUri"/> before the connection is opened (e.g. client id, redelivery
        /// policy, prefetch size).
        /// </summary>
        public Action<ConnectionFactory>? ConfigureConnectionFactory { get; set; }
    }
}
