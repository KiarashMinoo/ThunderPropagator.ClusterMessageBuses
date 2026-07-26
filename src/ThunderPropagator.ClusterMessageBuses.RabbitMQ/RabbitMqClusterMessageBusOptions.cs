using RabbitMQ.Client;

namespace ThunderPropagator.ClusterMessageBuses.RabbitMQ
{
    /// <summary>
    /// Configuration for <see cref="RabbitMqClusterMessageBus"/>.
    /// </summary>
    public sealed class RabbitMqClusterMessageBusOptions
    {
        /// <summary>
        /// AMQP 0-9-1 connection URI, e.g. <c>"amqp://user:pass@host:5672/vhost"</c>. Required.
        /// </summary>
        public string ConnectionString { get; set; } = "amqp://guest:guest@localhost:5672/";

        /// <summary>
        /// Prefix applied to every exchange/queue this transport declares, so multiple
        /// ThunderPropagator clusters (or environments) can share one RabbitMQ broker without
        /// colliding. Default: <c>"thunderpropagator.cluster"</c>.
        /// </summary>
        public string ExchangePrefix { get; set; } = "thunderpropagator.cluster";

        /// <summary>
        /// How long <see cref="RabbitMqClusterMessageBus"/> waits for a broker-native request/reply
        /// round trip (<c>RestoreFromLeaderAsync</c>, <c>SyncDeltaFromLeaderAsync</c>,
        /// <c>FetchPeerSubscriptionsAsync</c>) to complete before giving up. Default: 30 seconds.
        /// </summary>
        public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(30);

        /// <summary>
        /// Optional hook to fine-tune the <see cref="ConnectionFactory"/> built from
        /// <see cref="ConnectionString"/> before the connection is opened (e.g. TLS, automatic
        /// recovery, client-provided connection name).
        /// </summary>
        public Action<ConnectionFactory>? ConfigureConnectionFactory { get; set; }
    }
}
