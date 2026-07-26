namespace ThunderPropagator.ClusterMessageBuses.Kafka
{
    /// <summary>
    /// Configuration for <see cref="KafkaClusterMessageBus"/>.
    /// </summary>
    public sealed class KafkaClusterMessageBusOptions
    {
        /// <summary>
        /// Comma-separated list of Kafka bootstrap servers (e.g. <c>"broker1:9092,broker2:9092"</c>).
        /// Required.
        /// </summary>
        public string BootstrapServers { get; set; } = string.Empty;

        /// <summary>
        /// Prefix applied to every topic this transport creates or consumes, so multiple
        /// ThunderPropagator clusters (or environments) can share one Kafka cluster without
        /// colliding. Default: <c>"thunderpropagator.cluster"</c>.
        /// </summary>
        public string TopicPrefix { get; set; } = "thunderpropagator.cluster";

        /// <summary>
        /// Prefix used when generating this node's ephemeral consumer group ids (fan-out and
        /// subscription-event topics each get their own unique-per-process group so every node
        /// receives its own full copy of the broadcast — see the remarks on
        /// <see cref="KafkaClusterMessageBus"/> for why a shared/competing-consumer group would be
        /// wrong here). Default: <c>"thunderpropagator-cluster"</c>.
        /// </summary>
        public string ConsumerGroupPrefix { get; set; } = "thunderpropagator-cluster";

        /// <summary>
        /// How long <see cref="KafkaClusterMessageBus"/> waits for a broker-native request/reply
        /// round trip (<c>RestoreFromLeaderAsync</c>, <c>SyncDeltaFromLeaderAsync</c>,
        /// <c>FetchPeerSubscriptionsAsync</c>) to complete before giving up. Default: 30 seconds.
        /// </summary>
        public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(30);

        /// <summary>
        /// Optional hook to fine-tune the <see cref="Confluent.Kafka.ProducerConfig"/> built from
        /// <see cref="BootstrapServers"/> before the producer is created (e.g. compression, acks,
        /// SASL/TLS security settings).
        /// </summary>
        public Action<Confluent.Kafka.ProducerConfig>? ConfigureProducer { get; set; }

        /// <summary>
        /// Optional hook to fine-tune every <see cref="Confluent.Kafka.ConsumerConfig"/> this
        /// transport builds before each consumer is created (e.g. SASL/TLS security settings).
        /// <see cref="Confluent.Kafka.ConsumerConfig.GroupId"/> and
        /// <see cref="Confluent.Kafka.ConsumerConfig.AutoOffsetReset"/> are set by the transport
        /// itself afterward and should not be overridden here.
        /// </summary>
        public Action<Confluent.Kafka.ConsumerConfig>? ConfigureConsumer { get; set; }
    }
}
