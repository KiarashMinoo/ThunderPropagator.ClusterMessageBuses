namespace ThunderPropagator.ClusterMessageBuses.Mqtt
{
    /// <summary>
    /// Configuration for <see cref="MqttClusterMessageBus"/>.
    /// </summary>
    public sealed class MqttClusterMessageBusOptions
    {
        /// <summary>MQTT broker host. Default: <c>"localhost"</c>.</summary>
        public string Host { get; set; } = "localhost";

        /// <summary>MQTT broker TCP port. Default: <c>1883</c>.</summary>
        public int Port { get; set; } = 1883;

        /// <summary>
        /// Client identifier this node connects with. Default: <see langword="null"/>, which makes
        /// <see cref="MqttClusterTransport"/> generate a fresh random client id per process
        /// (mirroring the pattern used by <c>ThunderPropagator.Feeviders</c>'s own MQTT feeder/provider,
        /// which likewise defaults to a generated id when none is configured).
        /// </summary>
        public string? ClientId { get; set; }

        /// <summary>
        /// Prefix applied to every topic this transport publishes/subscribes to, so multiple
        /// ThunderPropagator clusters (or environments) can share one MQTT broker without colliding.
        /// Uses <c>/</c> as the segment separator, matching normal MQTT topic conventions (unlike the
        /// <c>.</c>/<c>-</c> separators used by this repo's other transports' topic/subject naming).
        /// Default: <c>"thunderpropagator/cluster"</c>.
        /// </summary>
        public string TopicPrefix { get; set; } = "thunderpropagator/cluster";

        /// <summary>
        /// How long <see cref="MqttClusterMessageBus"/> waits for a request/reply round trip
        /// (<c>RestoreFromLeaderAsync</c>, <c>SyncDeltaFromLeaderAsync</c>,
        /// <c>FetchPeerSubscriptionsAsync</c>) to complete before giving up. Default: 30 seconds.
        /// </summary>
        public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(30);
    }
}
