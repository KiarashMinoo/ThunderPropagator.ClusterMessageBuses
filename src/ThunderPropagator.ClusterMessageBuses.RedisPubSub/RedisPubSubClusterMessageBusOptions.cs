using StackExchange.Redis;

namespace ThunderPropagator.ClusterMessageBuses.RedisPubSub
{
    /// <summary>
    /// Configuration for <see cref="RedisPubSubClusterMessageBus"/>.
    /// </summary>
    public sealed class RedisPubSubClusterMessageBusOptions
    {
        /// <summary>
        /// StackExchange.Redis connection string, e.g. <c>"localhost:6379"</c>. Required.
        /// </summary>
        public string ConnectionString { get; set; } = "localhost:6379";

        /// <summary>
        /// Prefix applied to every pub/sub channel this transport publishes/subscribes to, so
        /// multiple ThunderPropagator clusters (or environments) can share one Redis server without
        /// colliding. Default: <c>"thunderpropagator:cluster"</c>.
        /// </summary>
        public string ChannelPrefix { get; set; } = "thunderpropagator:cluster";

        /// <summary>
        /// How long <see cref="RedisPubSubClusterMessageBus"/> waits for a request/reply round trip
        /// (<c>RestoreFromLeaderAsync</c>, <c>SyncDeltaFromLeaderAsync</c>,
        /// <c>FetchPeerSubscriptionsAsync</c>) to complete before giving up. Default: 30 seconds.
        /// </summary>
        public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(30);

        /// <summary>
        /// Optional hook to fine-tune the <see cref="ConfigurationOptions"/> parsed from
        /// <see cref="ConnectionString"/> before the connection is opened (e.g. TLS, password,
        /// allow-admin, retry policy).
        /// </summary>
        public Action<ConfigurationOptions>? ConfigureOptions { get; set; }
    }
}
