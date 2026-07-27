namespace ThunderPropagator.ClusterMessageBuses.TcpSocket
{
    /// <summary>
    /// Configuration for <see cref="TcpClusterMessageBus"/>.
    /// </summary>
    public sealed class TcpClusterMessageBusOptions
    {
        /// <summary>
        /// TCP port this node's own listener binds to, and that every outbound connection to a peer
        /// is opened against (every node in the cluster is assumed to listen on the same port —
        /// only the host, taken from each peer's discovered endpoint, differs). Default: 6100.
        /// </summary>
        public int Port { get; set; } = 6100;

        /// <summary>
        /// How long <see cref="TcpClusterMessageBus"/> waits for a request/reply round trip
        /// (<c>RestoreFromLeaderAsync</c>, <c>SyncDeltaFromLeaderAsync</c>,
        /// <c>FetchPeerSubscriptionsAsync</c>) to complete before giving up. Default: 30 seconds.
        /// </summary>
        public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(30);

        /// <summary>
        /// Largest single frame (length-prefix value), in bytes, this transport will read before
        /// treating the connection as corrupt and closing it — a safety cap against a peer sending a
        /// malformed or hostile length prefix. Default: 16 MiB.
        /// </summary>
        public int MaxFrameSize { get; set; } = 16 * 1024 * 1024;
    }
}
