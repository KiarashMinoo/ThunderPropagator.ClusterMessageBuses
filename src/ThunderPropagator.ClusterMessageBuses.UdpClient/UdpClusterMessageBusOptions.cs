namespace ThunderPropagator.ClusterMessageBuses.UdpClient
{
    /// <summary>
    /// Configuration for <see cref="UdpClusterMessageBus"/>.
    /// </summary>
    public sealed class UdpClusterMessageBusOptions
    {
        /// <summary>
        /// UDP port this node's own socket binds to, and that every datagram sent to a peer targets
        /// (every node in the cluster is assumed to listen on the same port — only the host, taken
        /// from each peer's discovered endpoint, differs). Unlike the TCP/WebSocket transports there
        /// is only ever one bound socket per node — UDP is connectionless, so there is nothing to
        /// open "per peer". Default: 6300.
        /// </summary>
        public int Port { get; set; } = 6300;

        /// <summary>
        /// Largest single datagram, in bytes, this transport will send or accept — a safety cap
        /// against oversized payloads that would either be silently dropped by network equipment or
        /// fragmented at the IP layer (fragmented UDP datagrams are far more likely to be lost than
        /// unfragmented ones, since losing any one fragment loses the whole datagram). Default: 60000
        /// bytes, comfortably under the 65507-byte theoretical IPv4 UDP payload ceiling while still
        /// well above what any fan-out/subscription-event/request/response envelope in this transport
        /// is expected to serialize to.
        /// </summary>
        public int MaxDatagramSize { get; set; } = 60_000;

        /// <summary>
        /// Total time <see cref="UdpClusterMessageBus"/> waits for a request/reply round trip
        /// (<c>RestoreFromLeaderAsync</c>, <c>SyncDeltaFromLeaderAsync</c>,
        /// <c>FetchPeerSubscriptionsAsync</c>) to complete, across every resend, before giving up.
        /// Default: 30 seconds.
        /// </summary>
        public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(30);

        /// <summary>
        /// How long to wait for a response after (re)sending a request datagram before resending it
        /// again — plain UDP has no delivery guarantee, so unlike every other transport in this repo,
        /// the request itself (not just the wait) must be retried on a timer while the overall
        /// <see cref="RequestTimeout"/> has not yet elapsed. Default: 2 seconds.
        /// </summary>
        public TimeSpan ResendInterval { get; set; } = TimeSpan.FromSeconds(2);
    }
}
