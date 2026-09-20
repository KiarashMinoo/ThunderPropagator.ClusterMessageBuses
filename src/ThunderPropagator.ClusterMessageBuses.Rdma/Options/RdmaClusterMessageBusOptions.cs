namespace ThunderPropagator.ClusterMessageBuses.Rdma
{
    /// <summary>
    /// Configuration for <see cref="RdmaClusterMessageBus"/>.
    /// </summary>
    /// <remarks>
    /// NOT VALIDATED ON REAL HARDWARE. Every default here is a reasonable starting point copied
    /// from common <c>rdma-core</c> example programs (e.g. <c>ibv_rc_pingpong</c>, <c>rping</c>) --
    /// tune them against the target NIC/fabric (queue-pair depth limits, MTU, path MTU negotiated
    /// by the fabric, etc. all vary by hardware) before relying on them in production.
    /// </remarks>
    public sealed class RdmaClusterMessageBusOptions
    {
        /// <summary>
        /// Name of the RDMA-capable device (InfiniBand HCA or RoCEv2 NIC) to open, as reported by
        /// <c>ibv_get_device_list</c> (e.g. <c>"mlx5_0"</c>). <see langword="null"/> (the default)
        /// selects the first device in the list returned by the host's RDMA driver stack -- fine for
        /// a single-NIC host, but on a multi-NIC/multi-fabric host the "first" device is
        /// enumeration-order-dependent and not guaranteed stable across reboots or driver updates;
        /// set this explicitly in that case.
        /// </summary>
        public string? DeviceName { get; set; }

        /// <summary>
        /// TCP-style port number carried in the <c>sockaddr_in</c> passed to RDMA CM's
        /// <c>rdma_resolve_addr</c>/<c>rdma_bind_addr</c>. This is a librdmacm addressing construct,
        /// not a literal TCP/IP port that competes with this transport's own kernel networking stack
        /// (RDMA CM performs its own resolution over the IB/RoCE fabric address space) -- but by
        /// convention it is chosen from the same numeric space as TCP ports, and every node in the
        /// cluster must agree on it. Default: 18515, the conventional example port used by
        /// <c>ibv_rc_pingpong</c> and similar rdma-core sample programs.
        /// </summary>
        public int Port { get; set; } = 18515;

        /// <summary>
        /// Size, in bytes, of every registered send/receive buffer -- the largest single
        /// <see cref="RdmaClusterFrame"/> (NJson-serialized, UTF8-encoded) this transport can send or
        /// receive in one two-sided SEND/RECV work request. Unlike TCP's length-prefixed byte
        /// stream, RDMA two-sided send/recv preserves message boundaries but caps each message at
        /// the posted receive buffer's size -- a frame larger than this is a hard failure, not a
        /// resumable partial read. Default: 65536 (64 KiB).
        /// </summary>
        public int MaxMessageSize { get; set; } = 65536;

        /// <summary>
        /// Number of entries in each queue pair's completion queue, and (mirrored 1:1) the number of
        /// receive buffers pre-posted per connection via <c>ibv_post_recv</c> before any send can be
        /// received -- RDMA reliable-connected send/recv requires a receive work request to already
        /// be posted before the peer's matching send arrives, or the send fails with a receiver-not-
        /// ready error. Default: 64.
        /// </summary>
        public int CompletionQueueDepth { get; set; } = 64;

        /// <summary>
        /// How long <see cref="RdmaClusterMessageBus"/> waits for a request/reply round trip
        /// (<c>RestoreFromLeaderAsync</c>, <c>SyncDeltaFromLeaderAsync</c>,
        /// <c>FetchPeerSubscriptionsAsync</c>, <c>PullSnapshotAsync</c>) to complete before giving
        /// up. Default: 30 seconds -- matches the TCP transport's default.
        /// </summary>
        public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(30);

        /// <summary>
        /// How long to wait for RDMA CM's out-of-band handshake (<c>rdma_resolve_addr</c> ->
        /// <c>rdma_resolve_route</c> -> <c>rdma_connect</c> -> the <c>RDMA_CM_EVENT_ESTABLISHED</c>
        /// event) to complete before treating a peer as unreachable. Default: 10 seconds.
        /// </summary>
        public TimeSpan ConnectTimeout { get; set; } = TimeSpan.FromSeconds(10);
    }
}
