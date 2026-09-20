namespace ThunderPropagator.ClusterMessageBuses.Srd
{
    /// <summary>
    /// Configuration for <see cref="SrdClusterMessageBus"/> and its
    /// <see cref="LibFabricSrdClusterEndpoint"/>.
    /// </summary>
    /// <remarks>
    /// NONE of these defaults have been exercised against real EFA hardware — see the warning banner
    /// atop <c>Native/LibFabricNativeMethods.cs</c> and this project's DI extension
    /// (<see cref="SrdClusterMessageBusExtensions"/>) for the full set of caveats.
    /// </remarks>
    public sealed class SrdClusterMessageBusOptions
    {
        /// <summary>
        /// Largest single SRD message, in bytes, this transport will send or accept — a safety cap
        /// enforced before <c>fi_send</c> is ever called. Default: 8192 bytes. EFA/SRD is a
        /// datagram-oriented transport (like UDP, unlike RDMA RC's byte-stream queue pairs), so
        /// messages are expected to stay comfortably smaller than RDMA RC's typical sizes; 8 KiB is a
        /// conservative starting point comfortably under EFA's documented maximum SRD payload —
        /// re-check against the actual EFA device/provider limits reported by <c>fi_getinfo</c> on the
        /// target instance (via <c>fi_info-&gt;ep_attr-&gt;max_msg_size</c>) before relying on this
        /// default in production.
        /// </summary>
        public int MaxMessageSize { get; set; } = 8192;

        /// <summary>
        /// How many <c>fi_recv</c> buffers this endpoint keeps posted concurrently. Unlike a socket,
        /// libfabric does not buffer an "unexpected" message that arrives with no matching posted
        /// receive — a message that arrives while every posted buffer is already consumed but not yet
        /// reposted is dropped by the provider. Default: 64. Raise this if the cluster is large or
        /// bursty enough that 64 concurrently in-flight receives is not enough headroom.
        /// </summary>
        public int PostedReceiveBufferCount { get; set; } = 64;

        /// <summary>
        /// Total time <see cref="SrdClusterMessageBus"/> waits for a request/reply round trip
        /// (<c>RestoreFromLeaderAsync</c>, <c>SyncDeltaFromLeaderAsync</c>,
        /// <c>FetchPeerSubscriptionsAsync</c>, <c>PullSnapshotAsync</c>) to complete before giving up.
        /// Unlike <c>UdpClusterMessageBus</c>, this transport does not resend the request itself on a
        /// timer — SRD is a reliable transport, so only the initial send is wrapped in
        /// <see cref="ThunderPropagator.ClusterMessageBuses.SharedKernel.ClusterResiliencePipelineFactory"/>'s
        /// retry/circuit-breaker policy, the same as the connection-oriented transports. Default: 30
        /// seconds.
        /// </summary>
        public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(30);

        /// <summary>
        /// Timeout passed to the blocking native <c>fi_cq_sread</c> call on the dedicated completion
        /// thread — the thread loops, blocking for at most this long per call, checking for shutdown
        /// between iterations, since libfabric's completion-queue APIs do not integrate with .NET's
        /// async/cancellation primitives directly. Default: 500 milliseconds. Smaller values make
        /// shutdown/dispose more responsive at the cost of slightly more CPU spent re-entering the
        /// native call; this is polling cadence, not a message-delivery timeout.
        /// </summary>
        public TimeSpan CompletionPollTimeout { get; set; } = TimeSpan.FromMilliseconds(500);

        /// <summary>
        /// Explicit libfabric provider name requested via <c>fi_getinfo</c> hints
        /// (<c>fi_fabric_attr.prov_name</c>). Default: <c>"efa"</c> — AWS's Elastic Fabric Adapter
        /// provider. Exposed (rather than hard-coded) purely so a test or a non-EFA development
        /// machine could point this at a different RDM-capable provider (e.g. libfabric's software
        /// "sockets" or "verbs" providers) for structural smoke-testing; production cluster
        /// configuration should leave this at the default to actually exercise EFA/SRD.
        /// </summary>
        public string ProviderName { get; set; } = "efa";

        /// <summary>
        /// Port number folded into this transport's <em>placeholder</em> peer-addressing scheme (see
        /// <c>Endpoint/SrdAddressing.cs</c>) — NOT a real libfabric/EFA addressing concept. Default:
        /// 6320. See <c>SrdAddressing</c>'s remarks for why this exists and why it needs to be replaced
        /// before this transport can talk to a real EFA peer.
        /// </summary>
        public int Port { get; set; } = 6320;
    }
}
