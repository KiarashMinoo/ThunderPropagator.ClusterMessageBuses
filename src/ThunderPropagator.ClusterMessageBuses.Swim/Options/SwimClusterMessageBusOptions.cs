namespace ThunderPropagator.ClusterMessageBuses.Swim
{
    /// <summary>
    /// Configuration for <see cref="SwimClusterMessageBus"/>.
    /// </summary>
    public sealed class SwimClusterMessageBusOptions
    {
        /// <summary>
        /// UDP port this node's own socket binds to, and that every datagram sent to a peer targets.
        /// One higher than <c>UdpClusterMessageBusOptions.Port</c>'s own default of 6300, so a
        /// deployment could in principle run both transports side by side without a port clash.
        /// Default: 6301.
        /// </summary>
        public int Port { get; set; } = 6301;

        /// <summary>
        /// Largest single datagram, in bytes, this transport will send or accept -- see
        /// <c>UdpClusterMessageBusOptions.MaxDatagramSize</c>'s own doc comment for the fragmentation
        /// rationale; the concern is identical here. Default: 60000 bytes.
        /// </summary>
        public int MaxDatagramSize { get; set; } = 60_000;

        /// <summary>
        /// How often the gossip-round background loop fires: reconciling membership against
        /// discovery and disseminating a batch of queued broadcast-queue items to
        /// <see cref="GossipFanout"/> random peers. The SWIM probe loop runs on the same interval,
        /// independently. Default: 200 milliseconds -- <c>memberlist</c>'s own default gossip
        /// interval.
        /// </summary>
        public TimeSpan GossipInterval { get; set; } = TimeSpan.FromMilliseconds(200);

        /// <summary>
        /// How many random peers each gossip round disseminates queued broadcast items to. Default: 3.
        /// </summary>
        public int GossipFanout { get; set; } = 3;

        /// <summary>
        /// How many other members are asked to indirectly probe a target on the prober's behalf
        /// (via <see cref="SwimMessageKind.PingReq"/>) after a direct <see cref="SwimMessageKind.Ping"/>
        /// times out. Default: 3.
        /// </summary>
        public int IndirectProbeRelayCount { get; set; } = 3;

        /// <summary>
        /// How long a direct <see cref="SwimMessageKind.Ping"/> (or the indirect round that follows
        /// it) waits for a matching <see cref="SwimMessageKind.Ack"/> before escalating -- a direct
        /// ping that times out triggers the indirect-probe fan-out; an indirect round that also times
        /// out marks the target <see cref="SwimMemberState.Suspect"/>. Default: 500 milliseconds.
        /// </summary>
        public TimeSpan ProbeTimeout { get; set; } = TimeSpan.FromSeconds(0.5);

        /// <summary>
        /// How long a member can stay <see cref="SwimMemberState.Suspect"/>, unrefuted, before this
        /// node declares it <see cref="SwimMemberState.Dead"/>. Should comfortably exceed
        /// <see cref="ProbeTimeout"/> plus one indirect-probe round trip, so a member that is
        /// genuinely alive but only briefly unreachable has a real chance to refute the suspicion
        /// first. Default: 3 seconds.
        /// </summary>
        public TimeSpan SuspicionTimeout { get; set; } = TimeSpan.FromSeconds(3);

        /// <summary>
        /// Multiplier in the <c>ceil(RetransmitMult * log10(N+1))</c> retransmit-limit formula every
        /// broadcast queue in this transport uses (N = known member count) -- how many times a
        /// queued item or membership update is retransmitted before this node assumes it has reached
        /// the whole cluster and stops. Default: 4 -- <c>memberlist</c>'s own default.
        /// </summary>
        public int RetransmitMult { get; set; } = 4;

        /// <summary>
        /// Maximum number of not-yet-fully-disseminated items each of this transport's two broadcast
        /// queues (application payloads, membership updates) holds at once. Once full, the
        /// already-most-retransmitted item is evicted to make room for a newly queued one, bounding
        /// memory at the cost of that older item reaching the cluster somewhat less reliably.
        /// Default: 4096.
        /// </summary>
        public int MaxBroadcastQueueSize { get; set; } = 4096;

        /// <summary>
        /// Total time <see cref="SwimClusterMessageBus"/> waits for a request/reply round trip
        /// (<c>RestoreFromLeaderAsync</c>, <c>SyncDeltaFromLeaderAsync</c>,
        /// <c>FetchPeerSubscriptionsAsync</c>, <c>PullSnapshotAsync</c>) to complete, across every
        /// resend, before giving up -- these are direct unicast pulls against a specific known
        /// leader/peer, never gossiped. Default: 30 seconds, matching <c>UdpClusterMessageBusOptions</c>.
        /// </summary>
        public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(30);

        /// <summary>
        /// How long to wait for a response after (re)sending a request datagram before resending it
        /// again -- plain UDP has no delivery guarantee, so the request itself must be retried on a
        /// timer, exactly like <c>UdpClusterMessageBusOptions.ResendInterval</c>. Default: 2 seconds.
        /// </summary>
        public TimeSpan ResendInterval { get; set; } = TimeSpan.FromSeconds(2);
    }
}
