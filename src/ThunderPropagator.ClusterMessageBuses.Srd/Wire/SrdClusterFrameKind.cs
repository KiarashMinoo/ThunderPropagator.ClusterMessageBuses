namespace ThunderPropagator.ClusterMessageBuses.Srd
{
    /// <summary>
    /// Discriminates what a <see cref="SrdClusterFrame"/> carries. Same five kinds as
    /// <c>UdpClusterFrameKind</c>, multiplexed here over messages sent/received on this node's single
    /// shared libfabric RDM endpoint rather than a per-peer connection.
    /// </summary>
    internal enum SrdClusterFrameKind
    {
        FanOut,
        SubscriptionEvent,
        Request,
        Response,

        /// <summary>Byte-oriented counterpart to <see cref="FanOut"/> -- see <c>ClusterByteMessage</c>'s own doc comment.</summary>
        ByteFanOut
    }
}
