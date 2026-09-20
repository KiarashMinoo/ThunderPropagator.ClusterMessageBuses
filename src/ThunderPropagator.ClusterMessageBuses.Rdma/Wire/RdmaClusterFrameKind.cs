namespace ThunderPropagator.ClusterMessageBuses.Rdma
{
    /// <summary>
    /// Discriminates the single kind of message multiplexed over one RDMA reliable-connected queue
    /// pair between two nodes. Mirrors <c>TcpClusterFrameKind</c> exactly -- like a TCP connection
    /// (and unlike the broker transports in this repo), one RC queue pair is one bidirectional,
    /// reliable, ordered channel, so every concern -- fan-out, subscription events, byte fan-out,
    /// and the hand-rolled request/reply RPC -- shares it, distinguished by this discriminator.
    /// </summary>
    internal enum RdmaClusterFrameKind
    {
        FanOut,
        SubscriptionEvent,
        Request,
        Response,

        /// <summary>Byte-oriented counterpart to <see cref="FanOut"/> -- see <c>ClusterByteMessage</c>'s own doc comment.</summary>
        ByteFanOut
    }
}
