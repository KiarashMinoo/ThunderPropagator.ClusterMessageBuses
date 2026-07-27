namespace ThunderPropagator.ClusterMessageBuses.TcpSocket
{
    /// <summary>
    /// Discriminates the single kind of message multiplexed over one physical TCP connection between
    /// two nodes. Like the WebSocket transport (and unlike the broker transports in this repo), a TCP
    /// connection is one bidirectional stream, so every concern — fan-out, subscription events, and
    /// the hand-rolled request/reply RPC — shares it, distinguished by this discriminator.
    /// </summary>
    internal enum TcpClusterFrameKind
    {
        FanOut,
        SubscriptionEvent,
        Request,
        Response
    }
}
