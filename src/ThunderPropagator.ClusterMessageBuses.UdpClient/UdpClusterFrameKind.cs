namespace ThunderPropagator.ClusterMessageBuses.UdpClient
{
    /// <summary>
    /// Discriminates what a <see cref="UdpClusterFrame"/> carries. Same four kinds as the
    /// WebSocket/TcpSocket transports, multiplexed here over datagrams sent/received on this node's
    /// single shared UDP socket rather than a per-peer connection.
    /// </summary>
    internal enum UdpClusterFrameKind
    {
        FanOut,
        SubscriptionEvent,
        Request,
        Response
    }
}
