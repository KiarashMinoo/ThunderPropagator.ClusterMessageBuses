namespace ThunderPropagator.ClusterMessageBuses.TcpSocket
{
    /// <summary>
    /// The single wire envelope sent over every TCP connection this transport opens or accepts,
    /// length-prefixed on the wire by <see cref="ITcpClusterConnection"/>'s framing.
    /// <see cref="PayloadJson"/>'s shape depends on <see cref="Kind"/>: <see cref="TcpFanOutPayload"/>
    /// for <see cref="TcpClusterFrameKind.FanOut"/>, <see cref="TcpSubscriptionEventPayload"/> for
    /// <see cref="TcpClusterFrameKind.SubscriptionEvent"/>, <see cref="TcpClusterRequestEnvelope"/>
    /// for <see cref="TcpClusterFrameKind.Request"/>, <see cref="TcpClusterResponseEnvelope"/> for
    /// <see cref="TcpClusterFrameKind.Response"/>.
    /// </summary>
    internal sealed record TcpClusterFrame(TcpClusterFrameKind Kind, string PayloadJson);
}
