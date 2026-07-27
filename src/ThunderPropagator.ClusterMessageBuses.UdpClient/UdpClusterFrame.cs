namespace ThunderPropagator.ClusterMessageBuses.UdpClient
{
    /// <summary>
    /// The single wire envelope sent as one whole UDP datagram. Unlike the TCP transport (which
    /// needs a hand-rolled length prefix because a TCP byte stream has no built-in message
    /// boundaries), a single UDP datagram already <i>is</i> exactly one message — the OS delivers it
    /// to <c>ReceiveAsync</c> whole or not at all, so no framing beyond this envelope is needed.
    /// <see cref="PayloadJson"/>'s shape depends on <see cref="Kind"/>: <see cref="UdpFanOutPayload"/>
    /// for <see cref="UdpClusterFrameKind.FanOut"/>, <see cref="UdpSubscriptionEventPayload"/> for
    /// <see cref="UdpClusterFrameKind.SubscriptionEvent"/>, <see cref="UdpClusterRequestEnvelope"/>
    /// for <see cref="UdpClusterFrameKind.Request"/>, <see cref="UdpClusterResponseEnvelope"/> for
    /// <see cref="UdpClusterFrameKind.Response"/>.
    /// </summary>
    internal sealed record UdpClusterFrame(UdpClusterFrameKind Kind, string PayloadJson);
}
