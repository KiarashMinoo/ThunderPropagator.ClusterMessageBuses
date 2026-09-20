namespace ThunderPropagator.ClusterMessageBuses.Swim
{
    /// <summary>
    /// The single wire envelope sent as one whole UDP datagram -- and also reused, unwrapped from
    /// its own datagram, as the shape of one item inside a Ping/PingReq/Ack payload's piggybacked
    /// item batch (see <see cref="SwimPingPayload"/>'s remarks for why the same envelope type serves
    /// both roles). <see cref="PayloadJson"/>'s shape depends on <see cref="Kind"/>:
    /// <see cref="SwimPingPayload"/> for <see cref="SwimMessageKind.Ping"/>,
    /// <see cref="SwimPingReqPayload"/> for <see cref="SwimMessageKind.PingReq"/>,
    /// <see cref="SwimAckPayload"/> for <see cref="SwimMessageKind.Ack"/>,
    /// <c>SharedKernel.ClusterChannelEnvelope&lt;ClusterFanOutMessage&gt;</c> for <see cref="SwimMessageKind.GossipFanOut"/>,
    /// <c>SharedKernel.ClusterChannelEnvelope&lt;ClusterSubscriptionEvent&gt;</c> for <see cref="SwimMessageKind.GossipSubscriptionEvent"/>,
    /// <c>SharedKernel.ClusterChannelEnvelope&lt;ClusterByteMessage&gt;</c> for <see cref="SwimMessageKind.GossipByteFanOut"/>,
    /// <c>SharedKernel.ClusterRequestEnvelope</c> for <see cref="SwimMessageKind.Request"/>,
    /// <c>SharedKernel.ClusterResponseEnvelope</c> for <see cref="SwimMessageKind.Response"/>.
    /// </summary>
    internal sealed record SwimDatagram(SwimMessageKind Kind, string PayloadJson);
}
