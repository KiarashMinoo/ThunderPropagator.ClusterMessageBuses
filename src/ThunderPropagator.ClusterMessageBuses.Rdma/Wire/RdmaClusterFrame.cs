namespace ThunderPropagator.ClusterMessageBuses.Rdma
{
    /// <summary>
    /// The single wire envelope sent over every RDMA reliable-connected queue pair this transport
    /// opens or accepts. Unlike <c>TcpClusterFrame</c>, this is never length-prefixed on the wire --
    /// two-sided RDMA SEND/RECV preserves message boundaries by itself (one <c>ibv_post_send</c>
    /// is received by exactly one <c>ibv_post_recv</c>, capped at that receive buffer's size), so
    /// the NJson UTF8 bytes of this record go straight into one registered send buffer, unframed.
    /// <see cref="PayloadJson"/>'s shape depends on <see cref="Kind"/>: a
    /// <see cref="ThunderPropagator.ClusterMessageBuses.SharedKernel.ClusterChannelEnvelope{TMessage}"/>
    /// (wrapping a <c>ClusterFanOutMessage</c>) for <see cref="RdmaClusterFrameKind.FanOut"/>, the
    /// same envelope (wrapping a <c>ClusterSubscriptionEvent</c>) for
    /// <see cref="RdmaClusterFrameKind.SubscriptionEvent"/>, a
    /// <see cref="ThunderPropagator.ClusterMessageBuses.SharedKernel.ClusterRequestEnvelope"/> for
    /// <see cref="RdmaClusterFrameKind.Request"/>, a
    /// <see cref="ThunderPropagator.ClusterMessageBuses.SharedKernel.ClusterResponseEnvelope"/> for
    /// <see cref="RdmaClusterFrameKind.Response"/>, the same channel envelope (wrapping a
    /// <c>ClusterByteMessage</c>) for <see cref="RdmaClusterFrameKind.ByteFanOut"/>.
    /// </summary>
    internal sealed record RdmaClusterFrame(RdmaClusterFrameKind Kind, string PayloadJson);
}
