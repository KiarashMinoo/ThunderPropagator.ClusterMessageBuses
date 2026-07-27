using ThunderPropagator.Application.Channels.Cluster.Subscriptions;

namespace ThunderPropagator.ClusterMessageBuses.TcpSocket
{
    /// <summary><see cref="TcpClusterFrame.PayloadJson"/> shape for <see cref="TcpClusterFrameKind.SubscriptionEvent"/>.</summary>
    internal sealed record TcpSubscriptionEventPayload(Guid ChannelKey, ClusterSubscriptionEvent Event);
}
