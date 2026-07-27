using ThunderPropagator.Application.Channels.Cluster.Subscriptions;

namespace ThunderPropagator.ClusterMessageBuses.UdpClient
{
    /// <summary><see cref="UdpClusterFrame.PayloadJson"/> shape for <see cref="UdpClusterFrameKind.SubscriptionEvent"/>.</summary>
    internal sealed record UdpSubscriptionEventPayload(Guid ChannelKey, ClusterSubscriptionEvent Event);
}
