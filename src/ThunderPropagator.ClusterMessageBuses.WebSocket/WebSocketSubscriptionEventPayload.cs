using ThunderPropagator.Application.Channels.Cluster.Subscriptions;

namespace ThunderPropagator.ClusterMessageBuses.WebSocket
{
    /// <summary><see cref="WebSocketClusterFrame.PayloadJson"/> shape for <see cref="WebSocketClusterFrameKind.SubscriptionEvent"/>.</summary>
    internal sealed record WebSocketSubscriptionEventPayload(Guid ChannelKey, ClusterSubscriptionEvent Event);
}
