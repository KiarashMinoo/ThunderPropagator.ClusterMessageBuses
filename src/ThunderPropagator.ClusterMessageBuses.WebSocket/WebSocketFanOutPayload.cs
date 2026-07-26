using ThunderPropagator.Application.Channels.Cluster.MessageBus;

namespace ThunderPropagator.ClusterMessageBuses.WebSocket
{
    /// <summary><see cref="WebSocketClusterFrame.PayloadJson"/> shape for <see cref="WebSocketClusterFrameKind.FanOut"/>.</summary>
    internal sealed record WebSocketFanOutPayload(Guid ChannelKey, ClusterFanOutMessage Message);
}
