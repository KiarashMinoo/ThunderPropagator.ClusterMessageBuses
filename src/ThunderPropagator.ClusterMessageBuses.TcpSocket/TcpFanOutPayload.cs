using ThunderPropagator.Application.Channels.Cluster.MessageBus;

namespace ThunderPropagator.ClusterMessageBuses.TcpSocket
{
    /// <summary><see cref="TcpClusterFrame.PayloadJson"/> shape for <see cref="TcpClusterFrameKind.FanOut"/>.</summary>
    internal sealed record TcpFanOutPayload(Guid ChannelKey, ClusterFanOutMessage Message);
}
