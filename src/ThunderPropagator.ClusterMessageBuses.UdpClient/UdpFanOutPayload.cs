using ThunderPropagator.Application.Channels.Cluster.MessageBus;

namespace ThunderPropagator.ClusterMessageBuses.UdpClient
{
    /// <summary><see cref="UdpClusterFrame.PayloadJson"/> shape for <see cref="UdpClusterFrameKind.FanOut"/>.</summary>
    internal sealed record UdpFanOutPayload(Guid ChannelKey, ClusterFanOutMessage Message);
}
