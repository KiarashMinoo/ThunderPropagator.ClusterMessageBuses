using ThunderPropagator.Application.Channels.Cluster.MessageBus;

namespace ThunderPropagator.ClusterMessageBuses.ZeroMQ
{
    internal sealed record ZeroMqFanOutPayload(Guid ChannelKey, ClusterFanOutMessage Message);
}
