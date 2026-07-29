using ThunderPropagator.Application.Channels.Cluster.Subscriptions;

namespace ThunderPropagator.ClusterMessageBuses.ZeroMQ
{
    internal sealed record ZeroMqSubscriptionEventPayload(Guid ChannelKey, ClusterSubscriptionEvent Event);
}
