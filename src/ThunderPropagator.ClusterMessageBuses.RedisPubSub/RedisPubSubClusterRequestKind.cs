namespace ThunderPropagator.ClusterMessageBuses.RedisPubSub
{
    /// <summary>
    /// Which of <see cref="ThunderPropagator.Application.Channels.Cluster.MessageBus.IClusterMessageBus"/>'s
    /// three leader/peer-pull operations a <see cref="RedisPubSubClusterRequestEnvelope"/> is asking for.
    /// </summary>
    internal enum RedisPubSubClusterRequestKind
    {
        RestoreSnapshot,
        SyncDelta,
        FetchSubscriptions
    }
}
