namespace ThunderPropagator.ClusterMessageBuses.GcpPubSub
{
    /// <summary>
    /// Which of <see cref="ThunderPropagator.Application.Channels.Cluster.MessageBus.IClusterMessageBus"/>'s
    /// three leader/peer-pull operations a <see cref="GcpPubSubClusterRequestEnvelope"/> is asking for.
    /// </summary>
    internal enum GcpPubSubClusterRequestKind
    {
        RestoreSnapshot,
        SyncDelta,
        FetchSubscriptions
    }
}
