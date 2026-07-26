namespace ThunderPropagator.ClusterMessageBuses.ActiveMQ
{
    /// <summary>
    /// Which of <see cref="ThunderPropagator.Application.Channels.Cluster.MessageBus.IClusterMessageBus"/>'s
    /// three leader/peer-pull operations an <see cref="ActiveMqClusterRequestEnvelope"/> is asking for.
    /// </summary>
    internal enum ActiveMqClusterRequestKind
    {
        RestoreSnapshot,
        SyncDelta,
        FetchSubscriptions
    }
}
