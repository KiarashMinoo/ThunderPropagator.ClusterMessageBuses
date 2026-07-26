namespace ThunderPropagator.ClusterMessageBuses.NATS
{
    /// <summary>
    /// Which of <see cref="ThunderPropagator.Application.Channels.Cluster.MessageBus.IClusterMessageBus"/>'s
    /// three leader/peer-pull operations a <see cref="NatsClusterRequestEnvelope"/> is asking for.
    /// </summary>
    internal enum NatsClusterRequestKind
    {
        RestoreSnapshot,
        SyncDelta,
        FetchSubscriptions
    }
}
