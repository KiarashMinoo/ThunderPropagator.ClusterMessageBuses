namespace ThunderPropagator.ClusterMessageBuses.Pulsar
{
    /// <summary>
    /// Which of <see cref="ThunderPropagator.Application.Channels.Cluster.MessageBus.IClusterMessageBus"/>'s
    /// three leader/peer-pull operations a <see cref="PulsarClusterRequestEnvelope"/> is asking for.
    /// </summary>
    internal enum PulsarClusterRequestKind
    {
        RestoreSnapshot,
        SyncDelta,
        FetchSubscriptions
    }
}
