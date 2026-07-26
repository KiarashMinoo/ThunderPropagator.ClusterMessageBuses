namespace ThunderPropagator.ClusterMessageBuses.Kafka
{
    /// <summary>
    /// Which of <see cref="ThunderPropagator.Application.Channels.Cluster.MessageBus.IClusterMessageBus"/>'s
    /// three leader/peer-pull operations a <see cref="KafkaClusterRequestEnvelope"/> is asking for.
    /// </summary>
    internal enum KafkaClusterRequestKind
    {
        RestoreSnapshot,
        SyncDelta,
        FetchSubscriptions
    }
}
