namespace ThunderPropagator.ClusterMessageBuses.RabbitMQ
{
    /// <summary>
    /// Which of <see cref="ThunderPropagator.Application.Channels.Cluster.MessageBus.IClusterMessageBus"/>'s
    /// three leader/peer-pull operations a <see cref="RabbitMqClusterRequestEnvelope"/> is asking for.
    /// </summary>
    internal enum RabbitMqClusterRequestKind
    {
        RestoreSnapshot,
        SyncDelta,
        FetchSubscriptions
    }
}
