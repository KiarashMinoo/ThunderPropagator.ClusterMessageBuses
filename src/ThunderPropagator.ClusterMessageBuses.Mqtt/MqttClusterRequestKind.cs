namespace ThunderPropagator.ClusterMessageBuses.Mqtt
{
    /// <summary>
    /// Which of <see cref="ThunderPropagator.Application.Channels.Cluster.MessageBus.IClusterMessageBus"/>'s
    /// three leader/peer-pull operations a <see cref="MqttClusterRequestEnvelope"/> is asking for.
    /// </summary>
    internal enum MqttClusterRequestKind
    {
        RestoreSnapshot,
        SyncDelta,
        FetchSubscriptions
    }
}
