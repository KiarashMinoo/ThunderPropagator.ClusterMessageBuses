namespace ThunderPropagator.ClusterMessageBuses.TcpSocket
{
    /// <summary>
    /// Which of <see cref="ThunderPropagator.Application.Channels.Cluster.MessageBus.IClusterMessageBus"/>'s
    /// three leader/peer-pull operations a <see cref="TcpClusterRequestEnvelope"/> is asking for.
    /// </summary>
    internal enum TcpClusterRequestKind
    {
        RestoreSnapshot,
        SyncDelta,
        FetchSubscriptions
    }
}
