namespace ThunderPropagator.ClusterMessageBuses.UdpClient
{
    /// <summary>
    /// Which of <see cref="ThunderPropagator.Application.Channels.Cluster.MessageBus.IClusterMessageBus"/>'s
    /// three leader/peer-pull operations a <see cref="UdpClusterRequestEnvelope"/> is asking for.
    /// </summary>
    internal enum UdpClusterRequestKind
    {
        RestoreSnapshot,
        SyncDelta,
        FetchSubscriptions
    }
}
