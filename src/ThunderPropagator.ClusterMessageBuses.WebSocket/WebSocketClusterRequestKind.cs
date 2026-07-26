namespace ThunderPropagator.ClusterMessageBuses.WebSocket
{
    /// <summary>
    /// Which of <see cref="ThunderPropagator.Application.Channels.Cluster.MessageBus.IClusterMessageBus"/>'s
    /// three leader/peer-pull operations a <see cref="WebSocketClusterRequestEnvelope"/> is asking for.
    /// </summary>
    internal enum WebSocketClusterRequestKind
    {
        RestoreSnapshot,
        SyncDelta,
        FetchSubscriptions
    }
}
