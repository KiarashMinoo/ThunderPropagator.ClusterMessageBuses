namespace ThunderPropagator.ClusterMessageBuses.ZeroMQ
{
    /// <summary>Identifies which of the three leader/peer-pull operations a <see cref="ZeroMqClusterRequestEnvelope"/> is for.</summary>
    internal enum ZeroMqClusterRequestKind
    {
        RestoreSnapshot,
        SyncDelta,
        FetchSubscriptions,
    }
}
