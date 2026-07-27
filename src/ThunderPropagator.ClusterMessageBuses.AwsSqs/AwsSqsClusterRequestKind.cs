namespace ThunderPropagator.ClusterMessageBuses.AwsSqs
{
    /// <summary>
    /// Which of <see cref="ThunderPropagator.Application.Channels.Cluster.MessageBus.IClusterMessageBus"/>'s
    /// three leader/peer-pull operations an <see cref="AwsSqsClusterRequestEnvelope"/> is asking for.
    /// </summary>
    internal enum AwsSqsClusterRequestKind
    {
        RestoreSnapshot,
        SyncDelta,
        FetchSubscriptions
    }
}
