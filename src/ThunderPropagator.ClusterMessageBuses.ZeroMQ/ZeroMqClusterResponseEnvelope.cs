namespace ThunderPropagator.ClusterMessageBuses.ZeroMQ
{
    /// <summary>
    /// The answer to a <see cref="ZeroMqClusterRequestEnvelope"/>. <c>PayloadJson</c> is
    /// NJson-serialized, request-kind-specific payload: <see cref="ThunderPropagator.Application.Channels.Snapshots.SnapshotEntry"/><c>[]</c>
    /// for <see cref="ZeroMqClusterRequestKind.RestoreSnapshot"/>, <see cref="ZeroMqSnapshotDeltaPayload"/>
    /// for <see cref="ZeroMqClusterRequestKind.SyncDelta"/>, or
    /// <see cref="ThunderPropagator.Application.Channels.Cluster.Subscriptions.ClusterSubscriptionDescriptor"/><c>[]</c>
    /// for <see cref="ZeroMqClusterRequestKind.FetchSubscriptions"/>.
    /// </summary>
    internal sealed record ZeroMqClusterResponseEnvelope(Guid CorrelationId, bool Success, string? ErrorMessage, string? PayloadJson);
}
