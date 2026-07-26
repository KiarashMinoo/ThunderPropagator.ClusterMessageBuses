namespace ThunderPropagator.ClusterMessageBuses.NATS
{
    /// <summary>
    /// Wire envelope sent back as the NATS reply to a <see cref="NatsClusterRequestEnvelope"/>.
    /// </summary>
    /// <param name="Success">
    /// <see langword="false"/> if the answering side failed to resolve the channel or otherwise
    /// faulted — the requester should throw rather than treat <see cref="PayloadJson"/> as valid.
    /// </param>
    /// <param name="ErrorMessage">Populated only when <see cref="Success"/> is <see langword="false"/>.</param>
    /// <param name="PayloadJson">
    /// NJson-serialized, request-kind-specific payload: <see cref="ThunderPropagator.Application.Channels.Snapshots.SnapshotEntry"/><c>[]</c>
    /// for <see cref="NatsClusterRequestKind.RestoreSnapshot"/>, a <see cref="NatsSnapshotDeltaPayload"/>
    /// for <see cref="NatsClusterRequestKind.SyncDelta"/>, or a
    /// <see cref="ThunderPropagator.Application.Channels.Cluster.Subscriptions.ClusterSubscriptionDescriptor"/><c>[]</c>
    /// for <see cref="NatsClusterRequestKind.FetchSubscriptions"/>.
    /// </param>
    internal sealed record NatsClusterResponseEnvelope(
        bool Success,
        string? ErrorMessage,
        string? PayloadJson);
}
