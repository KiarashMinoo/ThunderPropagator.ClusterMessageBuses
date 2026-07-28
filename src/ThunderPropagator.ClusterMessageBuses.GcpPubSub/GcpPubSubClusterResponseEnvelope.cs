namespace ThunderPropagator.ClusterMessageBuses.GcpPubSub
{
    /// <summary>
    /// Wire envelope published back to the requester's <see cref="GcpPubSubResourceNaming.ReplyTopicId"/>.
    /// </summary>
    /// <param name="CorrelationId">Matches the originating <see cref="GcpPubSubClusterRequestEnvelope.CorrelationId"/>.</param>
    /// <param name="Success">
    /// <see langword="false"/> if the answering side failed to resolve the channel or otherwise
    /// faulted — the requester should throw rather than treat <see cref="PayloadJson"/> as valid.
    /// </param>
    /// <param name="ErrorMessage">Populated only when <see cref="Success"/> is <see langword="false"/>.</param>
    /// <param name="PayloadJson">
    /// NJson-serialized, request-kind-specific payload: <see cref="ThunderPropagator.Application.Channels.Snapshots.SnapshotEntry"/><c>[]</c>
    /// for <see cref="GcpPubSubClusterRequestKind.RestoreSnapshot"/>, a <see cref="GcpPubSubSnapshotDeltaPayload"/>
    /// for <see cref="GcpPubSubClusterRequestKind.SyncDelta"/>, or a
    /// <see cref="ThunderPropagator.Application.Channels.Cluster.Subscriptions.ClusterSubscriptionDescriptor"/><c>[]</c>
    /// for <see cref="GcpPubSubClusterRequestKind.FetchSubscriptions"/>.
    /// </param>
    internal sealed record GcpPubSubClusterResponseEnvelope(
        Guid CorrelationId,
        bool Success,
        string? ErrorMessage,
        string? PayloadJson);
}
