namespace ThunderPropagator.ClusterMessageBuses.AwsSqs
{
    /// <summary>
    /// Wire envelope sent back to the requester's <see cref="AwsSqsResourceNaming.ReplyQueue"/>.
    /// </summary>
    /// <param name="CorrelationId">Matches the originating <see cref="AwsSqsClusterRequestEnvelope.CorrelationId"/>.</param>
    /// <param name="Success">
    /// <see langword="false"/> if the answering side failed to resolve the channel or otherwise
    /// faulted — the requester should throw rather than treat <see cref="PayloadJson"/> as valid.
    /// </param>
    /// <param name="ErrorMessage">Populated only when <see cref="Success"/> is <see langword="false"/>.</param>
    /// <param name="PayloadJson">
    /// NJson-serialized, request-kind-specific payload: <see cref="ThunderPropagator.Application.Channels.Snapshots.SnapshotEntry"/><c>[]</c>
    /// for <see cref="AwsSqsClusterRequestKind.RestoreSnapshot"/>, an <see cref="AwsSqsSnapshotDeltaPayload"/>
    /// for <see cref="AwsSqsClusterRequestKind.SyncDelta"/>, or a
    /// <see cref="ThunderPropagator.Application.Channels.Cluster.Subscriptions.ClusterSubscriptionDescriptor"/><c>[]</c>
    /// for <see cref="AwsSqsClusterRequestKind.FetchSubscriptions"/>.
    /// </param>
    internal sealed record AwsSqsClusterResponseEnvelope(
        Guid CorrelationId,
        bool Success,
        string? ErrorMessage,
        string? PayloadJson);
}
