namespace ThunderPropagator.ClusterMessageBuses.RedisPubSub
{
    /// <summary>
    /// Wire envelope published back to the requester's <see cref="RedisChannelNaming.ReplyChannel"/>
    /// in answer to a <see cref="RedisPubSubClusterRequestEnvelope"/>.
    /// </summary>
    /// <param name="CorrelationId">Matches the originating <see cref="RedisPubSubClusterRequestEnvelope.CorrelationId"/>.</param>
    /// <param name="Success">
    /// <see langword="false"/> if the answering side failed to resolve the channel or otherwise
    /// faulted — the requester should throw rather than treat <see cref="PayloadJson"/> as valid.
    /// </param>
    /// <param name="ErrorMessage">Populated only when <see cref="Success"/> is <see langword="false"/>.</param>
    /// <param name="PayloadJson">
    /// NJson-serialized, request-kind-specific payload: <see cref="ThunderPropagator.Application.Channels.Snapshots.SnapshotEntry"/><c>[]</c>
    /// for <see cref="RedisPubSubClusterRequestKind.RestoreSnapshot"/>, a <see cref="RedisPubSubSnapshotDeltaPayload"/>
    /// for <see cref="RedisPubSubClusterRequestKind.SyncDelta"/>, or a
    /// <see cref="ThunderPropagator.Application.Channels.Cluster.Subscriptions.ClusterSubscriptionDescriptor"/><c>[]</c>
    /// for <see cref="RedisPubSubClusterRequestKind.FetchSubscriptions"/>.
    /// </param>
    internal sealed record RedisPubSubClusterResponseEnvelope(
        Guid CorrelationId,
        bool Success,
        string? ErrorMessage,
        string? PayloadJson);
}
