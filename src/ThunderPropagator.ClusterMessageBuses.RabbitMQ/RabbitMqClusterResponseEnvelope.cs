namespace ThunderPropagator.ClusterMessageBuses.RabbitMQ
{
    /// <summary>
    /// Wire envelope published back to the requester's <see cref="RabbitMqTopicNaming.ReplyQueue"/>
    /// in answer to a <see cref="RabbitMqClusterRequestEnvelope"/>.
    /// </summary>
    /// <param name="CorrelationId">Matches the originating <see cref="RabbitMqClusterRequestEnvelope.CorrelationId"/>.</param>
    /// <param name="Success">
    /// <see langword="false"/> if the answering side failed to resolve the channel or otherwise
    /// faulted — the requester should throw rather than treat <see cref="PayloadJson"/> as valid.
    /// </param>
    /// <param name="ErrorMessage">Populated only when <see cref="Success"/> is <see langword="false"/>.</param>
    /// <param name="PayloadJson">
    /// NJson-serialized, request-kind-specific payload: <see cref="ThunderPropagator.Application.Channels.Snapshots.SnapshotEntry"/><c>[]</c>
    /// for <see cref="RabbitMqClusterRequestKind.RestoreSnapshot"/>, a <see cref="RabbitMqSnapshotDeltaPayload"/>
    /// for <see cref="RabbitMqClusterRequestKind.SyncDelta"/>, or a
    /// <see cref="ThunderPropagator.Application.Channels.Cluster.Subscriptions.ClusterSubscriptionDescriptor"/><c>[]</c>
    /// for <see cref="RabbitMqClusterRequestKind.FetchSubscriptions"/>.
    /// </param>
    internal sealed record RabbitMqClusterResponseEnvelope(
        Guid CorrelationId,
        bool Success,
        string? ErrorMessage,
        string? PayloadJson);
}
