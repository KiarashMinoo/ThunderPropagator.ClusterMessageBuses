namespace ThunderPropagator.ClusterMessageBuses.Kafka
{
    /// <summary>
    /// Wire envelope published back to the requester's <see cref="KafkaTopicNaming.ReplyTopic"/> in
    /// answer to a <see cref="KafkaClusterRequestEnvelope"/>.
    /// </summary>
    /// <param name="CorrelationId">Matches the originating <see cref="KafkaClusterRequestEnvelope.CorrelationId"/>.</param>
    /// <param name="Success">
    /// <see langword="false"/> if the answering side failed to resolve the channel or otherwise
    /// faulted — the requester should throw rather than treat <see cref="PayloadJson"/> as valid.
    /// </param>
    /// <param name="ErrorMessage">Populated only when <see cref="Success"/> is <see langword="false"/>.</param>
    /// <param name="PayloadJson">
    /// NJson-serialized, request-kind-specific payload: <see cref="ThunderPropagator.Application.Channels.Snapshots.SnapshotEntry"/><c>[]</c>
    /// for <see cref="KafkaClusterRequestKind.RestoreSnapshot"/>, a <see cref="KafkaSnapshotDeltaPayload"/>
    /// for <see cref="KafkaClusterRequestKind.SyncDelta"/>, or a
    /// <see cref="ThunderPropagator.Application.Channels.Cluster.Subscriptions.ClusterSubscriptionDescriptor"/><c>[]</c>
    /// for <see cref="KafkaClusterRequestKind.FetchSubscriptions"/>. The requester already knows
    /// which kind it asked for, so no discriminator is needed on the response itself.
    /// </param>
    internal sealed record KafkaClusterResponseEnvelope(
        Guid CorrelationId,
        bool Success,
        string? ErrorMessage,
        string? PayloadJson);
}
