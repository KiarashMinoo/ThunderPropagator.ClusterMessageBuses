namespace ThunderPropagator.ClusterMessageBuses.Mqtt
{
    /// <summary>
    /// Wire envelope published back to the requester's <see cref="MqttTopicNaming.ReplyTopic"/> in
    /// answer to a <see cref="MqttClusterRequestEnvelope"/>.
    /// </summary>
    /// <param name="CorrelationId">Matches the originating <see cref="MqttClusterRequestEnvelope.CorrelationId"/>.</param>
    /// <param name="Success">
    /// <see langword="false"/> if the answering side failed to resolve the channel or otherwise
    /// faulted — the requester should throw rather than treat <see cref="PayloadJson"/> as valid.
    /// </param>
    /// <param name="ErrorMessage">Populated only when <see cref="Success"/> is <see langword="false"/>.</param>
    /// <param name="PayloadJson">
    /// NJson-serialized, request-kind-specific payload: <see cref="ThunderPropagator.Application.Channels.Snapshots.SnapshotEntry"/><c>[]</c>
    /// for <see cref="MqttClusterRequestKind.RestoreSnapshot"/>, a <see cref="MqttSnapshotDeltaPayload"/>
    /// for <see cref="MqttClusterRequestKind.SyncDelta"/>, or a
    /// <see cref="ThunderPropagator.Application.Channels.Cluster.Subscriptions.ClusterSubscriptionDescriptor"/><c>[]</c>
    /// for <see cref="MqttClusterRequestKind.FetchSubscriptions"/>.
    /// </param>
    internal sealed record MqttClusterResponseEnvelope(
        Guid CorrelationId,
        bool Success,
        string? ErrorMessage,
        string? PayloadJson);
}
