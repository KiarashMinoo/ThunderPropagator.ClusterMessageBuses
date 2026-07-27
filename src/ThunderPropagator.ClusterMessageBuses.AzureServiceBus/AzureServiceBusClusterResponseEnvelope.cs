namespace ThunderPropagator.ClusterMessageBuses.AzureServiceBus
{
    /// <summary>
    /// Wire envelope sent back to the requester's <see cref="AzureServiceBusResourceNaming.ReplyQueue"/>.
    /// </summary>
    /// <param name="CorrelationId">Matches the originating <see cref="AzureServiceBusClusterRequestEnvelope.CorrelationId"/>.</param>
    /// <param name="Success">
    /// <see langword="false"/> if the answering side failed to resolve the channel or otherwise
    /// faulted — the requester should throw rather than treat <see cref="PayloadJson"/> as valid.
    /// </param>
    /// <param name="ErrorMessage">Populated only when <see cref="Success"/> is <see langword="false"/>.</param>
    /// <param name="PayloadJson">
    /// NJson-serialized, request-kind-specific payload: <see cref="ThunderPropagator.Application.Channels.Snapshots.SnapshotEntry"/><c>[]</c>
    /// for <see cref="AzureServiceBusClusterRequestKind.RestoreSnapshot"/>, an
    /// <see cref="AzureServiceBusSnapshotDeltaPayload"/> for <see cref="AzureServiceBusClusterRequestKind.SyncDelta"/>,
    /// or a <see cref="ThunderPropagator.Application.Channels.Cluster.Subscriptions.ClusterSubscriptionDescriptor"/><c>[]</c>
    /// for <see cref="AzureServiceBusClusterRequestKind.FetchSubscriptions"/>.
    /// </param>
    internal sealed record AzureServiceBusClusterResponseEnvelope(
        Guid CorrelationId,
        bool Success,
        string? ErrorMessage,
        string? PayloadJson);
}
