namespace ThunderPropagator.ClusterMessageBuses.SharedKernel
{
    /// <summary>
    /// Shared wire envelope answering a <see cref="ClusterRequestEnvelope"/>, consolidated from what
    /// used to be per-transport copies (<c>{Name}ClusterResponseEnvelope</c>) with identical shapes.
    /// </summary>
    /// <param name="CorrelationId">Matches the originating <see cref="ClusterRequestEnvelope.CorrelationId"/>.</param>
    /// <param name="Success">
    /// <see langword="false"/> if the answering side failed to resolve the channel or otherwise
    /// faulted -- the requester should throw rather than treat <see cref="PayloadJson"/> as valid.
    /// </param>
    /// <param name="ErrorMessage">Populated only when <see cref="Success"/> is <see langword="false"/>.</param>
    /// <param name="PayloadJson">
    /// NJson-serialized, request-kind-specific payload:
    /// <see cref="ThunderPropagator.Application.Channels.Snapshots.SnapshotEntry"/><c>[]</c> for
    /// <see cref="ClusterRequestKind.RestoreSnapshot"/>, a <see cref="ClusterSnapshotDeltaPayload"/>
    /// for <see cref="ClusterRequestKind.SyncDelta"/>,
    /// <see cref="ThunderPropagator.Application.Channels.Cluster.Subscriptions.ClusterSubscriptionDescriptor"/><c>[]</c>
    /// for <see cref="ClusterRequestKind.FetchSubscriptions"/>, or a raw <c>byte[]</c> (NJson-encoded)
    /// for <see cref="ClusterRequestKind.PullSnapshotBytes"/>. The requester already knows which kind
    /// it asked for, so no discriminator is needed on the response itself.
    /// </param>
    public sealed record ClusterResponseEnvelope(
        Guid CorrelationId,
        bool Success,
        string? ErrorMessage,
        string? PayloadJson);
}
