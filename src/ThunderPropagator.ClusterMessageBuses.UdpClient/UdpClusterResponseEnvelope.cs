namespace ThunderPropagator.ClusterMessageBuses.UdpClient
{
    /// <summary>
    /// <see cref="UdpClusterFrame.PayloadJson"/> shape for <see cref="UdpClusterFrameKind.Response"/>.
    /// </summary>
    /// <param name="CorrelationId">Matches the originating <see cref="UdpClusterRequestEnvelope.CorrelationId"/>.</param>
    /// <param name="Success">
    /// <see langword="false"/> if the answering side failed to resolve the channel or otherwise
    /// faulted — the requester should throw rather than treat <see cref="PayloadJson"/> as valid.
    /// </param>
    /// <param name="ErrorMessage">Populated only when <see cref="Success"/> is <see langword="false"/>.</param>
    /// <param name="PayloadJson">
    /// NJson-serialized, request-kind-specific payload: <see cref="ThunderPropagator.Application.Channels.Snapshots.SnapshotEntry"/><c>[]</c>
    /// for <see cref="UdpClusterRequestKind.RestoreSnapshot"/>, a <see cref="UdpSnapshotDeltaPayload"/>
    /// for <see cref="UdpClusterRequestKind.SyncDelta"/>, or a
    /// <see cref="ThunderPropagator.Application.Channels.Cluster.Subscriptions.ClusterSubscriptionDescriptor"/><c>[]</c>
    /// for <see cref="UdpClusterRequestKind.FetchSubscriptions"/>.
    /// </param>
    internal sealed record UdpClusterResponseEnvelope(
        Guid CorrelationId,
        bool Success,
        string? ErrorMessage,
        string? PayloadJson);
}
