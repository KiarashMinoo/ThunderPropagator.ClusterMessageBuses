namespace ThunderPropagator.ClusterMessageBuses.SharedKernel
{
    /// <summary>
    /// Shared wire envelope for one of <see cref="ClusterRequestKind"/>'s leader/peer-pull
    /// operations, consolidated from what used to be per-transport copies
    /// (<c>{Name}ClusterRequestEnvelope</c>) with identical shapes.
    /// </summary>
    /// <param name="CorrelationId">
    /// Matches the eventual <see cref="ClusterResponseEnvelope.CorrelationId"/> so the requester can
    /// complete the right pending call -- see <see cref="PendingRequestTracker{TResponse}"/>.
    /// </param>
    /// <param name="Kind">Which operation is being requested.</param>
    /// <param name="ChannelName">
    /// Identifies the channel by name (not key) for <see cref="ClusterRequestKind.RestoreSnapshot"/>
    /// and <see cref="ClusterRequestKind.SyncDelta"/>. Unused for
    /// <see cref="ClusterRequestKind.FetchSubscriptions"/>/<see cref="ClusterRequestKind.PullSnapshotBytes"/>,
    /// which identify the channel by <see cref="ChannelKey"/> instead.
    /// </param>
    /// <param name="ChannelKey">Identifies the channel by key for <see cref="ClusterRequestKind.FetchSubscriptions"/>/<see cref="ClusterRequestKind.PullSnapshotBytes"/>.</param>
    /// <param name="SinceTicks">
    /// <see cref="DateTimeOffset.UtcTicks"/> cutoff for <see cref="ClusterRequestKind.SyncDelta"/>;
    /// unused otherwise.
    /// </param>
    /// <param name="ReplyToNodeEndpoint">
    /// The requester's own node identity, for broker-based transports (Kafka, RabbitMQ, ...) whose
    /// answering side derives a reply topic/queue/subject from this rather than replying over a
    /// direct connection the request arrived on. <see langword="null"/> and unused for
    /// direct-connection/shared-socket transports (Tcp, WebSocket, Udp, ZeroMQ, Swim, Rdma, Srd),
    /// which reply over the same connection/socket the request came in on instead. Kept nullable and
    /// optional (rather than splitting into two envelope types) so this one shared record serves
    /// both transport families -- additive, not a forced shape every transport must populate.
    /// </param>
    public sealed record ClusterRequestEnvelope(
        Guid CorrelationId,
        ClusterRequestKind Kind,
        string? ChannelName,
        Guid? ChannelKey,
        long? SinceTicks,
        Uri? ReplyToNodeEndpoint = null);
}
