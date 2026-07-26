namespace ThunderPropagator.ClusterMessageBuses.Pulsar
{
    /// <summary>
    /// Wire envelope published to a target node's <see cref="PulsarTopicNaming.RequestTopic"/> for
    /// one of the three request/reply operations
    /// (<see cref="ThunderPropagator.Application.Channels.Cluster.MessageBus.IClusterMessageBus.RestoreFromLeaderAsync"/>,
    /// <c>SyncDeltaFromLeaderAsync</c>, <c>FetchPeerSubscriptionsAsync</c>).
    /// </summary>
    /// <param name="CorrelationId">
    /// Matches the eventual <see cref="PulsarClusterResponseEnvelope.CorrelationId"/> so the
    /// requester's reply-topic listener can complete the right pending call. Unlike NATS (which has
    /// native per-call reply inboxes), Pulsar has no built-in request/reply, so this transport tracks
    /// correlation by hand — the same approach used by <c>KafkaClusterMessageBus</c>.
    /// </param>
    /// <param name="Kind">Which operation is being requested.</param>
    /// <param name="ChannelName">
    /// Identifies the channel by name (not key) for <see cref="PulsarClusterRequestKind.RestoreSnapshot"/>
    /// and <see cref="PulsarClusterRequestKind.SyncDelta"/>. Unused for
    /// <see cref="PulsarClusterRequestKind.FetchSubscriptions"/>, which identifies the channel by
    /// <see cref="ChannelKey"/> instead.
    /// </param>
    /// <param name="ChannelKey">Identifies the channel by key for <see cref="PulsarClusterRequestKind.FetchSubscriptions"/>.</param>
    /// <param name="SinceTicks">
    /// <see cref="DateTimeOffset.UtcTicks"/> cutoff for <see cref="PulsarClusterRequestKind.SyncDelta"/>;
    /// unused otherwise.
    /// </param>
    /// <param name="ReplyToNodeEndpoint">
    /// The requester's own <see cref="ThunderPropagator.Application.Channels.Cluster.ClusterConfiguration.NodeEndpoint"/>.
    /// The answering side derives the reply topic from this itself
    /// (<see cref="PulsarTopicNaming.ReplyTopic"/>) rather than trusting a topic name supplied on the
    /// wire, so a request can never make this node publish to an arbitrary attacker-chosen topic.
    /// </param>
    internal sealed record PulsarClusterRequestEnvelope(
        Guid CorrelationId,
        PulsarClusterRequestKind Kind,
        string? ChannelName,
        Guid? ChannelKey,
        long? SinceTicks,
        Uri ReplyToNodeEndpoint);
}
