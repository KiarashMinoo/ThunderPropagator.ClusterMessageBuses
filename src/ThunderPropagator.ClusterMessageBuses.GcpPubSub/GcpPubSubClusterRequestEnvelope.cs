namespace ThunderPropagator.ClusterMessageBuses.GcpPubSub
{
    /// <summary>
    /// Wire envelope published to a target node's <see cref="GcpPubSubResourceNaming.RequestTopicId"/>
    /// for one of the three broker-native request/reply operations
    /// (<see cref="ThunderPropagator.Application.Channels.Cluster.MessageBus.IClusterMessageBus.RestoreFromLeaderAsync"/>,
    /// <c>SyncDeltaFromLeaderAsync</c>, <c>FetchPeerSubscriptionsAsync</c>).
    /// </summary>
    /// <param name="CorrelationId">
    /// Matches the eventual <see cref="GcpPubSubClusterResponseEnvelope.CorrelationId"/> so the
    /// requester's reply-subscription poller can complete the right pending call.
    /// </param>
    /// <param name="Kind">Which operation is being requested.</param>
    /// <param name="ChannelName">
    /// Identifies the channel by name (not key) for <see cref="GcpPubSubClusterRequestKind.RestoreSnapshot"/>
    /// and <see cref="GcpPubSubClusterRequestKind.SyncDelta"/>. Unused for
    /// <see cref="GcpPubSubClusterRequestKind.FetchSubscriptions"/>, which identifies the channel by
    /// <see cref="ChannelKey"/> instead.
    /// </param>
    /// <param name="ChannelKey">Identifies the channel by key for <see cref="GcpPubSubClusterRequestKind.FetchSubscriptions"/>.</param>
    /// <param name="SinceTicks">
    /// <see cref="DateTimeOffset.UtcTicks"/> cutoff for <see cref="GcpPubSubClusterRequestKind.SyncDelta"/>;
    /// unused otherwise.
    /// </param>
    /// <param name="ReplyToNodeEndpoint">
    /// The requester's own <see cref="ThunderPropagator.Application.Channels.Cluster.ClusterConfiguration.NodeEndpoint"/>.
    /// The answering side derives the reply topic from this itself
    /// (<see cref="GcpPubSubResourceNaming.ReplyTopicId"/>) rather than trusting a destination
    /// supplied on the wire, so a request can never make this node publish to an arbitrary
    /// attacker-chosen topic.
    /// </param>
    internal sealed record GcpPubSubClusterRequestEnvelope(
        Guid CorrelationId,
        GcpPubSubClusterRequestKind Kind,
        string? ChannelName,
        Guid? ChannelKey,
        long? SinceTicks,
        Uri ReplyToNodeEndpoint);
}
