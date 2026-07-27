namespace ThunderPropagator.ClusterMessageBuses.AwsSqs
{
    /// <summary>
    /// Wire envelope sent to a target node's <see cref="AwsSqsResourceNaming.RequestQueue"/> for one
    /// of the three broker-native request/reply operations
    /// (<see cref="ThunderPropagator.Application.Channels.Cluster.MessageBus.IClusterMessageBus.RestoreFromLeaderAsync"/>,
    /// <c>SyncDeltaFromLeaderAsync</c>, <c>FetchPeerSubscriptionsAsync</c>).
    /// </summary>
    /// <param name="CorrelationId">
    /// Matches the eventual <see cref="AwsSqsClusterResponseEnvelope.CorrelationId"/> so the
    /// requester's reply-queue poller can complete the right pending call.
    /// </param>
    /// <param name="Kind">Which operation is being requested.</param>
    /// <param name="ChannelName">
    /// Identifies the channel by name (not key) for <see cref="AwsSqsClusterRequestKind.RestoreSnapshot"/>
    /// and <see cref="AwsSqsClusterRequestKind.SyncDelta"/>. Unused for
    /// <see cref="AwsSqsClusterRequestKind.FetchSubscriptions"/>, which identifies the channel by
    /// <see cref="ChannelKey"/> instead.
    /// </param>
    /// <param name="ChannelKey">Identifies the channel by key for <see cref="AwsSqsClusterRequestKind.FetchSubscriptions"/>.</param>
    /// <param name="SinceTicks">
    /// <see cref="DateTimeOffset.UtcTicks"/> cutoff for <see cref="AwsSqsClusterRequestKind.SyncDelta"/>;
    /// unused otherwise.
    /// </param>
    /// <param name="ReplyToNodeEndpoint">
    /// The requester's own <see cref="ThunderPropagator.Application.Channels.Cluster.ClusterConfiguration.NodeEndpoint"/>.
    /// The answering side derives the reply queue from this itself
    /// (<see cref="AwsSqsResourceNaming.ReplyQueue"/>) rather than trusting a queue URL supplied on
    /// the wire, so a request can never make this node send to an arbitrary attacker-chosen queue.
    /// </param>
    internal sealed record AwsSqsClusterRequestEnvelope(
        Guid CorrelationId,
        AwsSqsClusterRequestKind Kind,
        string? ChannelName,
        Guid? ChannelKey,
        long? SinceTicks,
        Uri ReplyToNodeEndpoint);
}
