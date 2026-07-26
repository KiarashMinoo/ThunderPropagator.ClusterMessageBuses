namespace ThunderPropagator.ClusterMessageBuses.RabbitMQ
{
    /// <summary>
    /// Wire envelope published to a target node's <see cref="RabbitMqTopicNaming.RequestQueue"/>
    /// (via the default exchange, routing key = queue name) for one of the three broker-native
    /// request/reply operations
    /// (<see cref="ThunderPropagator.Application.Channels.Cluster.MessageBus.IClusterMessageBus.RestoreFromLeaderAsync"/>,
    /// <c>SyncDeltaFromLeaderAsync</c>, <c>FetchPeerSubscriptionsAsync</c>).
    /// </summary>
    /// <param name="CorrelationId">
    /// Matches the eventual <see cref="RabbitMqClusterResponseEnvelope.CorrelationId"/> so the
    /// requester's reply-queue listener can complete the right pending call.
    /// </param>
    /// <param name="Kind">Which operation is being requested.</param>
    /// <param name="ChannelName">
    /// Identifies the channel by name (not key) for <see cref="RabbitMqClusterRequestKind.RestoreSnapshot"/>
    /// and <see cref="RabbitMqClusterRequestKind.SyncDelta"/>. Unused for
    /// <see cref="RabbitMqClusterRequestKind.FetchSubscriptions"/>, which identifies the channel by
    /// <see cref="ChannelKey"/> instead.
    /// </param>
    /// <param name="ChannelKey">Identifies the channel by key for <see cref="RabbitMqClusterRequestKind.FetchSubscriptions"/>.</param>
    /// <param name="SinceTicks">
    /// <see cref="DateTimeOffset.UtcTicks"/> cutoff for <see cref="RabbitMqClusterRequestKind.SyncDelta"/>;
    /// unused otherwise.
    /// </param>
    /// <param name="ReplyToNodeEndpoint">
    /// The requester's own <see cref="ThunderPropagator.Application.Channels.Cluster.ClusterConfiguration.NodeEndpoint"/>.
    /// The answering side derives the reply queue from this itself
    /// (<see cref="RabbitMqTopicNaming.ReplyQueue"/>) rather than trusting a queue name supplied on
    /// the wire, so a request can never make this node publish to an arbitrary attacker-chosen queue.
    /// </param>
    internal sealed record RabbitMqClusterRequestEnvelope(
        Guid CorrelationId,
        RabbitMqClusterRequestKind Kind,
        string? ChannelName,
        Guid? ChannelKey,
        long? SinceTicks,
        Uri ReplyToNodeEndpoint);
}
