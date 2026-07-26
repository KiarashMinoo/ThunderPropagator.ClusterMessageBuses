namespace ThunderPropagator.ClusterMessageBuses.ActiveMQ
{
    /// <summary>
    /// Wire envelope published to a target node's <see cref="ActiveMqTopicNaming.RequestQueue"/> for
    /// one of the three request/reply operations
    /// (<see cref="ThunderPropagator.Application.Channels.Cluster.MessageBus.IClusterMessageBus.RestoreFromLeaderAsync"/>,
    /// <c>SyncDeltaFromLeaderAsync</c>, <c>FetchPeerSubscriptionsAsync</c>).
    /// </summary>
    /// <param name="CorrelationId">
    /// Matches the eventual <see cref="ActiveMqClusterResponseEnvelope.CorrelationId"/> so the
    /// requester's reply-queue listener can complete the right pending call. ActiveMQ/JMS has no
    /// built-in request/reply, so this transport tracks correlation by hand — the same approach used
    /// by <c>KafkaClusterMessageBus</c>/<c>RabbitMqClusterMessageBus</c>/<c>PulsarClusterMessageBus</c>/
    /// <c>MqttClusterMessageBus</c>.
    /// </param>
    /// <param name="Kind">Which operation is being requested.</param>
    /// <param name="ChannelName">
    /// Identifies the channel by name (not key) for <see cref="ActiveMqClusterRequestKind.RestoreSnapshot"/>
    /// and <see cref="ActiveMqClusterRequestKind.SyncDelta"/>. Unused for
    /// <see cref="ActiveMqClusterRequestKind.FetchSubscriptions"/>, which identifies the channel by
    /// <see cref="ChannelKey"/> instead.
    /// </param>
    /// <param name="ChannelKey">Identifies the channel by key for <see cref="ActiveMqClusterRequestKind.FetchSubscriptions"/>.</param>
    /// <param name="SinceTicks">
    /// <see cref="DateTimeOffset.UtcTicks"/> cutoff for <see cref="ActiveMqClusterRequestKind.SyncDelta"/>;
    /// unused otherwise.
    /// </param>
    /// <param name="ReplyToNodeEndpoint">
    /// The requester's own <see cref="ThunderPropagator.Application.Channels.Cluster.ClusterConfiguration.NodeEndpoint"/>.
    /// The answering side derives the reply queue from this itself
    /// (<see cref="ActiveMqTopicNaming.ReplyQueue"/>) rather than trusting a queue name supplied on
    /// the wire, so a request can never make this node publish to an arbitrary attacker-chosen queue.
    /// </param>
    internal sealed record ActiveMqClusterRequestEnvelope(
        Guid CorrelationId,
        ActiveMqClusterRequestKind Kind,
        string? ChannelName,
        Guid? ChannelKey,
        long? SinceTicks,
        Uri ReplyToNodeEndpoint);
}
