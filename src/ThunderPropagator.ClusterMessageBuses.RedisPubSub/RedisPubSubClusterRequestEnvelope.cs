namespace ThunderPropagator.ClusterMessageBuses.RedisPubSub
{
    /// <summary>
    /// Wire envelope published to a target node's <see cref="RedisChannelNaming.RequestChannel"/> for
    /// one of the three request/reply operations
    /// (<see cref="ThunderPropagator.Application.Channels.Cluster.MessageBus.IClusterMessageBus.RestoreFromLeaderAsync"/>,
    /// <c>SyncDeltaFromLeaderAsync</c>, <c>FetchPeerSubscriptionsAsync</c>).
    /// </summary>
    /// <param name="CorrelationId">
    /// Matches the eventual <see cref="RedisPubSubClusterResponseEnvelope.CorrelationId"/> so the
    /// requester's reply-channel subscription can complete the right pending call. Redis pub/sub has
    /// no built-in request/reply (unlike NATS's ephemeral inbox subjects), so this transport tracks
    /// correlation by hand — the same approach used by
    /// <c>KafkaClusterMessageBus</c>/<c>RabbitMqClusterMessageBus</c>/<c>PulsarClusterMessageBus</c>/
    /// <c>MqttClusterMessageBus</c>/<c>ActiveMqClusterMessageBus</c>.
    /// </param>
    /// <param name="Kind">Which operation is being requested.</param>
    /// <param name="ChannelName">
    /// Identifies the channel by name (not key) for <see cref="RedisPubSubClusterRequestKind.RestoreSnapshot"/>
    /// and <see cref="RedisPubSubClusterRequestKind.SyncDelta"/>. Unused for
    /// <see cref="RedisPubSubClusterRequestKind.FetchSubscriptions"/>, which identifies the channel by
    /// <see cref="ChannelKey"/> instead.
    /// </param>
    /// <param name="ChannelKey">Identifies the channel by key for <see cref="RedisPubSubClusterRequestKind.FetchSubscriptions"/>.</param>
    /// <param name="SinceTicks">
    /// <see cref="DateTimeOffset.UtcTicks"/> cutoff for <see cref="RedisPubSubClusterRequestKind.SyncDelta"/>;
    /// unused otherwise.
    /// </param>
    /// <param name="ReplyToNodeEndpoint">
    /// The requester's own <see cref="ThunderPropagator.Application.Channels.Cluster.ClusterConfiguration.NodeEndpoint"/>.
    /// The answering side derives the reply channel from this itself
    /// (<see cref="RedisChannelNaming.ReplyChannel"/>) rather than trusting a channel name supplied on
    /// the wire, so a request can never make this node publish to an arbitrary attacker-chosen channel.
    /// </param>
    internal sealed record RedisPubSubClusterRequestEnvelope(
        Guid CorrelationId,
        RedisPubSubClusterRequestKind Kind,
        string? ChannelName,
        Guid? ChannelKey,
        long? SinceTicks,
        Uri ReplyToNodeEndpoint);
}
