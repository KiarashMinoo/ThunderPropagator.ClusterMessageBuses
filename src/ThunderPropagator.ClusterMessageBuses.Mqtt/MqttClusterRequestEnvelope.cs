namespace ThunderPropagator.ClusterMessageBuses.Mqtt
{
    /// <summary>
    /// Wire envelope published to a target node's <see cref="MqttTopicNaming.RequestTopic"/> for one
    /// of the three request/reply operations
    /// (<see cref="ThunderPropagator.Application.Channels.Cluster.MessageBus.IClusterMessageBus.RestoreFromLeaderAsync"/>,
    /// <c>SyncDeltaFromLeaderAsync</c>, <c>FetchPeerSubscriptionsAsync</c>).
    /// </summary>
    /// <param name="CorrelationId">
    /// Matches the eventual <see cref="MqttClusterResponseEnvelope.CorrelationId"/> so the requester's
    /// reply-topic listener can complete the right pending call. MQTT has no built-in request/reply
    /// (unlike NATS's ephemeral inbox subjects), so this transport tracks correlation by hand — the
    /// same approach used by <c>KafkaClusterMessageBus</c>/<c>PulsarClusterMessageBus</c>.
    /// </param>
    /// <param name="Kind">Which operation is being requested.</param>
    /// <param name="ChannelName">
    /// Identifies the channel by name (not key) for <see cref="MqttClusterRequestKind.RestoreSnapshot"/>
    /// and <see cref="MqttClusterRequestKind.SyncDelta"/>. Unused for
    /// <see cref="MqttClusterRequestKind.FetchSubscriptions"/>, which identifies the channel by
    /// <see cref="ChannelKey"/> instead.
    /// </param>
    /// <param name="ChannelKey">Identifies the channel by key for <see cref="MqttClusterRequestKind.FetchSubscriptions"/>.</param>
    /// <param name="SinceTicks">
    /// <see cref="DateTimeOffset.UtcTicks"/> cutoff for <see cref="MqttClusterRequestKind.SyncDelta"/>;
    /// unused otherwise.
    /// </param>
    /// <param name="ReplyToNodeEndpoint">
    /// The requester's own <see cref="ThunderPropagator.Application.Channels.Cluster.ClusterConfiguration.NodeEndpoint"/>.
    /// The answering side derives the reply topic from this itself
    /// (<see cref="MqttTopicNaming.ReplyTopic"/>) rather than trusting a topic name supplied on the
    /// wire, so a request can never make this node publish to an arbitrary attacker-chosen topic.
    /// </param>
    internal sealed record MqttClusterRequestEnvelope(
        Guid CorrelationId,
        MqttClusterRequestKind Kind,
        string? ChannelName,
        Guid? ChannelKey,
        long? SinceTicks,
        Uri ReplyToNodeEndpoint);
}
