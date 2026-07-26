namespace ThunderPropagator.ClusterMessageBuses.Kafka
{
    /// <summary>
    /// Wire envelope published to a target node's <see cref="KafkaTopicNaming.RequestTopic"/> for
    /// one of the three broker-native request/reply operations
    /// (<see cref="ThunderPropagator.Application.Channels.Cluster.MessageBus.IClusterMessageBus.RestoreFromLeaderAsync"/>,
    /// <c>SyncDeltaFromLeaderAsync</c>, <c>FetchPeerSubscriptionsAsync</c>).
    /// </summary>
    /// <param name="CorrelationId">
    /// Matches the eventual <see cref="KafkaClusterResponseEnvelope.CorrelationId"/> so the
    /// requester's reply-topic listener can complete the right pending call.
    /// </param>
    /// <param name="Kind">Which operation is being requested.</param>
    /// <param name="ChannelName">
    /// Identifies the channel by name (not key) for <see cref="KafkaClusterRequestKind.RestoreSnapshot"/>
    /// and <see cref="KafkaClusterRequestKind.SyncDelta"/> — mirrors the HTTP transport's use of
    /// <c>channel.Metadata.ChannelName</c> in its snapshot/delta routes. The answering side resolves
    /// its own local <see cref="ThunderPropagator.Application.Channels.IChannel"/> for that name via
    /// <see cref="ThunderPropagator.Infrastructure.Channels.ChannelManager.GetChannel(string)"/>.
    /// Unused for <see cref="KafkaClusterRequestKind.FetchSubscriptions"/>, which identifies the
    /// channel by <see cref="ChannelKey"/> instead (matching the HTTP transport's
    /// <c>/subscriptions/{channelKey}/self</c> route).
    /// </param>
    /// <param name="ChannelKey">Identifies the channel by key for <see cref="KafkaClusterRequestKind.FetchSubscriptions"/>.</param>
    /// <param name="SinceTicks">
    /// <see cref="DateTimeOffset.UtcTicks"/> cutoff for <see cref="KafkaClusterRequestKind.SyncDelta"/>;
    /// unused otherwise.
    /// </param>
    /// <param name="ReplyToNodeEndpoint">
    /// The requester's own <see cref="ThunderPropagator.Application.Channels.Cluster.ClusterConfiguration.NodeEndpoint"/>.
    /// The answering side derives the reply topic from this itself
    /// (<see cref="KafkaTopicNaming.ReplyTopic"/>) rather than trusting a topic name supplied on the
    /// wire, so a request can never make this node produce to an arbitrary attacker-chosen topic.
    /// </param>
    internal sealed record KafkaClusterRequestEnvelope(
        Guid CorrelationId,
        KafkaClusterRequestKind Kind,
        string? ChannelName,
        Guid? ChannelKey,
        long? SinceTicks,
        Uri ReplyToNodeEndpoint);
}
