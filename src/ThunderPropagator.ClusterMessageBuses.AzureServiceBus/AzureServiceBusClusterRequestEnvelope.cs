namespace ThunderPropagator.ClusterMessageBuses.AzureServiceBus
{
    /// <summary>
    /// Wire envelope sent to a target node's <see cref="AzureServiceBusResourceNaming.RequestQueue"/>
    /// for one of the three broker-native request/reply operations
    /// (<see cref="ThunderPropagator.Application.Channels.Cluster.MessageBus.IClusterMessageBus.RestoreFromLeaderAsync"/>,
    /// <c>SyncDeltaFromLeaderAsync</c>, <c>FetchPeerSubscriptionsAsync</c>).
    /// </summary>
    /// <param name="CorrelationId">
    /// Matches the eventual <see cref="AzureServiceBusClusterResponseEnvelope.CorrelationId"/> so the
    /// requester's reply-queue poller can complete the right pending call.
    /// </param>
    /// <param name="Kind">Which operation is being requested.</param>
    /// <param name="ChannelName">
    /// Identifies the channel by name (not key) for <see cref="AzureServiceBusClusterRequestKind.RestoreSnapshot"/>
    /// and <see cref="AzureServiceBusClusterRequestKind.SyncDelta"/>. Unused for
    /// <see cref="AzureServiceBusClusterRequestKind.FetchSubscriptions"/>, which identifies the
    /// channel by <see cref="ChannelKey"/> instead.
    /// </param>
    /// <param name="ChannelKey">Identifies the channel by key for <see cref="AzureServiceBusClusterRequestKind.FetchSubscriptions"/>.</param>
    /// <param name="SinceTicks">
    /// <see cref="DateTimeOffset.UtcTicks"/> cutoff for <see cref="AzureServiceBusClusterRequestKind.SyncDelta"/>;
    /// unused otherwise.
    /// </param>
    /// <param name="ReplyToNodeEndpoint">
    /// The requester's own <see cref="ThunderPropagator.Application.Channels.Cluster.ClusterConfiguration.NodeEndpoint"/>.
    /// The answering side derives the reply queue from this itself
    /// (<see cref="AzureServiceBusResourceNaming.ReplyQueue"/>) rather than trusting a queue name
    /// supplied on the wire, so a request can never make this node send to an arbitrary
    /// attacker-chosen queue.
    /// </param>
    internal sealed record AzureServiceBusClusterRequestEnvelope(
        Guid CorrelationId,
        AzureServiceBusClusterRequestKind Kind,
        string? ChannelName,
        Guid? ChannelKey,
        long? SinceTicks,
        Uri ReplyToNodeEndpoint);
}
