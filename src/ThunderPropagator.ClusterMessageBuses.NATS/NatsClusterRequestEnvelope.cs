using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.NATS
{
    /// <summary>
    /// Wire envelope sent as a NATS request to a target node's
    /// <see cref="NatsSubjectNaming.RequestSubject"/> for one of the three broker-native
    /// request/reply operations
    /// (<see cref="ThunderPropagator.Application.Channels.Cluster.MessageBus.IClusterMessageBus.RestoreFromLeaderAsync"/>,
    /// <c>SyncDeltaFromLeaderAsync</c>, <c>FetchPeerSubscriptionsAsync</c>).
    /// </summary>
    /// <remarks>
    /// Unlike <c>KafkaClusterRequestEnvelope</c> / <c>RabbitMqClusterRequestEnvelope</c>, this
    /// envelope carries no correlation id or reply-to address: NATS's native request/reply already
    /// correlates a reply to its request via a per-call ephemeral inbox subject the client library
    /// manages itself, so there is nothing left for this transport to track by hand.
    /// </remarks>
    /// <param name="Kind">Which operation is being requested.</param>
    /// <param name="ChannelName">
    /// Identifies the channel by name (not key) for <see cref="ClusterRequestKind.RestoreSnapshot"/>
    /// and <see cref="ClusterRequestKind.SyncDelta"/>. Unused for
    /// <see cref="ClusterRequestKind.FetchSubscriptions"/>, which identifies the channel by
    /// <see cref="ChannelKey"/> instead.
    /// </param>
    /// <param name="ChannelKey">Identifies the channel by key for <see cref="ClusterRequestKind.FetchSubscriptions"/>.</param>
    /// <param name="SinceTicks">
    /// <see cref="DateTimeOffset.UtcTicks"/> cutoff for <see cref="ClusterRequestKind.SyncDelta"/>;
    /// unused otherwise.
    /// </param>
    internal sealed record NatsClusterRequestEnvelope(
        ClusterRequestKind Kind,
        string? ChannelName,
        Guid? ChannelKey,
        long? SinceTicks);
}
