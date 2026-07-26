using ThunderPropagator.Application.Channels.Snapshots;

namespace ThunderPropagator.ClusterMessageBuses.RedisPubSub
{
    /// <summary>
    /// <see cref="RedisPubSubClusterResponseEnvelope.PayloadJson"/> shape for
    /// <see cref="RedisPubSubClusterRequestKind.SyncDelta"/> — mirrors the HTTP transport's
    /// <c>ClusterSnapshotDeltaEndpoints.SnapshotDeltaResponse</c> exactly.
    /// </summary>
    internal sealed class RedisPubSubSnapshotDeltaPayload
    {
        public SnapshotEntry[] UpdatedEntries { get; init; } = [];
        public int[] DeletedHashKeys { get; init; } = [];
    }
}
