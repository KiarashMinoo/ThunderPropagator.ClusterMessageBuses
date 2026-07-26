using ThunderPropagator.Application.Channels.Snapshots;

namespace ThunderPropagator.ClusterMessageBuses.NATS
{
    /// <summary>
    /// <see cref="NatsClusterResponseEnvelope.PayloadJson"/> shape for
    /// <see cref="NatsClusterRequestKind.SyncDelta"/> — mirrors the HTTP transport's
    /// <c>ClusterSnapshotDeltaEndpoints.SnapshotDeltaResponse</c> exactly.
    /// </summary>
    internal sealed class NatsSnapshotDeltaPayload
    {
        public SnapshotEntry[] UpdatedEntries { get; init; } = [];
        public int[] DeletedHashKeys { get; init; } = [];
    }
}
