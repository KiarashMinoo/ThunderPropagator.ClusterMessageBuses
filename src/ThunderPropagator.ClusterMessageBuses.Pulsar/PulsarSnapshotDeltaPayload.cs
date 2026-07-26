using ThunderPropagator.Application.Channels.Snapshots;

namespace ThunderPropagator.ClusterMessageBuses.Pulsar
{
    /// <summary>
    /// <see cref="PulsarClusterResponseEnvelope.PayloadJson"/> shape for
    /// <see cref="PulsarClusterRequestKind.SyncDelta"/> — mirrors the HTTP transport's
    /// <c>ClusterSnapshotDeltaEndpoints.SnapshotDeltaResponse</c> exactly.
    /// </summary>
    internal sealed class PulsarSnapshotDeltaPayload
    {
        public SnapshotEntry[] UpdatedEntries { get; init; } = [];
        public int[] DeletedHashKeys { get; init; } = [];
    }
}
