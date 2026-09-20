using ThunderPropagator.Application.Channels.Snapshots;

namespace ThunderPropagator.ClusterMessageBuses.SharedKernel
{
    /// <summary>
    /// <see cref="ClusterResponseEnvelope.PayloadJson"/> shape for
    /// <see cref="ClusterRequestKind.SyncDelta"/> -- entries updated since a cutoff, plus hash keys
    /// deleted since that cutoff. Consolidated from what used to be per-transport copies
    /// (<c>{Name}SnapshotDeltaPayload</c>) with identical shapes.
    /// </summary>
    public sealed class ClusterSnapshotDeltaPayload
    {
        public SnapshotEntry[] UpdatedEntries { get; init; } = [];
        public int[] DeletedHashKeys { get; init; } = [];
    }
}
