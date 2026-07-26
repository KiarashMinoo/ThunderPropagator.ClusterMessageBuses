using ThunderPropagator.Application.Channels.Snapshots;

namespace ThunderPropagator.ClusterMessageBuses.ActiveMQ
{
    /// <summary>
    /// <see cref="ActiveMqClusterResponseEnvelope.PayloadJson"/> shape for
    /// <see cref="ActiveMqClusterRequestKind.SyncDelta"/> — mirrors the HTTP transport's
    /// <c>ClusterSnapshotDeltaEndpoints.SnapshotDeltaResponse</c> exactly.
    /// </summary>
    internal sealed class ActiveMqSnapshotDeltaPayload
    {
        public SnapshotEntry[] UpdatedEntries { get; init; } = [];
        public int[] DeletedHashKeys { get; init; } = [];
    }
}
