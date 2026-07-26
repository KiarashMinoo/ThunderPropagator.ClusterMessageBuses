using ThunderPropagator.Application.Channels.Snapshots;

namespace ThunderPropagator.ClusterMessageBuses.Kafka
{
    /// <summary>
    /// <see cref="KafkaClusterResponseEnvelope.PayloadJson"/> shape for
    /// <see cref="KafkaClusterRequestKind.SyncDelta"/> — mirrors the HTTP transport's
    /// <c>ClusterSnapshotDeltaEndpoints.SnapshotDeltaResponse</c> exactly, since it answers the same
    /// question (entries updated since a cutoff, plus hash keys deleted since that cutoff) over a
    /// different transport.
    /// </summary>
    internal sealed class KafkaSnapshotDeltaPayload
    {
        public SnapshotEntry[] UpdatedEntries { get; init; } = [];
        public int[] DeletedHashKeys { get; init; } = [];
    }
}
