using ThunderPropagator.Application.Channels.Snapshots;

namespace ThunderPropagator.ClusterMessageBuses.RabbitMQ
{
    /// <summary>
    /// <see cref="RabbitMqClusterResponseEnvelope.PayloadJson"/> shape for
    /// <see cref="RabbitMqClusterRequestKind.SyncDelta"/> — mirrors the HTTP transport's
    /// <c>ClusterSnapshotDeltaEndpoints.SnapshotDeltaResponse</c> exactly.
    /// </summary>
    internal sealed class RabbitMqSnapshotDeltaPayload
    {
        public SnapshotEntry[] UpdatedEntries { get; init; } = [];
        public int[] DeletedHashKeys { get; init; } = [];
    }
}
