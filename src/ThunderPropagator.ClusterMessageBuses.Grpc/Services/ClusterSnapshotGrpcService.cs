using Grpc.Core;
using ThunderPropagator.ClusterMessageBuses.Grpc.Protos;

namespace ThunderPropagator.ClusterMessageBuses.Grpc.Services
{
    /// <summary>Server-side implementation of <c>ClusterSnapshot</c>: delegates straight to <see cref="GrpcClusterMessageBus"/>'s answering-side builders.</summary>
    internal sealed class ClusterSnapshotGrpcService : ClusterSnapshot.ClusterSnapshotBase
    {
        private readonly GrpcClusterMessageBus _bus;

        public ClusterSnapshotGrpcService(GrpcClusterMessageBus bus)
        {
            _bus = bus;
        }

        public override Task<RestoreSnapshotResponse> RestoreSnapshot(RestoreSnapshotRequest request, ServerCallContext context)
            => _bus.BuildRestoreSnapshotResponseAsync(request, context.CancellationToken);

        public override Task<SyncDeltaResponse> SyncDelta(SyncDeltaRequest request, ServerCallContext context)
            => _bus.BuildSyncDeltaResponseAsync(request, context.CancellationToken);
    }
}
