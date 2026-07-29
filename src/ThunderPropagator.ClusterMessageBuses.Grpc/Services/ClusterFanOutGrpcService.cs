using Grpc.Core;
using ThunderPropagator.ClusterMessageBuses.Grpc.Protos;

namespace ThunderPropagator.ClusterMessageBuses.Grpc.Services
{
    /// <summary>
    /// Server-side implementation of <c>ClusterFanOut</c>: reads every <see cref="PushedMessageBatch"/>
    /// a peer writes on its outbound call to this node and dispatches each to
    /// <see cref="GrpcClusterMessageBus.HandleFanOutDeliveryAsync"/>. Resolved per-call by ASP.NET
    /// Core's own gRPC service activation; <see cref="GrpcClusterMessageBus"/> is registered as a
    /// singleton in the embedded host's DI container (see
    /// <c>GrpcClusterMessageBus.DefaultHostFactoryAsync</c>), so every call reaches the same handler
    /// registries the bus's own outbound publishes use.
    /// </summary>
    internal sealed class ClusterFanOutGrpcService : ClusterFanOut.ClusterFanOutBase
    {
        private readonly GrpcClusterMessageBus _bus;

        public ClusterFanOutGrpcService(GrpcClusterMessageBus bus)
        {
            _bus = bus;
        }

        public override async Task Stream(
            IAsyncStreamReader<PushedMessageBatch> requestStream,
            IServerStreamWriter<PushedMessageBatch> responseStream,
            ServerCallContext context)
        {
            await foreach (var batch in requestStream.ReadAllAsync(context.CancellationToken).ConfigureAwait(false))
            {
                await _bus.HandleFanOutDeliveryAsync(batch, context.CancellationToken).ConfigureAwait(false);
            }
        }
    }
}
