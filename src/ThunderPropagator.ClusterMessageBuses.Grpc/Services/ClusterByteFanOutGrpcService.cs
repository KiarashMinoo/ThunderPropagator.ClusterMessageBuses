using Grpc.Core;
using ThunderPropagator.ClusterMessageBuses.Grpc.Protos;

namespace ThunderPropagator.ClusterMessageBuses.Grpc.Services
{
    /// <summary>
    /// Server-side implementation of <c>ClusterByteFanOut</c>: byte-oriented counterpart to
    /// <see cref="ClusterFanOutGrpcService"/> -- reads every <see cref="PushedByteMessageBatch"/> a
    /// peer writes on its outbound call to this node and dispatches each to
    /// <see cref="GrpcClusterMessageBus.HandleByteFanOutDeliveryAsync"/>.
    /// </summary>
    internal sealed class ClusterByteFanOutGrpcService : ClusterByteFanOut.ClusterByteFanOutBase
    {
        private readonly GrpcClusterMessageBus _bus;

        public ClusterByteFanOutGrpcService(GrpcClusterMessageBus bus)
        {
            _bus = bus;
        }

        public override async Task Stream(
            IAsyncStreamReader<PushedByteMessageBatch> requestStream,
            IServerStreamWriter<PushedByteMessageBatch> responseStream,
            ServerCallContext context)
        {
            await foreach (var batch in requestStream.ReadAllAsync(context.CancellationToken).ConfigureAwait(false))
            {
                await _bus.HandleByteFanOutDeliveryAsync(batch, context.CancellationToken).ConfigureAwait(false);
            }
        }
    }
}
