using Grpc.Core;
using ThunderPropagator.ClusterMessageBuses.Grpc.Protos;

namespace ThunderPropagator.ClusterMessageBuses.Grpc.Services
{
    /// <summary>
    /// Server-side implementation of <c>ClusterSubscriptionSync</c>: dispatches every inbound
    /// <see cref="SubscriptionEvent"/> to <see cref="GrpcClusterMessageBus.HandleSubscriptionEventDeliveryAsync"/>,
    /// acknowledging each one — the peer's own maintenance loop reads these acks purely as a
    /// liveness signal for its outbound stream (see <c>GrpcClusterMessageBus.RunPeerMaintenanceLoopAsync</c>).
    /// </summary>
    internal sealed class ClusterSubscriptionSyncGrpcService : ClusterSubscriptionSync.ClusterSubscriptionSyncBase
    {
        private readonly GrpcClusterMessageBus _bus;

        public ClusterSubscriptionSyncGrpcService(GrpcClusterMessageBus bus)
        {
            _bus = bus;
        }

        public override async Task Stream(
            IAsyncStreamReader<SubscriptionEvent> requestStream,
            IServerStreamWriter<SubscriptionAck> responseStream,
            ServerCallContext context)
        {
            await foreach (var wireEvent in requestStream.ReadAllAsync(context.CancellationToken).ConfigureAwait(false))
            {
                await _bus.HandleSubscriptionEventDeliveryAsync(wireEvent, context.CancellationToken).ConfigureAwait(false);
                await responseStream.WriteAsync(new SubscriptionAck { Acknowledged = true }).ConfigureAwait(false);
            }
        }
    }
}
