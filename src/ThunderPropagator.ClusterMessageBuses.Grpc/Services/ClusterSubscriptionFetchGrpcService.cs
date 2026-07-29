using Grpc.Core;
using ThunderPropagator.ClusterMessageBuses.Grpc.Protos;

namespace ThunderPropagator.ClusterMessageBuses.Grpc.Services
{
    /// <summary>Server-side implementation of <c>ClusterSubscriptionFetch</c>: delegates straight to <see cref="GrpcClusterMessageBus"/>'s answering-side builder.</summary>
    internal sealed class ClusterSubscriptionFetchGrpcService : ClusterSubscriptionFetch.ClusterSubscriptionFetchBase
    {
        private readonly GrpcClusterMessageBus _bus;

        public ClusterSubscriptionFetchGrpcService(GrpcClusterMessageBus bus)
        {
            _bus = bus;
        }

        public override Task<FetchSubscriptionsResponse> FetchSubscriptions(FetchSubscriptionsRequest request, ServerCallContext context)
            => _bus.BuildFetchSubscriptionsResponseAsync(request, context.CancellationToken);
    }
}
