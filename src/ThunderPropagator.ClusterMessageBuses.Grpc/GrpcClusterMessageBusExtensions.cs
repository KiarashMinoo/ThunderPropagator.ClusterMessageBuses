using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.Grpc
{
    /// <summary>DI registration for the gRPC <see cref="IClusterMessageBus"/> transport.</summary>
    public static class GrpcClusterMessageBusExtensions
    {
        /// <summary>
        /// Registers <see cref="GrpcClusterMessageBus"/> as <see cref="IClusterMessageBus"/>. Uses
        /// <c>TryAddSingleton</c> throughout, so a caller's own <see cref="IClusterMessageBus"/>
        /// registration always wins regardless of call order.
        /// </summary>
        public static IServiceCollection AddClusterGrpcMessageBus(this IServiceCollection services, Action<GrpcClusterMessageBusOptions> configure)
        {
            services.Configure(configure);
            services.TryAddSingleton<IClusterChannelResolver, ChannelManagerResolver>();
            services.TryAddSingleton<GrpcClusterMessageBus>();
            services.TryAddSingleton<IClusterMessageBus>(sp => sp.GetRequiredService<GrpcClusterMessageBus>());

            return services;
        }
    }
}
