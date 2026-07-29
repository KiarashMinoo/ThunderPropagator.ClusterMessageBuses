using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.ZeroMQ
{
    /// <summary>DI registration for the ZeroMQ <see cref="IClusterMessageBus"/> transport.</summary>
    public static class ZeroMqClusterMessageBusExtensions
    {
        /// <summary>
        /// Registers <see cref="ZeroMqClusterMessageBus"/> as <see cref="IClusterMessageBus"/>. Uses
        /// <c>TryAddSingleton</c> throughout, so a caller's own <see cref="IClusterMessageBus"/>
        /// registration always wins regardless of call order.
        /// </summary>
        public static IServiceCollection AddClusterZeroMqMessageBus(this IServiceCollection services, Action<ClusterZeroMqOptions> configure)
        {
            services.Configure(configure);
            services.TryAddSingleton<IClusterChannelResolver, ChannelManagerResolver>();
            services.TryAddSingleton<ZeroMqClusterMessageBus>();
            services.TryAddSingleton<IClusterMessageBus>(sp => sp.GetRequiredService<ZeroMqClusterMessageBus>());

            return services;
        }
    }
}
