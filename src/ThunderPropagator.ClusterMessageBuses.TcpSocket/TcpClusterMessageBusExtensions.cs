using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;

using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.TcpSocket
{
    /// <summary>
    /// DI registration for the raw-TCP direct-peer-connection <see cref="IClusterMessageBus"/> transport.
    /// </summary>
    public static class TcpClusterMessageBusExtensions
    {
        /// <summary>
        /// Registers <see cref="TcpClusterMessageBus"/> as the <see cref="IClusterMessageBus"/>
        /// singleton: this node runs its own TCP listener and dials one persistent outbound
        /// connection per peer (discovered via
        /// <see cref="ThunderPropagator.Application.Channels.Cluster.Discovery.IClusterNodeDiscovery"/>),
        /// multiplexing fan-out, subscription-event, and the hand-rolled request/reply RPC over each
        /// connection — see <see cref="TcpClusterMessageBus"/>'s remarks for the full design.
        /// </summary>
        /// <remarks>
        /// Requires <see cref="ThunderPropagator.Application.Channels.Cluster.ClusterConfiguration.NodeEndpoint"/>
        /// to be set (reused purely as this node's identity, not as a literal listen address — the
        /// TCP listener binds every interface on <see cref="TcpClusterMessageBusOptions.Port"/>) and
        /// an <see cref="ThunderPropagator.Application.Channels.Cluster.Discovery.IClusterNodeDiscovery"/>
        /// to already be registered — this method does not register one itself; register e.g. core's
        /// <c>AddStaticClusterNodeDiscovery(...)</c> alongside this call. Also requires a
        /// <see cref="ThunderPropagator.Infrastructure.Channels.ChannelManager"/> to already be
        /// registered (added automatically by <c>AddThunderPropagator</c>).
        /// <para>
        /// To use a different transport instead, register your own <see cref="IClusterMessageBus"/>
        /// with <c>services.AddSingleton&lt;IClusterMessageBus, TImpl&gt;()</c> — this method
        /// registers its own implementation with <c>TryAddSingleton</c>, which only fills the slot
        /// if nothing is registered yet, so your registration wins whether it runs before or after
        /// this call (or skip this call entirely). No core changes are needed to swap transports.
        /// </para>
        /// </remarks>
        /// <example>
        /// <code>
        /// services.AddThunderPropagator(configSection)
        ///         .AddStaticClusterNodeDiscovery(peerEndpoints)
        ///         .AddClusterTcpMessageBus(options => options.Port = 6200);
        /// </code>
        /// </example>
        public static IServiceCollection AddClusterTcpMessageBus(this IServiceCollection services, Action<TcpClusterMessageBusOptions> configure)
        {
            services.Configure(configure);
            services.TryAddSingleton<IClusterChannelResolver, ChannelManagerResolver>();
            services.TryAddSingleton<TcpClusterMessageBus>();
            services.TryAddSingleton<IClusterMessageBus>(sp => sp.GetRequiredService<TcpClusterMessageBus>());

            return services;
        }
    }
}
