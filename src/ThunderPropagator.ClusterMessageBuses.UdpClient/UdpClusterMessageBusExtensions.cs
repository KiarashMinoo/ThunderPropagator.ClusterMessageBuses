using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;

using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.UdpClient
{
    /// <summary>
    /// DI registration for the raw-UDP direct-peer-messaging <see cref="IClusterMessageBus"/> transport.
    /// </summary>
    public static class UdpClusterMessageBusExtensions
    {
        /// <summary>
        /// Registers <see cref="UdpClusterMessageBus"/> as the <see cref="IClusterMessageBus"/>
        /// singleton: this node binds a single shared UDP socket used to both send fan-out/
        /// subscription-event/request datagrams to every peer (discovered via
        /// <see cref="ThunderPropagator.Application.Channels.Cluster.Discovery.IClusterNodeDiscovery"/>)
        /// and receive whatever peers send back — see <see cref="UdpClusterMessageBus"/>'s remarks for
        /// the full design, including how it compensates for UDP's lack of delivery guarantees on the
        /// request/reply path.
        /// </summary>
        /// <remarks>
        /// Requires <see cref="ThunderPropagator.Application.Channels.Cluster.ClusterConfiguration.NodeEndpoint"/>
        /// to be set (reused purely as this node's identity, not as a literal listen address — the
        /// UDP socket binds every interface on <see cref="UdpClusterMessageBusOptions.Port"/>) and an
        /// <see cref="ThunderPropagator.Application.Channels.Cluster.Discovery.IClusterNodeDiscovery"/>
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
        ///         .AddClusterUdpMessageBus(options => options.Port = 6300);
        /// </code>
        /// </example>
        public static IServiceCollection AddClusterUdpMessageBus(this IServiceCollection services, Action<UdpClusterMessageBusOptions> configure)
        {
            services.Configure(configure);
            services.TryAddSingleton<IClusterChannelResolver, ChannelManagerResolver>();
            services.TryAddSingleton<UdpClusterMessageBus>();
            services.TryAddSingleton<IClusterMessageBus>(sp => sp.GetRequiredService<UdpClusterMessageBus>());

            return services;
        }
    }
}
