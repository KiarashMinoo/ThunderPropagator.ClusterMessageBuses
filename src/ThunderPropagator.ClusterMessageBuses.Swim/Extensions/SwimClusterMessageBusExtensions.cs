using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;

using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.Swim
{
    /// <summary>
    /// DI registration for the SWIM-protocol gossip-based <see cref="IClusterMessageBus"/> transport.
    /// </summary>
    public static class SwimClusterMessageBusExtensions
    {
        /// <summary>
        /// Registers <see cref="SwimClusterMessageBus"/> as the <see cref="IClusterMessageBus"/>
        /// singleton: this node binds a single shared UDP socket used for SWIM's own
        /// failure-detection traffic (pings/ping-reqs/acks, which also carry piggybacked gossip),
        /// for the point-to-point leader/peer-pull request/reply operations, and for whatever
        /// standalone gossip datagrams the periodic gossip round sends -- see
        /// <see cref="SwimClusterMessageBus"/>'s remarks for the full design, including the
        /// eventual-consistency/latency tradeoff its epidemic fan-out makes versus every other
        /// transport in this repo.
        /// </summary>
        /// <remarks>
        /// Requires <see cref="ThunderPropagator.Application.Channels.Cluster.ClusterConfiguration.NodeEndpoint"/>
        /// to be set (reused purely as this node's identity, not as a literal listen address -- the
        /// UDP socket binds every interface on <see cref="SwimClusterMessageBusOptions.Port"/>) and an
        /// <see cref="ThunderPropagator.Application.Channels.Cluster.Discovery.IClusterNodeDiscovery"/>
        /// to already be registered -- this method does not register one itself; register e.g. core's
        /// <c>AddStaticClusterNodeDiscovery(...)</c> alongside this call. Also requires a
        /// <see cref="ThunderPropagator.Infrastructure.Channels.ChannelManager"/> to already be
        /// registered (added automatically by <c>AddThunderPropagator</c>).
        /// <para>
        /// To use a different transport instead, register your own <see cref="IClusterMessageBus"/>
        /// with <c>services.AddSingleton&lt;IClusterMessageBus, TImpl&gt;()</c> -- this method
        /// registers its own implementation with <c>TryAddSingleton</c>, which only fills the slot
        /// if nothing is registered yet, so your registration wins whether it runs before or after
        /// this call (or skip this call entirely). No core changes are needed to swap transports.
        /// </para>
        /// </remarks>
        /// <example>
        /// <code>
        /// services.AddThunderPropagator(configSection)
        ///         .AddStaticClusterNodeDiscovery(peerEndpoints)
        ///         .AddClusterSwimMessageBus(options => options.Port = 6301);
        /// </code>
        /// </example>
        public static IServiceCollection AddClusterSwimMessageBus(this IServiceCollection services, Action<SwimClusterMessageBusOptions> configure)
        {
            services.Configure(configure);
            services.TryAddSingleton<IClusterChannelResolver, ChannelManagerResolver>();
            services.TryAddSingleton<SwimClusterMessageBus>();
            services.TryAddSingleton<IClusterMessageBus>(sp => sp.GetRequiredService<SwimClusterMessageBus>());

            return services;
        }
    }
}
