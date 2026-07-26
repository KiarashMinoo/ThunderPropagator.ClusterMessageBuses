using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;

using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.WebApi
{
    /// <summary>
    /// DI registration for the self-hosted HTTP direct-peer-connection <see cref="IClusterMessageBus"/> transport.
    /// </summary>
    public static class WebApiClusterMessageBusExtensions
    {
        /// <summary>
        /// Registers <see cref="WebApiClusterMessageBus"/> as the <see cref="IClusterMessageBus"/>
        /// singleton: this node binds its own embedded HTTP listener and calls out to peers
        /// (discovered via
        /// <see cref="ThunderPropagator.Application.Channels.Cluster.Discovery.IClusterNodeDiscovery"/>)
        /// with a plain <see cref="System.Net.Http.HttpClient"/> — no ASP.NET Core hosting is
        /// required by either side, unlike core's own <c>HttpClusterMessageBus</c>. See
        /// <see cref="WebApiClusterMessageBus"/>'s remarks for the full design.
        /// </summary>
        /// <remarks>
        /// Requires <see cref="ThunderPropagator.Application.Channels.Cluster.ClusterConfiguration.NodeEndpoint"/>
        /// to be set (used both as this node's own listener bind address and its identity) and an
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
        ///         .AddClusterWebApiMessageBus(options => options.ListenPath = "/cluster/webapi");
        /// </code>
        /// </example>
        public static IServiceCollection AddClusterWebApiMessageBus(this IServiceCollection services, Action<WebApiClusterMessageBusOptions> configure)
        {
            services.Configure(configure);
            services.TryAddSingleton<IClusterChannelResolver, ChannelManagerResolver>();
            services.TryAddSingleton<WebApiClusterMessageBus>();
            services.TryAddSingleton<IClusterMessageBus>(sp => sp.GetRequiredService<WebApiClusterMessageBus>());

            return services;
        }
    }
}
