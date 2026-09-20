using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;

using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.Srd
{
    /// <summary>
    /// DI registration for the native-libfabric, AWS-EFA/SRD-backed <see cref="IClusterMessageBus"/>
    /// transport.
    /// </summary>
    public static class SrdClusterMessageBusExtensions
    {
        /// <summary>
        /// Registers <see cref="SrdClusterMessageBus"/> as the <see cref="IClusterMessageBus"/>
        /// singleton: this node binds a single shared libfabric RDM endpoint (over the "efa" provider)
        /// used to both send fan-out/subscription-event/request messages to every peer (discovered via
        /// <see cref="ThunderPropagator.Application.Channels.Cluster.Discovery.IClusterNodeDiscovery"/>)
        /// and receive whatever peers send back — see <see cref="SrdClusterMessageBus"/>'s remarks for
        /// the full design.
        /// </summary>
        /// <remarks>
        /// <para>
        /// <b>THIS TRANSPORT REQUIRES A NATIVE OS LIBRARY, NOT A NUGET PACKAGE</b> —
        /// <c>libfabric.so.1</c> (typically installed via a distribution's <c>libfabric-dev</c>/
        /// <c>libfabric-devel</c> package, or AWS's own <c>aws-efa-installer</c> on an EC2 instance)
        /// must already be present on the host's library search path. Nothing in this project's
        /// <c>.csproj</c> provides, restores, or bundles it — this is pure <c>DllImport</c>
        /// native interop, not a managed libfabric binding (none exists as a NuGet package).
        /// </para>
        /// <para>
        /// <b>THIS TRANSPORT ONLY FUNCTIONS ON AWS EC2 INSTANCES WITH AN ELASTIC FABRIC ADAPTER (EFA)
        /// ATTACHED.</b> On every other host — including a developer workstation, CI runner, or any
        /// non-EFA-equipped EC2 instance type — the underlying <c>fi_getinfo</c> call will find no
        /// matching "efa" provider, and construction will throw a clear
        /// <see cref="InvalidOperationException"/> explaining exactly that, rather than surfacing a
        /// cryptic P/Invoke failure. See <c>Native/LibFabricNativeMethods.cs</c>'s banner comment and
        /// <c>Endpoint/LibFabricSrdClusterEndpoint.cs</c>'s remarks for the full list of what has not
        /// been validated (struct field layouts against the real libfabric headers, and this
        /// transport's placeholder peer-addressing scheme in <c>Endpoint/SrdAddressing.cs</c> chief
        /// among them) — this transport has been written entirely against documentation, with no
        /// dotnet SDK, C compiler, or EFA hardware available to compile, link, or run it, and needs
        /// full validation on a real EFA-equipped EC2 instance before it can be trusted in production.
        /// </para>
        /// <para>
        /// Requires <see cref="ThunderPropagator.Application.Channels.Cluster.ClusterConfiguration.NodeEndpoint"/>
        /// to be set (reused purely as this node's identity) and an
        /// <see cref="ThunderPropagator.Application.Channels.Cluster.Discovery.IClusterNodeDiscovery"/>
        /// to already be registered — this method does not register one itself; register e.g. core's
        /// <c>AddStaticClusterNodeDiscovery(...)</c> alongside this call. Also requires a
        /// <see cref="ThunderPropagator.Infrastructure.Channels.ChannelManager"/> to already be
        /// registered (added automatically by <c>AddThunderPropagator</c>).
        /// </para>
        /// <para>
        /// To use a different transport instead, register your own <see cref="IClusterMessageBus"/>
        /// with <c>services.AddSingleton&lt;IClusterMessageBus, TImpl&gt;()</c> — this method registers
        /// its own implementation with <c>TryAddSingleton</c>, which only fills the slot if nothing is
        /// registered yet, so your registration wins whether it runs before or after this call (or
        /// skip this call entirely). No core changes are needed to swap transports.
        /// </para>
        /// </remarks>
        /// <example>
        /// <code>
        /// services.AddThunderPropagator(configSection)
        ///         .AddStaticClusterNodeDiscovery(peerEndpoints)
        ///         .AddClusterSrdMessageBus(options => options.MaxMessageSize = 8192);
        /// </code>
        /// </example>
        public static IServiceCollection AddClusterSrdMessageBus(this IServiceCollection services, Action<SrdClusterMessageBusOptions> configure)
        {
            services.Configure(configure);
            services.TryAddSingleton<IClusterChannelResolver, ChannelManagerResolver>();
            services.TryAddSingleton<SrdClusterMessageBus>();
            services.TryAddSingleton<IClusterMessageBus>(sp => sp.GetRequiredService<SrdClusterMessageBus>());

            return services;
        }
    }
}
