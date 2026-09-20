using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;

using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.Rdma
{
    /// <summary>
    /// DI registration for the RDMA (InfiniBand/RoCEv2) direct-peer-connection
    /// <see cref="IClusterMessageBus"/> transport.
    /// </summary>
    public static class RdmaClusterMessageBusExtensions
    {
        /// <summary>
        /// Registers <see cref="RdmaClusterMessageBus"/> as the <see cref="IClusterMessageBus"/>
        /// singleton: this node runs its own RDMA CM listener and dials one persistent outbound
        /// reliable-connected (RC) queue pair per peer (discovered via
        /// <see cref="ThunderPropagator.Application.Channels.Cluster.Discovery.IClusterNodeDiscovery"/>),
        /// multiplexing fan-out, subscription-event, byte fan-out, and the hand-rolled
        /// request/reply RPC over each queue pair -- see <see cref="RdmaClusterMessageBus"/>'s
        /// remarks for the full design.
        /// </summary>
        /// <remarks>
        /// <para>
        /// !!! NOT VALIDATED ON REAL HARDWARE !!! This transport was written and reviewed without
        /// access to a C compiler, the dotnet SDK, or RDMA-capable hardware -- it is a best-effort
        /// implementation against the documented public <c>libibverbs</c>/<c>librdmacm</c> C APIs,
        /// for the consuming application's own team to compile and validate against real Linux
        /// RDMA hardware before relying on it. See the banner comments at the top of
        /// <c>Native/LibIbVerbsNativeMethods.cs</c> and <c>Native/LibRdmaCmNativeMethods.cs</c> for
        /// exactly what must be checked first -- struct field offsets in native interop are a
        /// correctness-critical risk (silent memory corruption), not a cosmetic one.
        /// </para>
        /// <para>
        /// Runtime requirements this method does not, and cannot, verify at registration time:
        /// </para>
        /// <list type="bullet">
        /// <item><description>
        /// <c>libibverbs.so.1</c> and <c>librdmacm.so.1</c> must already be installed on the host
        /// (e.g. via the distro's <c>rdma-core</c>/<c>libibverbs</c>/<c>librdmacm</c> packages).
        /// They are native OS shared libraries, not NuGet packages -- nothing in this project's
        /// <c>.csproj</c> installs or vendors them. If either is missing, the first native call
        /// throws <see cref="System.DllNotFoundException"/> the first time this transport is used
        /// (lazily, on first publish/subscribe/listen -- not at DI registration time).
        /// </description></item>
        /// <item><description>
        /// This transport only functions on Linux (both native libraries are Linux shared objects;
        /// there is no Windows/macOS RDMA userspace stack this binds against).
        /// </description></item>
        /// <item><description>
        /// The host needs a real RDMA-capable device -- an InfiniBand HCA, or a RoCEv2-capable
        /// Ethernet NIC with its RDMA driver stack loaded. On a host with the libraries installed
        /// but no such device, <c>ibv_get_device_list</c> returns zero devices and this transport
        /// fails at first use with a descriptive exception, not a silent no-op.
        /// </description></item>
        /// </list>
        /// <para>
        /// Also requires <see cref="ThunderPropagator.Application.Channels.Cluster.ClusterConfiguration.NodeEndpoint"/>
        /// to be set (reused purely as this node's identity, not as a literal listen address) and
        /// an <see cref="ThunderPropagator.Application.Channels.Cluster.Discovery.IClusterNodeDiscovery"/>
        /// to already be registered -- this method does not register one itself; register e.g.
        /// core's <c>AddStaticClusterNodeDiscovery(...)</c> alongside this call. Also requires a
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
        ///         .AddClusterRdmaMessageBus(options => options.Port = 18515);
        /// </code>
        /// </example>
        public static IServiceCollection AddClusterRdmaMessageBus(this IServiceCollection services, Action<RdmaClusterMessageBusOptions> configure)
        {
            services.Configure(configure);
            services.TryAddSingleton<IClusterChannelResolver, ChannelManagerResolver>();
            services.TryAddSingleton<RdmaClusterMessageBus>();
            services.TryAddSingleton<IClusterMessageBus>(sp => sp.GetRequiredService<RdmaClusterMessageBus>());

            return services;
        }
    }
}
