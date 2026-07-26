using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;

using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.NATS
{
    /// <summary>
    /// DI registration for the NATS <see cref="IClusterMessageBus"/> transport.
    /// </summary>
    public static class NatsClusterMessageBusExtensions
    {
        /// <summary>
        /// Registers <see cref="NatsClusterMessageBus"/> as the <see cref="IClusterMessageBus"/>
        /// singleton: fan-out and subscription-event propagation over one subject per channel, plus
        /// NATS's native request/reply for <c>RestoreFromLeaderAsync</c>, <c>SyncDeltaFromLeaderAsync</c>,
        /// and <c>FetchPeerSubscriptionsAsync</c> (one request subject per node; replies use NATS's
        /// own per-call ephemeral inbox subjects). Requires
        /// <see cref="ThunderPropagator.Application.Channels.Cluster.ClusterConfiguration.NodeEndpoint"/>
        /// to be set (reused purely as this node's subject-naming identity, not as a literal URL) and
        /// a <see cref="ThunderPropagator.Infrastructure.Channels.ChannelManager"/> to already be
        /// registered (added automatically by <c>AddThunderPropagator</c>).
        /// </summary>
        /// <remarks>
        /// To use a different transport instead, register your own <see cref="IClusterMessageBus"/>
        /// with <c>services.AddSingleton&lt;IClusterMessageBus, TImpl&gt;()</c> — this method
        /// registers its own implementation with <c>TryAddSingleton</c>, which only fills the slot
        /// if nothing is registered yet, so your registration wins whether it runs before or after
        /// this call (or skip this call entirely). No core changes are needed to swap transports.
        /// </remarks>
        /// <example>
        /// <code>
        /// services.AddThunderPropagator(configSection)
        ///         .AddClusterNatsMessageBus(options => options.Url = "nats://broker1:4222");
        /// </code>
        /// </example>
        public static IServiceCollection AddClusterNatsMessageBus(this IServiceCollection services, Action<NatsClusterMessageBusOptions> configure)
        {
            services.Configure(configure);
            services.TryAddSingleton<IClusterChannelResolver, ChannelManagerResolver>();
            services.TryAddSingleton<NatsClusterMessageBus>();
            services.TryAddSingleton<IClusterMessageBus>(sp => sp.GetRequiredService<NatsClusterMessageBus>());

            return services;
        }
    }
}
