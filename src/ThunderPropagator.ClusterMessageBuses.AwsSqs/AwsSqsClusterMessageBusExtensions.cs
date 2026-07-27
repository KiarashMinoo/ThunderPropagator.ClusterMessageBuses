using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;

using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.AwsSqs
{
    /// <summary>
    /// DI registration for the AWS SNS/SQS <see cref="IClusterMessageBus"/> transport.
    /// </summary>
    public static class AwsSqsClusterMessageBusExtensions
    {
        /// <summary>
        /// Registers <see cref="AwsSqsClusterMessageBus"/> as the <see cref="IClusterMessageBus"/>
        /// singleton: fan-out and subscription-event propagation over one SNS topic per channel
        /// (with every node's own exclusive SQS queue subscribed to it), plus a broker-native
        /// request/reply scheme (one request/reply queue pair per node) for
        /// <c>RestoreFromLeaderAsync</c>, <c>SyncDeltaFromLeaderAsync</c>, and
        /// <c>FetchPeerSubscriptionsAsync</c>. Requires
        /// <see cref="ThunderPropagator.Application.Channels.Cluster.ClusterConfiguration.NodeEndpoint"/>
        /// to be set (reused purely as this node's queue-naming identity, not as a literal URL), AWS
        /// credentials resolvable via the default AWS SDK credential chain (or supplied via
        /// <see cref="AwsSqsClusterMessageBusOptions.ConfigureSqsClient"/>/<see cref="AwsSqsClusterMessageBusOptions.ConfigureSnsClient"/>),
        /// and a <see cref="ThunderPropagator.Infrastructure.Channels.ChannelManager"/> to already be
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
        ///         .AddClusterAwsSqsMessageBus(options => options.ConfigureSqsClient = c => c.RegionEndpoint = RegionEndpoint.USEast1);
        /// </code>
        /// </example>
        public static IServiceCollection AddClusterAwsSqsMessageBus(this IServiceCollection services, Action<AwsSqsClusterMessageBusOptions> configure)
        {
            services.Configure(configure);
            services.TryAddSingleton<IClusterChannelResolver, ChannelManagerResolver>();
            services.TryAddSingleton<AwsSqsClusterMessageBus>();
            services.TryAddSingleton<IClusterMessageBus>(sp => sp.GetRequiredService<AwsSqsClusterMessageBus>());

            return services;
        }
    }
}
