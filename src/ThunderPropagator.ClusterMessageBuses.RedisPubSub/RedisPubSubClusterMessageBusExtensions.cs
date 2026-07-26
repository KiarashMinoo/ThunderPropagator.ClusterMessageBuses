using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;

using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.RedisPubSub
{
    /// <summary>
    /// DI registration for the Redis pub/sub <see cref="IClusterMessageBus"/> transport.
    /// </summary>
    public static class RedisPubSubClusterMessageBusExtensions
    {
        /// <summary>
        /// Registers <see cref="RedisPubSubClusterMessageBus"/> as the <see cref="IClusterMessageBus"/>
        /// singleton: fan-out and subscription-event propagation over one Redis pub/sub channel per
        /// ThunderPropagator channel (every node subscribing independently, since plain Redis pub/sub
        /// already delivers a copy of every message to every subscriber), plus a hand-rolled
        /// correlation-id request/reply scheme over one Redis channel pair per node for
        /// <c>RestoreFromLeaderAsync</c>, <c>SyncDeltaFromLeaderAsync</c>, and
        /// <c>FetchPeerSubscriptionsAsync</c>. Requires
        /// <see cref="ThunderPropagator.Application.Channels.Cluster.ClusterConfiguration.NodeEndpoint"/>
        /// to be set (reused purely as this node's channel-naming identity, not as a literal URL) and
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
        ///         .AddClusterRedisPubSubMessageBus(options => options.ConnectionString = "redis1:6379");
        /// </code>
        /// </example>
        public static IServiceCollection AddClusterRedisPubSubMessageBus(this IServiceCollection services, Action<RedisPubSubClusterMessageBusOptions> configure)
        {
            services.Configure(configure);
            services.TryAddSingleton<IClusterChannelResolver, ChannelManagerResolver>();
            services.TryAddSingleton<RedisPubSubClusterMessageBus>();
            services.TryAddSingleton<IClusterMessageBus>(sp => sp.GetRequiredService<RedisPubSubClusterMessageBus>());

            return services;
        }
    }
}
