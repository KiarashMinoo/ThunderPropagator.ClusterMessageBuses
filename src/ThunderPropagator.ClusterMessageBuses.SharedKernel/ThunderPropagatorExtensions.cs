using Microsoft.Extensions.DependencyInjection;
using ThunderPropagator.Application;

namespace ThunderPropagator.ClusterMessageBuses.SharedKernel
{
    /// <summary>
    /// Shared dependency-injection helpers used by every transport project in this repo.
    /// </summary>
    public static partial class ThunderPropagatorExtensions
    {
        /// <summary>
        /// Registers <paramref name="services" /> under the standard ThunderPropagator feature-gate
        /// mechanism, matching the pattern already used by
        /// <c>ThunderPropagator.RecoveryHandler.SharedKernel</c>. Every transport's own
        /// <c>AddCluster{Transport}MessageBus()</c> extension should call through this instead of
        /// registering its <see cref="Microsoft.Extensions.DependencyInjection.IServiceCollection" />
        /// entries unconditionally, so the transport can be feature-flagged off without a code change
        /// at the call site.
        /// </summary>
        /// <typeparam name="TFeature">The feature marker type gating this transport.</typeparam>
        internal static IServiceCollection AddFeature<TFeature>(this IServiceCollection services) where TFeature : class, IFeature
        {
            Infrastructure.Extensions.ThunderPropagatorExtensions.AddFeature<TFeature>(services);
            return services;
        }
    }
}
