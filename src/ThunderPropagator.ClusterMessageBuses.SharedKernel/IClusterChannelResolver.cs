using ThunderPropagator.Application.Channels;

namespace ThunderPropagator.ClusterMessageBuses.SharedKernel
{
    /// <summary>
    /// Resolves a live local <see cref="IChannel"/> by name or key when a transport answers a
    /// peer's broker-native (or direct) request. A thin seam over
    /// <see cref="ThunderPropagator.Infrastructure.Channels.ChannelManager"/>: that class is
    /// <c>sealed</c> in Release builds with non-virtual members, so it can't be substituted
    /// directly in tests — <see cref="ChannelManagerResolver"/> is the only production
    /// implementation, and every transport's tests fake this interface instead. Shared here
    /// (rather than duplicated per transport) since every transport in this repo needs exactly
    /// the same seam.
    /// </summary>
    public interface IClusterChannelResolver
    {
        /// <summary>Resolves by channel name (used for RestoreSnapshot/SyncDelta requests, mirroring the HTTP transport's channel-name routes).</summary>
        /// <exception cref="InvalidOperationException">No channel with that name is registered.</exception>
        IChannel GetChannel(string channelName);

        /// <summary>Resolves by channel key (used for FetchSubscriptions requests, mirroring the HTTP transport's channel-key route).</summary>
        /// <exception cref="ThunderPropagator.Infrastructure.Channels.InvalidChannelKeyException">No channel with that key is registered.</exception>
        IChannel GetChannel(Guid channelKey);
    }
}
