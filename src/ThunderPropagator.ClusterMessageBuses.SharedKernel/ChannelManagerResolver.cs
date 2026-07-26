using ThunderPropagator.Application.Channels;
using ThunderPropagator.Infrastructure.Channels;

namespace ThunderPropagator.ClusterMessageBuses.SharedKernel
{
    /// <summary>
    /// Production <see cref="IClusterChannelResolver"/> — delegates straight to the real,
    /// DI-registered <see cref="ChannelManager"/> singleton.
    /// </summary>
    public sealed class ChannelManagerResolver : IClusterChannelResolver
    {
        private readonly ChannelManager _channelManager;

        public ChannelManagerResolver(ChannelManager channelManager)
        {
            _channelManager = channelManager;
        }

        public IChannel GetChannel(string channelName) => _channelManager.GetChannel(channelName);

        public IChannel GetChannel(Guid channelKey) => _channelManager.GetChannel(channelKey);
    }
}
