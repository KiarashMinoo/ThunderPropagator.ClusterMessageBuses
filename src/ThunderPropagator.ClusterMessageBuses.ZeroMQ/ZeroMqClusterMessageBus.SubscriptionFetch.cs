using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.BuildingBlocks.Application.Helpers;

namespace ThunderPropagator.ClusterMessageBuses.ZeroMQ
{
    internal sealed partial class ZeroMqClusterMessageBus
    {
        public override async Task<IReadOnlyList<ClusterSubscriptionDescriptor>> FetchPeerSubscriptionsAsync(Uri peerEndpoint, Guid channelKey, CancellationToken cancellationToken = default)
        {
            try
            {
                var response = await SendRequestAsync(
                    peerEndpoint, ZeroMqClusterRequestKind.FetchSubscriptions, null, channelKey, null, cancellationToken)
                    .ConfigureAwait(false);

                return response.PayloadJson?.FromNJson<ClusterSubscriptionDescriptor[]>() ?? [];
            }
            catch (Exception exception) when (exception is TimeoutException or InvalidOperationException)
            {
                // FetchPeerSubscriptionsAsync is called against every discovered peer during
                // bootstrap — an unreachable or rejecting one must degrade gracefully rather than
                // fail the whole bootstrap, mirroring every other transport's same behavior here.
                return [];
            }
        }

        /// <summary>Answering side of <see cref="FetchPeerSubscriptionsAsync"/>.</summary>
        private Task<ZeroMqClusterResponseEnvelope> BuildFetchSubscriptionsResponseAsync(ZeroMqClusterRequestEnvelope request, CancellationToken cancellationToken)
        {
            try
            {
                var channel = _channelResolver.GetChannel(request.ChannelKey!.Value);
                var descriptors = GetLocalClusterSubscriptionDescriptors(channel);

                return Task.FromResult(new ZeroMqClusterResponseEnvelope(request.CorrelationId, true, null, descriptors.ToNJson()));
            }
            catch (Exception exception)
            {
                return Task.FromResult(new ZeroMqClusterResponseEnvelope(request.CorrelationId, false, exception.Message, null));
            }
        }
    }
}
