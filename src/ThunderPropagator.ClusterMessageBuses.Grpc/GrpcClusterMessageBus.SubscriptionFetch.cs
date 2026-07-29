using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.Grpc.Protos;

namespace ThunderPropagator.ClusterMessageBuses.Grpc
{
    internal sealed partial class GrpcClusterMessageBus
    {
        public override async Task<IReadOnlyList<ClusterSubscriptionDescriptor>> FetchPeerSubscriptionsAsync(Uri peerEndpoint, Guid channelKey, CancellationToken cancellationToken = default)
        {
            try
            {
                var clients = await _unaryClientsFactory(peerEndpoint, cancellationToken).ConfigureAwait(false);
                var request = new FetchSubscriptionsRequest { ChannelKey = channelKey.ToString() };

                var response = await CallClusterUnaryAsync(
                    (deadline, ct) => clients.SubscriptionFetch.FetchSubscriptionsAsync(request, deadline: deadline, cancellationToken: ct),
                    cancellationToken).ConfigureAwait(false);

                if (!response.Success)
                    return [];

                return response.DescriptorsJson.FromNJson<ClusterSubscriptionDescriptor[]>() ?? [];
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
        internal Task<FetchSubscriptionsResponse> BuildFetchSubscriptionsResponseAsync(FetchSubscriptionsRequest request, CancellationToken cancellationToken)
        {
            try
            {
                if (!Guid.TryParse(request.ChannelKey, out var channelKey))
                    throw new FormatException($"'{request.ChannelKey}' is not a valid channel key.");

                var channel = _channelResolver.GetChannel(channelKey);
                var descriptors = GetLocalClusterSubscriptionDescriptors(channel);

                return Task.FromResult(new FetchSubscriptionsResponse { Success = true, DescriptorsJson = descriptors.ToNJson() });
            }
            catch (Exception exception)
            {
                return Task.FromResult(new FetchSubscriptionsResponse { Success = false, ErrorMessage = exception.Message });
            }
        }
    }
}
