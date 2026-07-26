using Microsoft.Extensions.Logging;
using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.BuildingBlocks.Application.Helpers;

namespace ThunderPropagator.ClusterMessageBuses.ActiveMQ
{
    internal sealed partial class ActiveMqClusterMessageBus
    {
        public override async Task<IReadOnlyList<ClusterSubscriptionDescriptor>> FetchPeerSubscriptionsAsync(Uri peerEndpoint, Guid channelKey, CancellationToken cancellationToken = default)
        {
            try
            {
                var response = await SendRequestAsync(
                    peerEndpoint, ActiveMqClusterRequestKind.FetchSubscriptions, null, channelKey, null, cancellationToken)
                    .ConfigureAwait(false);

                return response.PayloadJson?.FromNJson<ClusterSubscriptionDescriptor[]>() ?? [];
            }
            catch (Exception exception) when (exception is TimeoutException or InvalidOperationException)
            {
                Log.FetchPeerSubscriptionsFailed(_logger, exception, peerEndpoint.Host);
                return [];
            }
        }

        /// <summary>Answering side of <see cref="FetchPeerSubscriptionsAsync"/>: mirrors <c>ClusterSubscriptionEndpoints</c>'s <c>/self</c> handler.</summary>
        private ActiveMqClusterResponseEnvelope BuildFetchSubscriptionsResponse(ActiveMqClusterRequestEnvelope request)
        {
            var channel = _channelResolver.GetChannel(request.ChannelKey!.Value);
            var descriptors = GetLocalClusterSubscriptionDescriptors(channel);

            return new ActiveMqClusterResponseEnvelope(request.CorrelationId, true, null, descriptors.ToNJson());
        }

        private static partial class Log
        {
            [LoggerMessage(EventId = 91050, Level = LogLevel.Warning,
                Message = "[Cluster] Fetching subscriptions from ActiveMQ peer '{Host}' failed.")]
            public static partial void FetchPeerSubscriptionsFailed(ILogger logger, Exception exception, string host);
        }
    }
}
