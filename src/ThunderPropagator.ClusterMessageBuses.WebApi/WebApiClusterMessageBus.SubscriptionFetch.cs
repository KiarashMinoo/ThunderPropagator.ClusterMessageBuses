using System.Net.Http;
using Microsoft.Extensions.Logging;
using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.BuildingBlocks.Application.Helpers;

namespace ThunderPropagator.ClusterMessageBuses.WebApi
{
    internal sealed partial class WebApiClusterMessageBus
    {
        public override async Task<IReadOnlyList<ClusterSubscriptionDescriptor>> FetchPeerSubscriptionsAsync(Uri peerEndpoint, Guid channelKey, CancellationToken cancellationToken = default)
        {
            try
            {
                var url = WebApiClusterRouting.PeerSubscriptionsSelfUrl(peerEndpoint, _options.ListenPath, channelKey);
                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                using var response = await _httpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);

                if (!response.IsSuccessStatusCode)
                {
                    Log.FetchPeerSubscriptionsRejected(_logger, peerEndpoint.Host, (int)response.StatusCode);
                    return [];
                }

                var body = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
                return body.FromNJson<ClusterSubscriptionDescriptor[]>() ?? [];
            }
            catch (Exception exception)
            {
                Log.FetchPeerSubscriptionsFailed(_logger, exception, peerEndpoint.Host);
                return [];
            }
        }

        /// <summary>Answering side of <see cref="FetchPeerSubscriptionsAsync"/>: mirrors core's own <c>ClusterSubscriptionEndpoints</c>'s <c>/self</c> handler.</summary>
        internal string BuildSubscriptionsSelfResponseBody(Guid channelKey)
        {
            var channel = _channelResolver.GetChannel(channelKey);
            var descriptors = GetLocalClusterSubscriptionDescriptors(channel);

            return descriptors.ToNJson();
        }

        private static partial class Log
        {
            [LoggerMessage(EventId = 91350, Level = LogLevel.Warning,
                Message = "[Cluster] Fetching subscriptions from WebApi peer '{Host}' was rejected with status {StatusCode}.")]
            public static partial void FetchPeerSubscriptionsRejected(ILogger logger, string host, int statusCode);

            [LoggerMessage(EventId = 91351, Level = LogLevel.Warning,
                Message = "[Cluster] Fetching subscriptions from WebApi peer '{Host}' failed.")]
            public static partial void FetchPeerSubscriptionsFailed(ILogger logger, Exception exception, string host);
        }
    }
}
