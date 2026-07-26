using System.Collections.Concurrent;
using System.Net.Http;
using System.Text;
using Microsoft.Extensions.Logging;
using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.BuildingBlocks.Application.Helpers;

namespace ThunderPropagator.ClusterMessageBuses.WebApi
{
    internal sealed partial class WebApiClusterMessageBus
    {
        public override async Task PublishAsync(Guid channelKey, ClusterSubscriptionEvent subscriptionEvent, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var stamped = subscriptionEvent with { OriginId = _selfId };
            var body = stamped.ToNJson();

            var peers = await _discovery.GetPeersAsync(cancellationToken).ConfigureAwait(false);

            await Task.WhenAll(peers.Select(async peer =>
            {
                try
                {
                    var url = WebApiClusterRouting.PeerSubscriptionEventUrl(peer.Endpoint, _options.ListenPath, channelKey);
                    using var request = new HttpRequestMessage(HttpMethod.Post, url)
                    {
                        Content = new StringContent(body, Encoding.UTF8, "application/json"),
                    };

                    using var response = await _httpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
                    if (!response.IsSuccessStatusCode)
                        Log.SubscriptionEventSendToPeerRejected(_logger, peer.Endpoint.Host, (int)response.StatusCode);
                }
                catch (Exception exception)
                {
                    Log.SubscriptionEventSendToPeerFailed(_logger, exception, peer.Endpoint.Host);
                }
            })).ConfigureAwait(false);
        }

        public override Task<IAsyncDisposable> SubscribeAsync(Guid channelKey, Func<ClusterSubscriptionEvent, CancellationToken, Task> onEvent, CancellationToken cancellationToken = default)
        {
            _subscriptionEventHandlers[channelKey] = onEvent;
            return Task.FromResult<IAsyncDisposable>(new SubscriptionEventSubscription(_subscriptionEventHandlers, channelKey));
        }

        /// <summary>Internal (rather than private) so tests can drive it directly with a raw payload.</summary>
        internal async Task HandleSubscriptionEventDeliveryAsync(string payload, Guid channelKey, CancellationToken cancellationToken)
        {
            ClusterSubscriptionEvent? subscriptionEvent;
            try
            {
                subscriptionEvent = payload.FromNJson<ClusterSubscriptionEvent>();
            }
            catch (Exception exception)
            {
                Log.SubscriptionEventUnparseable(_logger, exception);
                return;
            }

            if (subscriptionEvent is null || subscriptionEvent.OriginId == _selfId)
                return;

            if (!_subscriptionEventHandlers.TryGetValue(channelKey, out var handler))
                return;

            try
            {
                await handler(subscriptionEvent, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Log.SubscriptionEventHandlerFaulted(_logger, exception);
            }
        }

        private sealed class SubscriptionEventSubscription : IAsyncDisposable
        {
            private readonly ConcurrentDictionary<Guid, Func<ClusterSubscriptionEvent, CancellationToken, Task>> _handlers;
            private readonly Guid _channelKey;

            internal SubscriptionEventSubscription(ConcurrentDictionary<Guid, Func<ClusterSubscriptionEvent, CancellationToken, Task>> handlers, Guid channelKey)
            {
                _handlers = handlers;
                _channelKey = channelKey;
            }

            public ValueTask DisposeAsync()
            {
                _handlers.TryRemove(_channelKey, out _);
                return ValueTask.CompletedTask;
            }
        }

        private static partial class Log
        {
            [LoggerMessage(EventId = 91330, Level = LogLevel.Warning,
                Message = "[Cluster] WebApi subscription-event POST to peer '{Host}' was rejected with status {StatusCode}.")]
            public static partial void SubscriptionEventSendToPeerRejected(ILogger logger, string host, int statusCode);

            [LoggerMessage(EventId = 91331, Level = LogLevel.Warning,
                Message = "[Cluster] WebApi subscription-event send to peer '{Host}' failed.")]
            public static partial void SubscriptionEventSendToPeerFailed(ILogger logger, Exception exception, string host);

            [LoggerMessage(EventId = 91332, Level = LogLevel.Warning,
                Message = "[Cluster] WebApi subscription event could not be parsed; skipping it.")]
            public static partial void SubscriptionEventUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91333, Level = LogLevel.Error,
                Message = "[Cluster] WebApi subscription-event handler faulted.")]
            public static partial void SubscriptionEventHandlerFaulted(ILogger logger, Exception exception);
        }
    }
}
