using System.Collections.Concurrent;
using System.Net.Http;
using System.Text;
using Microsoft.Extensions.Logging;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.BuildingBlocks.Application.Helpers;

namespace ThunderPropagator.ClusterMessageBuses.WebApi
{
    internal sealed partial class WebApiClusterMessageBus
    {
        public override async Task PublishAsync(Guid channelKey, ClusterFanOutMessage message, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var stamped = message with { OriginId = _selfId };
            var body = stamped.ToNJson();

            var peers = await _discovery.GetPeersAsync(cancellationToken).ConfigureAwait(false);

            await Task.WhenAll(peers.Select(async peer =>
            {
                try
                {
                    var url = WebApiClusterRouting.PeerFanOutUrl(peer.Endpoint, _options.ListenPath, channelKey);
                    using var request = new HttpRequestMessage(HttpMethod.Post, url)
                    {
                        Content = new StringContent(body, Encoding.UTF8, "application/json"),
                    };

                    using var response = await _httpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
                    if (!response.IsSuccessStatusCode)
                        Log.FanOutSendToPeerRejected(_logger, peer.Endpoint.Host, (int)response.StatusCode);
                }
                catch (Exception exception)
                {
                    // One unreachable peer must not fail the whole fan-out — mirrors
                    // HttpClusterMessageBus's per-peer try/catch around its parallel POSTs.
                    Log.FanOutSendToPeerFailed(_logger, exception, peer.Endpoint.Host);
                }
            })).ConfigureAwait(false);
        }

        public override Task<IAsyncDisposable> SubscribeAsync(Guid channelKey, Func<ClusterFanOutMessage, CancellationToken, Task> onMessage, CancellationToken cancellationToken = default)
        {
            _fanOutHandlers[channelKey] = onMessage;
            return Task.FromResult<IAsyncDisposable>(new FanOutSubscription(_fanOutHandlers, channelKey));
        }

        /// <summary>Internal (rather than private) so tests can drive it directly with a raw payload.</summary>
        internal async Task HandleFanOutDeliveryAsync(string payload, Guid channelKey, CancellationToken cancellationToken)
        {
            ClusterFanOutMessage? message;
            try
            {
                message = payload.FromNJson<ClusterFanOutMessage>();
            }
            catch (Exception exception)
            {
                Log.FanOutMessageUnparseable(_logger, exception);
                return;
            }

            if (message is null || message.OriginId == _selfId)
                return;

            if (!_fanOutHandlers.TryGetValue(channelKey, out var handler))
                return;

            try
            {
                await handler(message, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Log.FanOutHandlerFaulted(_logger, exception);
            }
        }

        private sealed class FanOutSubscription : IAsyncDisposable
        {
            private readonly ConcurrentDictionary<Guid, Func<ClusterFanOutMessage, CancellationToken, Task>> _handlers;
            private readonly Guid _channelKey;

            internal FanOutSubscription(ConcurrentDictionary<Guid, Func<ClusterFanOutMessage, CancellationToken, Task>> handlers, Guid channelKey)
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
            [LoggerMessage(EventId = 91320, Level = LogLevel.Warning,
                Message = "[Cluster] WebApi fan-out POST to peer '{Host}' was rejected with status {StatusCode}.")]
            public static partial void FanOutSendToPeerRejected(ILogger logger, string host, int statusCode);

            [LoggerMessage(EventId = 91321, Level = LogLevel.Warning,
                Message = "[Cluster] WebApi fan-out send to peer '{Host}' failed.")]
            public static partial void FanOutSendToPeerFailed(ILogger logger, Exception exception, string host);

            [LoggerMessage(EventId = 91322, Level = LogLevel.Warning,
                Message = "[Cluster] WebApi fan-out message could not be parsed; skipping it.")]
            public static partial void FanOutMessageUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91323, Level = LogLevel.Error,
                Message = "[Cluster] WebApi fan-out handler faulted.")]
            public static partial void FanOutHandlerFaulted(ILogger logger, Exception exception);
        }
    }
}
