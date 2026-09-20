using System.Net.Http;
using System.Text;
using Microsoft.Extensions.Logging;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.BuildingBlocks.Application.Helpers;

namespace ThunderPropagator.ClusterMessageBuses.WebApi
{
    /// <summary>
    /// Byte-oriented counterpart to <c>WebApiClusterMessageBus.FanOut.cs</c> /
    /// <c>.Snapshots.cs</c> -- see <see cref="ClusterByteMessage"/>'s own doc comment for why this
    /// surface exists alongside, not instead of, the <c>ClusterFanOutMessage</c>-based one. Routed
    /// under <c>bytefanout/{channelKey}</c> (POST) and <c>bytesnapshot/{channelKey}</c> (GET) --
    /// separate route segments from <c>fanout</c>/<c>snapshot</c> rather than an overload of them,
    /// since the two payload shapes need different wire handling (raw base64 bytes vs. a
    /// <c>SnapshotEntry[]</c>/channel-name route) even though both ultimately move bytes over HTTP.
    /// </summary>
    internal sealed partial class WebApiClusterMessageBus
    {
        public override async Task PublishAsync(Guid channelKey, ClusterByteMessage message, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var stamped = message with { OriginId = _selfId };
            var body = stamped.ToNJson();

            var peers = await _discovery.GetPeersAsync(cancellationToken).ConfigureAwait(false);

            await Task.WhenAll(peers.Select(async peer =>
            {
                try
                {
                    var url = WebApiClusterRouting.PeerByteFanOutUrl(peer.Endpoint, _options.ListenPath, channelKey);
                    using var request = new HttpRequestMessage(HttpMethod.Post, url)
                    {
                        Content = new StringContent(body, Encoding.UTF8, "application/json"),
                    };

                    using var response = await _httpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
                    if (!response.IsSuccessStatusCode)
                        Log.ByteFanOutSendToPeerRejected(_logger, peer.Endpoint.Host, (int)response.StatusCode);
                }
                catch (Exception exception)
                {
                    // One unreachable peer must not fail the whole fan-out -- same rationale as the
                    // ClusterFanOutMessage overload in WebApiClusterMessageBus.FanOut.cs.
                    Log.ByteFanOutSendToPeerFailed(_logger, exception, peer.Endpoint.Host);
                }
            })).ConfigureAwait(false);
        }

        public override Task<IAsyncDisposable> SubscribeAsync(Guid channelKey, Func<ClusterByteMessage, CancellationToken, Task> onMessage, CancellationToken cancellationToken = default)
        {
            return Task.FromResult(_byteFanOutHandlers.Register(channelKey, onMessage));
        }

        /// <summary>Internal (rather than private) so tests can drive it directly with a raw payload.</summary>
        internal async Task HandleByteFanOutDeliveryAsync(string payload, Guid channelKey, CancellationToken cancellationToken)
        {
            ClusterByteMessage? message;
            try
            {
                message = payload.FromNJson<ClusterByteMessage>();
            }
            catch (Exception exception)
            {
                Log.ByteFanOutMessageUnparseable(_logger, exception);
                return;
            }

            if (message is null || message.OriginId == _selfId)
                return;

            try
            {
                await _byteFanOutHandlers.InvokeAsync(channelKey, message, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Log.ByteFanOutHandlerFaulted(_logger, exception);
            }
        }

        public override async Task<byte[]?> PullSnapshotAsync(Uri leaderEndpoint, Guid channelKey, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var url = WebApiClusterRouting.PeerByteSnapshotUrl(leaderEndpoint, _options.ListenPath, channelKey);
            using var request = new HttpRequestMessage(HttpMethod.Get, url);
            using var response = await _httpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
            response.EnsureSuccessStatusCode();

            var body = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
            return body.FromNJson<byte[]?>();
        }

        /// <summary>
        /// Answering side of <see cref="PullSnapshotAsync"/>. Returns a JSON-null body (serialized
        /// "null") when no <see cref="IClusterByteSnapshotProvider"/> is registered, or it has
        /// nothing to offer yet -- mirrored by <see cref="PullSnapshotAsync"/> deserializing that
        /// back to a null byte[].
        /// </summary>
        internal async Task<string> BuildByteSnapshotResponseBodyAsync(Guid channelKey, CancellationToken cancellationToken)
        {
            if (_byteSnapshotProvider is null)
                return "null";

            var snapshot = await _byteSnapshotProvider.GetSnapshotAsync(channelKey, cancellationToken).ConfigureAwait(false);
            return snapshot.ToNJson();
        }

        private static partial class Log
        {
            [LoggerMessage(EventId = 91350, Level = LogLevel.Warning,
                Message = "[Cluster] WebApi byte fan-out POST to peer '{Host}' was rejected with status {StatusCode}.")]
            public static partial void ByteFanOutSendToPeerRejected(ILogger logger, string host, int statusCode);

            [LoggerMessage(EventId = 91351, Level = LogLevel.Warning,
                Message = "[Cluster] WebApi byte fan-out send to peer '{Host}' failed.")]
            public static partial void ByteFanOutSendToPeerFailed(ILogger logger, Exception exception, string host);

            [LoggerMessage(EventId = 91352, Level = LogLevel.Warning,
                Message = "[Cluster] WebApi byte fan-out message could not be parsed; skipping it.")]
            public static partial void ByteFanOutMessageUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91353, Level = LogLevel.Error,
                Message = "[Cluster] WebApi byte fan-out handler faulted.")]
            public static partial void ByteFanOutHandlerFaulted(ILogger logger, Exception exception);
        }
    }
}
