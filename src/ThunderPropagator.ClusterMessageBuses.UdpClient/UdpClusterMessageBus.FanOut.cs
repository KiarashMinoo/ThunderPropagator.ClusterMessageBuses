using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.BuildingBlocks.Application.Helpers;

namespace ThunderPropagator.ClusterMessageBuses.UdpClient
{
    internal sealed partial class UdpClusterMessageBus
    {
        public override async Task PublishAsync(Guid channelKey, ClusterFanOutMessage message, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var stamped = message with { OriginId = _selfId };
            var frame = new UdpClusterFrame(UdpClusterFrameKind.FanOut, new UdpFanOutPayload(channelKey, stamped).ToNJson());

            var peers = await _discovery.GetPeersAsync(cancellationToken).ConfigureAwait(false);

            await Task.WhenAll(peers.Select(async peer =>
            {
                try
                {
                    var remoteEndpoint = await _peerEndpointResolver(peer.Endpoint, cancellationToken).ConfigureAwait(false);
                    await SendFrameAsync(frame, remoteEndpoint, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    // One unreachable/unresolvable peer must not fail the whole fan-out — and unlike
                    // every other transport, even a "successful" send here is only best-effort: UDP
                    // gives no delivery guarantee at all.
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
        internal async Task HandleFanOutDeliveryAsync(string payload, CancellationToken cancellationToken)
        {
            UdpFanOutPayload? fanOut;
            try
            {
                fanOut = payload.FromNJson<UdpFanOutPayload>();
            }
            catch (Exception exception)
            {
                Log.FanOutMessageUnparseable(_logger, exception);
                return;
            }

            if (fanOut is null || fanOut.Message.OriginId == _selfId)
                return;

            if (!_fanOutHandlers.TryGetValue(fanOut.ChannelKey, out var handler))
                return;

            try
            {
                await handler(fanOut.Message, cancellationToken).ConfigureAwait(false);
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
            [LoggerMessage(EventId = 91510, Level = LogLevel.Warning,
                Message = "[Cluster] UDP fan-out send to peer '{Host}' failed.")]
            public static partial void FanOutSendToPeerFailed(ILogger logger, Exception exception, string host);

            [LoggerMessage(EventId = 91511, Level = LogLevel.Warning,
                Message = "[Cluster] UDP fan-out message could not be parsed; skipping it.")]
            public static partial void FanOutMessageUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91512, Level = LogLevel.Error,
                Message = "[Cluster] UDP fan-out handler faulted.")]
            public static partial void FanOutHandlerFaulted(ILogger logger, Exception exception);
        }
    }
}
