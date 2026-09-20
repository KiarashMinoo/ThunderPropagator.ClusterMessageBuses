using Microsoft.Extensions.Logging;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.UdpClient
{
    internal sealed partial class UdpClusterMessageBus
    {
        public override async Task PublishAsync(Guid channelKey, ClusterFanOutMessage message, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var stamped = message with { OriginId = _selfId };
            var frame = new ClusterFrame(ClusterFrameKind.FanOut, new ClusterChannelEnvelope<ClusterFanOutMessage>(channelKey, stamped).ToNJson());

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
            return Task.FromResult(_fanOutHandlers.Register(channelKey, onMessage));
        }

        /// <summary>Internal (rather than private) so tests can drive it directly with a raw payload.</summary>
        internal async Task HandleFanOutDeliveryAsync(string payload, CancellationToken cancellationToken)
        {
            ClusterChannelEnvelope<ClusterFanOutMessage>? fanOut;
            try
            {
                fanOut = payload.FromNJson<ClusterChannelEnvelope<ClusterFanOutMessage>>();
            }
            catch (Exception exception)
            {
                Log.FanOutMessageUnparseable(_logger, exception);
                return;
            }

            if (fanOut is null || fanOut.Message.OriginId == _selfId)
                return;

            try
            {
                await _fanOutHandlers.InvokeAsync(fanOut.ChannelKey, fanOut.Message, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Log.FanOutHandlerFaulted(_logger, exception);
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
