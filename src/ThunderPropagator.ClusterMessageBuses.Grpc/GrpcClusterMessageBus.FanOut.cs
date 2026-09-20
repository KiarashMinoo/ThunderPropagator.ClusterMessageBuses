using Microsoft.Extensions.Logging;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.Grpc.Protos;

namespace ThunderPropagator.ClusterMessageBuses.Grpc
{
    internal sealed partial class GrpcClusterMessageBus
    {
        public override async Task PublishAsync(Guid channelKey, ClusterFanOutMessage message, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var stamped = message with { OriginId = _selfId };
            var batch = new PushedMessageBatch { ChannelKey = channelKey.ToString(), PayloadJson = stamped.ToNJson() };

            var peers = await _discovery.GetPeersAsync(cancellationToken).ConfigureAwait(false);

            await Task.WhenAll(peers.Select(async peer =>
            {
                try
                {
                    var connection = await GetOrCreateOutboundConnectionAsync(peer.Endpoint, cancellationToken).ConfigureAwait(false);
                    await _resiliencePipeline.ExecuteAsync(
                        async ct => await connection.SendFanOutAsync(batch, ct).ConfigureAwait(false),
                        cancellationToken).ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    // One unreachable peer must not fail the whole fan-out — mirrors every other
                    // direct-peer transport's per-peer try/catch around its parallel sends.
                    Log.FanOutSendToPeerFailed(_logger, exception, peer.Endpoint.Host);
                }
            })).ConfigureAwait(false);
        }

        public override Task<IAsyncDisposable> SubscribeAsync(Guid channelKey, Func<ClusterFanOutMessage, CancellationToken, Task> onMessage, CancellationToken cancellationToken = default)
        {
            return Task.FromResult(_fanOutHandlers.Register(channelKey, onMessage));
        }

        /// <summary>Internal (rather than private) so tests can drive it directly with a raw batch, and so <c>ClusterFanOutGrpcService</c> can dispatch inbound deliveries into it.</summary>
        internal async Task HandleFanOutDeliveryAsync(PushedMessageBatch batch, CancellationToken cancellationToken)
        {
            if (!Guid.TryParse(batch.ChannelKey, out var channelKey))
            {
                Log.FanOutMessageUnparseable(_logger, new FormatException($"'{batch.ChannelKey}' is not a valid channel key."));
                return;
            }

            ClusterFanOutMessage? message;
            try
            {
                message = batch.PayloadJson.FromNJson<ClusterFanOutMessage>();
            }
            catch (Exception exception)
            {
                // A single malformed delivery must not take down the peer stream — log and move on.
                Log.FanOutMessageUnparseable(_logger, exception);
                return;
            }

            if (message is null || message.OriginId == _selfId)
                return;

            try
            {
                await _fanOutHandlers.InvokeAsync(channelKey, message, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Log.FanOutHandlerFaulted(_logger, exception);
            }
        }

        private static partial class Log
        {
            [LoggerMessage(EventId = 91410, Level = LogLevel.Warning,
                Message = "[Cluster] gRPC fan-out send to peer '{Host}' failed.")]
            public static partial void FanOutSendToPeerFailed(ILogger logger, Exception exception, string host);

            [LoggerMessage(EventId = 91411, Level = LogLevel.Warning,
                Message = "[Cluster] gRPC fan-out message could not be parsed; skipping it.")]
            public static partial void FanOutMessageUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91412, Level = LogLevel.Error,
                Message = "[Cluster] gRPC fan-out handler faulted.")]
            public static partial void FanOutHandlerFaulted(ILogger logger, Exception exception);
        }
    }
}
