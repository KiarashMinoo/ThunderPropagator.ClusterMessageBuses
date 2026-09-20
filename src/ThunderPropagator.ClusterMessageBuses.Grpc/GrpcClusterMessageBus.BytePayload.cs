using Microsoft.Extensions.Logging;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.Grpc.Protos;

namespace ThunderPropagator.ClusterMessageBuses.Grpc
{
    /// <summary>
    /// Byte-oriented counterpart to <c>GrpcClusterMessageBus.FanOut.cs</c> / <c>.Snapshots.cs</c> --
    /// see <see cref="ClusterByteMessage"/>'s own doc comment for why this surface exists alongside,
    /// not instead of, the <c>ClusterFanOutMessage</c>-based one.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Approach taken, and why:</b> <c>Protos/cluster.proto</c> was extended directly with a new
    /// <c>ClusterByteFanOut</c> service (mirroring <c>ClusterFanOut</c>'s one-duplex-stream-per-peer
    /// shape, carrying a new <see cref="PushedByteMessageBatch"/> message) and a new
    /// <c>PullSnapshotBytes</c> unary RPC added to the existing <c>ClusterSnapshot</c> service
    /// (mirroring <c>RestoreSnapshot</c>/<c>SyncDelta</c>'s unary request/response shape). This is
    /// the "ideally a new RPC method on the same service definition" option: Grpc.Tools regenerates
    /// the C# client/server stubs from this .proto automatically on the next <c>dotnet build</c> (the
    /// project's own <c>&lt;Protobuf Include="Protos\cluster.proto" GrpcServices="Both" /&gt;</c> item
    /// already covers it) — editing the .proto is the normal, safe way to extend a proto-first gRPC
    /// contract, not something that needs a compiler available in this editing session to verify.
    /// No existing message/service/rpc was renamed, removed, or had a field renumbered, so every
    /// existing generated type/caller is unaffected.
    /// </para>
    /// <para>
    /// Fan-out uses its own long-lived duplex stream per peer (<see cref="GrpcPeerConnection.ByteFanOutCall"/>),
    /// opened alongside the existing fan-out/subscription-sync streams in
    /// <c>DefaultPeerConnectionFactoryAsync</c> and served by the new
    /// <c>Services.ClusterByteFanOutGrpcService</c> — exactly the same "one call per direction"
    /// design as <c>ClusterFanOutGrpcService</c>. The snapshot pull is a plain unary RPC against the
    /// existing <see cref="GrpcUnaryClients.Snapshot"/> client, needing no hand-rolled correlation-id
    /// scheme at all, exactly like <c>RestoreFromLeaderAsync</c>.
    /// </para>
    /// </remarks>
    internal sealed partial class GrpcClusterMessageBus
    {
        /// <inheritdoc />
        public override async Task PublishAsync(Guid channelKey, ClusterByteMessage message, CancellationToken cancellationToken = default)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var stamped = message with { OriginId = _selfId };
            var batch = new PushedByteMessageBatch { ChannelKey = channelKey.ToString(), PayloadJson = stamped.ToNJson() };

            var peers = await _discovery.GetPeersAsync(cancellationToken).ConfigureAwait(false);

            await Task.WhenAll(peers.Select(async peer =>
            {
                try
                {
                    var connection = await GetOrCreateOutboundConnectionAsync(peer.Endpoint, cancellationToken).ConfigureAwait(false);
                    await _resiliencePipeline.ExecuteAsync(
                        async ct => await connection.SendByteFanOutAsync(batch, ct).ConfigureAwait(false),
                        cancellationToken).ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    // One unreachable peer must not fail the whole fan-out -- mirrors the
                    // ClusterFanOutMessage overload in GrpcClusterMessageBus.FanOut.cs.
                    Log.ByteFanOutSendToPeerFailed(_logger, exception, peer.Endpoint.Host);
                }
            })).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public override Task<IAsyncDisposable> SubscribeAsync(Guid channelKey, Func<ClusterByteMessage, CancellationToken, Task> onMessage, CancellationToken cancellationToken = default)
        {
            return Task.FromResult(_byteFanOutHandlers.Register(channelKey, onMessage));
        }

        /// <summary>Internal (rather than private) so tests can drive it directly with a raw batch, and so <c>ClusterByteFanOutGrpcService</c> can dispatch inbound deliveries into it.</summary>
        internal async Task HandleByteFanOutDeliveryAsync(PushedByteMessageBatch batch, CancellationToken cancellationToken)
        {
            if (!Guid.TryParse(batch.ChannelKey, out var channelKey))
            {
                Log.ByteFanOutMessageUnparseable(_logger, new FormatException($"'{batch.ChannelKey}' is not a valid channel key."));
                return;
            }

            ClusterByteMessage? message;
            try
            {
                message = batch.PayloadJson.FromNJson<ClusterByteMessage>();
            }
            catch (Exception exception)
            {
                // A single malformed delivery must not take down the peer stream -- log and move on.
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

        /// <inheritdoc />
        public override async Task<byte[]?> PullSnapshotAsync(Uri leaderEndpoint, Guid channelKey, CancellationToken cancellationToken = default)
        {
            var clients = await _unaryClientsFactory(leaderEndpoint, cancellationToken).ConfigureAwait(false);
            var request = new PullSnapshotBytesRequest { ChannelKey = channelKey.ToString() };

            var response = await CallClusterUnaryAsync(
                (deadline, ct) => clients.Snapshot.PullSnapshotBytesAsync(request, deadline: deadline, cancellationToken: ct),
                cancellationToken).ConfigureAwait(false);

            if (!response.Success)
                throw new InvalidOperationException(response.ErrorMessage);

            return response.SnapshotJson?.FromNJson<byte[]?>();
        }

        /// <summary>
        /// Answering side of <see cref="PullSnapshotAsync"/>. Returns a successful response with a
        /// JSON-null <see cref="PullSnapshotBytesResponse.SnapshotJson"/> when no
        /// <see cref="IClusterByteSnapshotProvider"/> is registered, or it has nothing to offer yet --
        /// mirrored by <see cref="PullSnapshotAsync"/> deserializing that back to a null byte[],
        /// exactly like <c>BuildRestoreSnapshotResponseAsync</c> answers with an empty array rather
        /// than a failure when a channel has nothing to restore.
        /// </summary>
        internal async Task<PullSnapshotBytesResponse> BuildPullSnapshotBytesResponseAsync(PullSnapshotBytesRequest request, CancellationToken cancellationToken)
        {
            try
            {
                if (!Guid.TryParse(request.ChannelKey, out var channelKey))
                    throw new FormatException($"'{request.ChannelKey}' is not a valid channel key.");

                if (_byteSnapshotProvider is null)
                    return new PullSnapshotBytesResponse { Success = true, SnapshotJson = "null" };

                var snapshot = await _byteSnapshotProvider.GetSnapshotAsync(channelKey, cancellationToken).ConfigureAwait(false);
                return new PullSnapshotBytesResponse { Success = true, SnapshotJson = snapshot.ToNJson() };
            }
            catch (Exception exception)
            {
                return new PullSnapshotBytesResponse { Success = false, ErrorMessage = exception.Message };
            }
        }

        private static partial class Log
        {
            [LoggerMessage(EventId = 91440, Level = LogLevel.Warning,
                Message = "[Cluster] gRPC byte fan-out send to peer '{Host}' failed.")]
            public static partial void ByteFanOutSendToPeerFailed(ILogger logger, Exception exception, string host);

            [LoggerMessage(EventId = 91441, Level = LogLevel.Warning,
                Message = "[Cluster] gRPC byte fan-out message could not be parsed; skipping it.")]
            public static partial void ByteFanOutMessageUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91442, Level = LogLevel.Error,
                Message = "[Cluster] gRPC byte fan-out handler faulted.")]
            public static partial void ByteFanOutHandlerFaulted(ILogger logger, Exception exception);
        }
    }
}
