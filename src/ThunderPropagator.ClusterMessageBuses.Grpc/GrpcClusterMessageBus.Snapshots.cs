using Grpc.Core;
using Microsoft.Extensions.Logging;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Channels.Snapshots;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.Grpc.Protos;

namespace ThunderPropagator.ClusterMessageBuses.Grpc
{
    internal sealed partial class GrpcClusterMessageBus
    {
        public override async Task RestoreFromLeaderAsync(Uri leaderEndpoint, IChannel channel, CancellationToken cancellationToken = default)
        {
            Log.RestoringChannel(_logger, channel.Metadata.ChannelName, leaderEndpoint.Host);

            var clients = await _unaryClientsFactory(leaderEndpoint, cancellationToken).ConfigureAwait(false);
            var request = new RestoreSnapshotRequest { ChannelName = channel.Metadata.ChannelName };

            var response = await CallClusterUnaryAsync(
                (deadline, ct) => clients.Snapshot.RestoreSnapshotAsync(request, deadline: deadline, cancellationToken: ct),
                cancellationToken).ConfigureAwait(false);

            if (!response.Success)
                throw new InvalidOperationException(response.ErrorMessage);

            var entries = response.EntriesJson.FromNJson<SnapshotEntry[]>() ?? [];

            var count = 0;
            foreach (var entry in entries.Where(e => e.State == SnapshotEntryState.Active))
            {
                OverwriteSnapshot(channel, entry);
                count++;
            }

            Log.RestoredChannel(_logger, count, channel.Metadata.ChannelName);
        }

        public override async Task SyncDeltaFromLeaderAsync(Uri leaderEndpoint, IChannel channel, DateTimeOffset since, CancellationToken cancellationToken = default)
        {
            var clients = await _unaryClientsFactory(leaderEndpoint, cancellationToken).ConfigureAwait(false);
            var request = new SyncDeltaRequest { ChannelName = channel.Metadata.ChannelName, SinceTicks = since.UtcTicks };

            var response = await CallClusterUnaryAsync(
                (deadline, ct) => clients.Snapshot.SyncDeltaAsync(request, deadline: deadline, cancellationToken: ct),
                cancellationToken).ConfigureAwait(false);

            if (!response.Success)
                throw new InvalidOperationException(response.ErrorMessage);

            var updatedEntries = response.UpdatedEntriesJson.FromNJson<SnapshotEntry[]>() ?? [];

            var updatedCount = 0;
            foreach (var entry in updatedEntries.Where(e => e.State == SnapshotEntryState.Active))
            {
                OverwriteSnapshot(channel, entry);
                updatedCount++;
            }

            var deletedCount = 0;
            foreach (var hashKey in response.DeletedHashKeys)
            {
                DeleteSnapshot(channel, hashKey);
                deletedCount++;
            }

            if (updatedCount > 0 || deletedCount > 0)
                Log.DeltaSyncApplied(_logger, updatedCount, deletedCount, channel.Metadata.ChannelName);
        }

        /// <summary>Answering side of <see cref="RestoreFromLeaderAsync"/>.</summary>
        internal async Task<RestoreSnapshotResponse> BuildRestoreSnapshotResponseAsync(RestoreSnapshotRequest request, CancellationToken cancellationToken)
        {
            try
            {
                var channel = _channelResolver.GetChannel(request.ChannelName);
                var entries = await channel.SearchSnapshotsAsync(e => e.State == SnapshotEntryState.Active, 0, 0, cancellationToken).ConfigureAwait(false);

                return new RestoreSnapshotResponse { Success = true, EntriesJson = entries.ToNJson() };
            }
            catch (Exception exception)
            {
                return new RestoreSnapshotResponse { Success = false, ErrorMessage = exception.Message };
            }
        }

        /// <summary>Answering side of <see cref="SyncDeltaFromLeaderAsync"/>.</summary>
        internal async Task<SyncDeltaResponse> BuildSyncDeltaResponseAsync(SyncDeltaRequest request, CancellationToken cancellationToken)
        {
            try
            {
                var since = new DateTimeOffset(request.SinceTicks, TimeSpan.Zero);
                var channel = _channelResolver.GetChannel(request.ChannelName);

                var updatedEntries = await channel.SearchSnapshotsAsync(
                    e => e.State == SnapshotEntryState.Active && e.LastModified >= since, 0, 0, cancellationToken).ConfigureAwait(false);
                var deletedHashKeys = GetSnapshotTombstonesSince(channel, since);

                var response = new SyncDeltaResponse { Success = true, UpdatedEntriesJson = updatedEntries.ToNJson() };
                response.DeletedHashKeys.AddRange(deletedHashKeys);
                return response;
            }
            catch (Exception exception)
            {
                return new SyncDeltaResponse { Success = false, ErrorMessage = exception.Message };
            }
        }

        /// <summary>
        /// Applies the shared deadline + <c>ResiliencePipeline</c> wrapping, and translates a
        /// deadline-exceeded/unavailable-peer <see cref="RpcException"/> into a <see cref="TimeoutException"/>
        /// so callers see the same outward contract every other transport in this repo uses for an
        /// unresponsive leader/peer, rather than a gRPC-specific exception type.
        /// </summary>
        private async Task<TResponse> CallClusterUnaryAsync<TResponse>(
            Func<DateTime, CancellationToken, AsyncUnaryCall<TResponse>> call,
            CancellationToken cancellationToken)
        {
            var deadline = DateTime.UtcNow.Add(_options.RequestTimeout);

            try
            {
                return await _resiliencePipeline.ExecuteAsync(
                    async ct => await call(deadline, ct).ResponseAsync.ConfigureAwait(false),
                    cancellationToken).ConfigureAwait(false);
            }
            catch (RpcException exception) when (exception.StatusCode is StatusCode.DeadlineExceeded or StatusCode.Unavailable)
            {
                throw new TimeoutException($"gRPC call to the peer timed out or the peer was unavailable: {exception.Status.Detail}", exception);
            }
        }

        private static partial class Log
        {
            [LoggerMessage(EventId = 91430, Level = LogLevel.Information,
                Message = "[Cluster] gRPC restoring channel '{Channel}' from leader host '{Host}'.")]
            public static partial void RestoringChannel(ILogger logger, string channel, string host);

            [LoggerMessage(EventId = 91431, Level = LogLevel.Information,
                Message = "[Cluster] gRPC restored {Count} snapshot entries for channel '{Channel}'.")]
            public static partial void RestoredChannel(ILogger logger, int count, string channel);

            [LoggerMessage(EventId = 91432, Level = LogLevel.Debug,
                Message = "[Cluster] gRPC delta sync applied {Updated} updates and {Deleted} deletions for channel '{Channel}'.")]
            public static partial void DeltaSyncApplied(ILogger logger, int updated, int deleted, string channel);
        }
    }
}
