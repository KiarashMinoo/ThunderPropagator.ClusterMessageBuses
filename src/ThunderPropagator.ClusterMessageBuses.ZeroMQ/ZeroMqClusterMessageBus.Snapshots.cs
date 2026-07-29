using Microsoft.Extensions.Logging;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Channels.Snapshots;
using ThunderPropagator.BuildingBlocks.Application.Helpers;

namespace ThunderPropagator.ClusterMessageBuses.ZeroMQ
{
    internal sealed partial class ZeroMqClusterMessageBus
    {
        public override async Task RestoreFromLeaderAsync(Uri leaderEndpoint, IChannel channel, CancellationToken cancellationToken = default)
        {
            Log.RestoringChannel(_logger, channel.Metadata.ChannelName, leaderEndpoint.Host);

            var response = await SendRequestAsync(
                leaderEndpoint, ZeroMqClusterRequestKind.RestoreSnapshot, channel.Metadata.ChannelName, null, null, cancellationToken)
                .ConfigureAwait(false);

            var entries = response.PayloadJson?.FromNJson<SnapshotEntry[]>() ?? [];

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
            var response = await SendRequestAsync(
                leaderEndpoint, ZeroMqClusterRequestKind.SyncDelta, channel.Metadata.ChannelName, null, since.UtcTicks, cancellationToken)
                .ConfigureAwait(false);

            var delta = response.PayloadJson?.FromNJson<ZeroMqSnapshotDeltaPayload>();
            if (delta is null)
                return;

            var updatedCount = 0;
            foreach (var entry in delta.UpdatedEntries.Where(e => e.State == SnapshotEntryState.Active))
            {
                OverwriteSnapshot(channel, entry);
                updatedCount++;
            }

            var deletedCount = 0;
            foreach (var hashKey in delta.DeletedHashKeys)
            {
                DeleteSnapshot(channel, hashKey);
                deletedCount++;
            }

            if (updatedCount > 0 || deletedCount > 0)
                Log.DeltaSyncApplied(_logger, updatedCount, deletedCount, channel.Metadata.ChannelName);
        }

        /// <summary>Answering side of <see cref="RestoreFromLeaderAsync"/>.</summary>
        private async Task<ZeroMqClusterResponseEnvelope> BuildRestoreSnapshotResponseAsync(ZeroMqClusterRequestEnvelope request, CancellationToken cancellationToken)
        {
            try
            {
                var channel = _channelResolver.GetChannel(request.ChannelName!);
                var entries = await channel.SearchSnapshotsAsync(e => e.State == SnapshotEntryState.Active, 0, 0, cancellationToken).ConfigureAwait(false);

                return new ZeroMqClusterResponseEnvelope(request.CorrelationId, true, null, entries.ToNJson());
            }
            catch (Exception exception)
            {
                return new ZeroMqClusterResponseEnvelope(request.CorrelationId, false, exception.Message, null);
            }
        }

        /// <summary>Answering side of <see cref="SyncDeltaFromLeaderAsync"/>.</summary>
        private async Task<ZeroMqClusterResponseEnvelope> BuildSyncDeltaResponseAsync(ZeroMqClusterRequestEnvelope request, CancellationToken cancellationToken)
        {
            try
            {
                var since = new DateTimeOffset(request.SinceTicks!.Value, TimeSpan.Zero);
                var channel = _channelResolver.GetChannel(request.ChannelName!);

                var updatedEntries = await channel.SearchSnapshotsAsync(
                    e => e.State == SnapshotEntryState.Active && e.LastModified >= since, 0, 0, cancellationToken).ConfigureAwait(false);
                var deletedHashKeys = GetSnapshotTombstonesSince(channel, since);

                var payload = new ZeroMqSnapshotDeltaPayload { UpdatedEntries = updatedEntries, DeletedHashKeys = deletedHashKeys };
                return new ZeroMqClusterResponseEnvelope(request.CorrelationId, true, null, payload.ToNJson());
            }
            catch (Exception exception)
            {
                return new ZeroMqClusterResponseEnvelope(request.CorrelationId, false, exception.Message, null);
            }
        }

        private static partial class Log
        {
            [LoggerMessage(EventId = 91540, Level = LogLevel.Information,
                Message = "[Cluster] ZeroMQ restoring channel '{Channel}' from leader host '{Host}'.")]
            public static partial void RestoringChannel(ILogger logger, string channel, string host);

            [LoggerMessage(EventId = 91541, Level = LogLevel.Information,
                Message = "[Cluster] ZeroMQ restored {Count} snapshot entries for channel '{Channel}'.")]
            public static partial void RestoredChannel(ILogger logger, int count, string channel);

            [LoggerMessage(EventId = 91542, Level = LogLevel.Debug,
                Message = "[Cluster] ZeroMQ delta sync applied {Updated} updates and {Deleted} deletions for channel '{Channel}'.")]
            public static partial void DeltaSyncApplied(ILogger logger, int updated, int deleted, string channel);
        }
    }
}
