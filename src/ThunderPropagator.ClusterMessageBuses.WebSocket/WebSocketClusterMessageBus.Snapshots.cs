using Microsoft.Extensions.Logging;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Channels.Snapshots;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.WebSocket
{
    internal sealed partial class WebSocketClusterMessageBus
    {
        public override async Task RestoreFromLeaderAsync(Uri leaderEndpoint, IChannel channel, CancellationToken cancellationToken = default)
        {
            Log.RestoringChannel(_logger, channel.Metadata.ChannelName, leaderEndpoint.Host);

            var response = await SendRequestAsync(
                leaderEndpoint, ClusterRequestKind.RestoreSnapshot, channel.Metadata.ChannelName, null, null, cancellationToken)
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
                leaderEndpoint, ClusterRequestKind.SyncDelta, channel.Metadata.ChannelName, null, since.UtcTicks, cancellationToken)
                .ConfigureAwait(false);

            var delta = response.PayloadJson?.FromNJson<ClusterSnapshotDeltaPayload>();
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

        /// <summary>Answering side of <see cref="RestoreFromLeaderAsync"/>: mirrors <c>ClusterSnapshotEndpoints</c>'s GET-snapshot handler.</summary>
        private async Task<ClusterResponseEnvelope> BuildRestoreSnapshotResponseAsync(ClusterRequestEnvelope request, CancellationToken cancellationToken)
        {
            var channel = _channelResolver.GetChannel(request.ChannelName!);
            var entries = await channel.SearchSnapshotsAsync(e => e.State == SnapshotEntryState.Active, 0, 0, cancellationToken).ConfigureAwait(false);

            return new ClusterResponseEnvelope(request.CorrelationId, true, null, entries.ToNJson());
        }

        /// <summary>Answering side of <see cref="SyncDeltaFromLeaderAsync"/>: mirrors <c>ClusterSnapshotDeltaEndpoints</c>'s GET-delta handler.</summary>
        private async Task<ClusterResponseEnvelope> BuildSyncDeltaResponseAsync(ClusterRequestEnvelope request, CancellationToken cancellationToken)
        {
            var since = new DateTimeOffset(request.SinceTicks!.Value, TimeSpan.Zero);
            var channel = _channelResolver.GetChannel(request.ChannelName!);

            var updatedEntries = await channel.SearchSnapshotsAsync(
                e => e.State == SnapshotEntryState.Active && e.LastModified >= since, 0, 0, cancellationToken).ConfigureAwait(false);
            var deletedHashKeys = GetSnapshotTombstonesSince(channel, since);

            var payload = new ClusterSnapshotDeltaPayload { UpdatedEntries = updatedEntries, DeletedHashKeys = deletedHashKeys };
            return new ClusterResponseEnvelope(request.CorrelationId, true, null, payload.ToNJson());
        }

        private static partial class Log
        {
            [LoggerMessage(EventId = 91240, Level = LogLevel.Information,
                Message = "[Cluster] WebSocket restoring channel '{Channel}' from leader host '{Host}'.")]
            public static partial void RestoringChannel(ILogger logger, string channel, string host);

            [LoggerMessage(EventId = 91241, Level = LogLevel.Information,
                Message = "[Cluster] WebSocket restored {Count} snapshot entries for channel '{Channel}'.")]
            public static partial void RestoredChannel(ILogger logger, int count, string channel);

            [LoggerMessage(EventId = 91242, Level = LogLevel.Debug,
                Message = "[Cluster] WebSocket delta sync applied {Updated} updates and {Deleted} deletions for channel '{Channel}'.")]
            public static partial void DeltaSyncApplied(ILogger logger, int updated, int deleted, string channel);
        }
    }
}
