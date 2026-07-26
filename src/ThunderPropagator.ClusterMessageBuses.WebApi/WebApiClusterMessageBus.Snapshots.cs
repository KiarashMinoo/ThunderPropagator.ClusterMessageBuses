using System.Net.Http;
using Microsoft.Extensions.Logging;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Channels.Snapshots;
using ThunderPropagator.BuildingBlocks.Application.Helpers;

namespace ThunderPropagator.ClusterMessageBuses.WebApi
{
    internal sealed partial class WebApiClusterMessageBus
    {
        public override async Task RestoreFromLeaderAsync(Uri leaderEndpoint, IChannel channel, CancellationToken cancellationToken = default)
        {
            Log.RestoringChannel(_logger, channel.Metadata.ChannelName, leaderEndpoint.Host);

            var url = WebApiClusterRouting.PeerSnapshotUrl(leaderEndpoint, _options.ListenPath, channel.Metadata.ChannelName);
            using var request = new HttpRequestMessage(HttpMethod.Get, url);
            using var response = await _httpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
            response.EnsureSuccessStatusCode();

            var body = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
            var entries = body.FromNJson<SnapshotEntry[]>() ?? [];

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
            var url = WebApiClusterRouting.PeerSnapshotDeltaUrl(leaderEndpoint, _options.ListenPath, channel.Metadata.ChannelName, since.UtcTicks);
            using var request = new HttpRequestMessage(HttpMethod.Get, url);
            using var response = await _httpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
            response.EnsureSuccessStatusCode();

            var body = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
            var delta = body.FromNJson<WebApiSnapshotDeltaPayload>();
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

        /// <summary>Answering side of <see cref="RestoreFromLeaderAsync"/>: mirrors core's own <c>ClusterSnapshotEndpoints</c>'s GET-snapshot handler.</summary>
        internal async Task<string> BuildSnapshotResponseBodyAsync(string channelName, CancellationToken cancellationToken)
        {
            var channel = _channelResolver.GetChannel(channelName);
            var entries = await channel.SearchSnapshotsAsync(e => e.State == SnapshotEntryState.Active, 0, 0, cancellationToken).ConfigureAwait(false);

            return entries.ToNJson();
        }

        /// <summary>Answering side of <see cref="SyncDeltaFromLeaderAsync"/>: mirrors core's own <c>ClusterSnapshotDeltaEndpoints</c>'s GET-delta handler.</summary>
        internal async Task<string> BuildSnapshotDeltaResponseBodyAsync(string channelName, long sinceTicks, CancellationToken cancellationToken)
        {
            var since = new DateTimeOffset(sinceTicks, TimeSpan.Zero);
            var channel = _channelResolver.GetChannel(channelName);

            var updatedEntries = await channel.SearchSnapshotsAsync(
                e => e.State == SnapshotEntryState.Active && e.LastModified >= since, 0, 0, cancellationToken).ConfigureAwait(false);
            var deletedHashKeys = GetSnapshotTombstonesSince(channel, since);

            var payload = new WebApiSnapshotDeltaPayload { UpdatedEntries = updatedEntries, DeletedHashKeys = deletedHashKeys };
            return payload.ToNJson();
        }

        private static partial class Log
        {
            [LoggerMessage(EventId = 91340, Level = LogLevel.Information,
                Message = "[Cluster] WebApi restoring channel '{Channel}' from leader host '{Host}'.")]
            public static partial void RestoringChannel(ILogger logger, string channel, string host);

            [LoggerMessage(EventId = 91341, Level = LogLevel.Information,
                Message = "[Cluster] WebApi restored {Count} snapshot entries for channel '{Channel}'.")]
            public static partial void RestoredChannel(ILogger logger, int count, string channel);

            [LoggerMessage(EventId = 91342, Level = LogLevel.Debug,
                Message = "[Cluster] WebApi delta sync applied {Updated} updates and {Deleted} deletions for channel '{Channel}'.")]
            public static partial void DeltaSyncApplied(ILogger logger, int updated, int deleted, string channel);
        }
    }
}
