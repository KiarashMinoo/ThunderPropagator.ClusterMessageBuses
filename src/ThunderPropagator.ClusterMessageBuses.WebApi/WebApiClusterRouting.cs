using System.Globalization;

namespace ThunderPropagator.ClusterMessageBuses.WebApi
{
    /// <summary>
    /// Builds every route path <see cref="WebApiClusterMessageBus"/> calls out to on a peer, and
    /// matches an inbound request's method/path/query against those same routes on the receiving
    /// side. Mirrors core's own <c>HttpClusterMessageBus</c> route shape (<c>fanout/{channelKey}</c>,
    /// <c>subscriptions/{channelKey}</c>, <c>snapshot/{channelName}</c>,
    /// <c>snapshot/{channelName}/delta</c>, <c>subscriptions/{channelKey}/self</c>) under this
    /// transport's own configurable <see cref="WebApiClusterMessageBusOptions.ListenPath"/> prefix.
    /// </summary>
    internal static class WebApiClusterRouting
    {
        internal static string FanOutPath(string listenPath, Guid channelKey)
            => $"{NormalizePrefix(listenPath)}fanout/{channelKey:N}";

        internal static string SubscriptionEventPath(string listenPath, Guid channelKey)
            => $"{NormalizePrefix(listenPath)}subscriptions/{channelKey:N}";

        internal static string SnapshotPath(string listenPath, string channelName)
            => $"{NormalizePrefix(listenPath)}snapshot/{Uri.EscapeDataString(channelName)}";

        internal static string SnapshotDeltaPath(string listenPath, string channelName, long sinceTicks)
            => $"{SnapshotPath(listenPath, channelName)}/delta?since={sinceTicks.ToString(CultureInfo.InvariantCulture)}";

        internal static string SubscriptionsSelfPath(string listenPath, Guid channelKey)
            => $"{SubscriptionEventPath(listenPath, channelKey)}/self";

        /// <summary>Prefix used for this node's own <see cref="System.Net.HttpListener"/> binding — same as every route's shared prefix, without a trailing route segment.</summary>
        internal static string ListenPrefix(Uri nodeEndpoint, string listenPath)
            => $"{nodeEndpoint.Scheme}://{nodeEndpoint.Authority}{NormalizePrefix(listenPath)}";

        /// <summary>The base URL used to address <paramref name="peerEndpoint"/>'s routes.</summary>
        internal static string PeerBaseUrl(Uri peerEndpoint, string listenPath)
            => $"{peerEndpoint.Scheme}://{peerEndpoint.Authority}{NormalizePrefix(listenPath)}";

        internal static string PeerFanOutUrl(Uri peerEndpoint, string listenPath, Guid channelKey)
            => $"{PeerBaseUrl(peerEndpoint, listenPath)}fanout/{channelKey:N}";

        internal static string PeerSubscriptionEventUrl(Uri peerEndpoint, string listenPath, Guid channelKey)
            => $"{PeerBaseUrl(peerEndpoint, listenPath)}subscriptions/{channelKey:N}";

        internal static string PeerSnapshotUrl(Uri peerEndpoint, string listenPath, string channelName)
            => $"{PeerBaseUrl(peerEndpoint, listenPath)}snapshot/{Uri.EscapeDataString(channelName)}";

        internal static string PeerSnapshotDeltaUrl(Uri peerEndpoint, string listenPath, string channelName, long sinceTicks)
            => $"{PeerSnapshotUrl(peerEndpoint, listenPath, channelName)}/delta?since={sinceTicks.ToString(CultureInfo.InvariantCulture)}";

        internal static string PeerSubscriptionsSelfUrl(Uri peerEndpoint, string listenPath, Guid channelKey)
            => $"{PeerSubscriptionEventUrl(peerEndpoint, listenPath, channelKey)}/self";

        private static string NormalizePrefix(string listenPath)
        {
            if (string.IsNullOrEmpty(listenPath))
                return "/";

            var path = listenPath.StartsWith('/') ? listenPath : $"/{listenPath}";
            return path.EndsWith('/') ? path : $"{path}/";
        }

        /// <summary>
        /// Matches <paramref name="method"/>/<paramref name="absolutePath"/>/<paramref name="query"/>
        /// against this transport's known routes. Returns <see langword="null"/> if nothing matches
        /// (the caller should respond 404).
        /// </summary>
        internal static WebApiRouteMatch? Match(string listenPath, string method, string absolutePath, string? query)
        {
            var prefix = NormalizePrefix(listenPath);
            if (!absolutePath.StartsWith(prefix, StringComparison.Ordinal))
                return null;

            var remainder = absolutePath[prefix.Length..];
            var segments = remainder.Split('/', StringSplitOptions.RemoveEmptyEntries);

            if (string.Equals(method, "POST", StringComparison.OrdinalIgnoreCase) && segments.Length == 2)
            {
                if (segments[0] == "fanout" && Guid.TryParseExact(segments[1], "N", out var fanOutKey))
                    return new WebApiRouteMatch(WebApiRouteKind.FanOut, ChannelKey: fanOutKey);

                if (segments[0] == "subscriptions" && Guid.TryParseExact(segments[1], "N", out var subscriptionKey))
                    return new WebApiRouteMatch(WebApiRouteKind.SubscriptionEvent, ChannelKey: subscriptionKey);
            }

            if (string.Equals(method, "GET", StringComparison.OrdinalIgnoreCase))
            {
                if (segments.Length == 2 && segments[0] == "snapshot")
                    return new WebApiRouteMatch(WebApiRouteKind.Snapshot, ChannelName: Uri.UnescapeDataString(segments[1]));

                if (segments.Length == 3 && segments[0] == "snapshot" && segments[2] == "delta")
                    return new WebApiRouteMatch(WebApiRouteKind.SnapshotDelta, ChannelName: Uri.UnescapeDataString(segments[1]), SinceTicks: ParseSinceTicks(query));

                if (segments.Length == 3 && segments[0] == "subscriptions" && segments[2] == "self" && Guid.TryParseExact(segments[1], "N", out var fetchKey))
                    return new WebApiRouteMatch(WebApiRouteKind.SubscriptionsSelf, ChannelKey: fetchKey);
            }

            return null;
        }

        private static long ParseSinceTicks(string? query)
        {
            if (string.IsNullOrEmpty(query))
                return 0;

            foreach (var pair in query.TrimStart('?').Split('&', StringSplitOptions.RemoveEmptyEntries))
            {
                var parts = pair.Split('=', 2);
                if (parts.Length == 2 && parts[0] == "since" && long.TryParse(parts[1], NumberStyles.Integer, CultureInfo.InvariantCulture, out var ticks))
                    return ticks;
            }

            return 0;
        }
    }
}
