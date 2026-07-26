namespace ThunderPropagator.ClusterMessageBuses.WebSocket
{
    /// <summary>
    /// Builds the two addresses <see cref="WebSocketClusterMessageBus"/> needs: the local prefix its
    /// own listener binds to (derived from this node's own
    /// <see cref="ThunderPropagator.Application.Channels.Cluster.ClusterConfiguration.NodeEndpoint"/>),
    /// and the <c>ws://</c>/<c>wss://</c> URL used to open an outbound connection to a given peer
    /// (derived from that peer's <see cref="ThunderPropagator.Application.Channels.Cluster.Discovery.ClusterNodeEntry.Endpoint"/>).
    /// Both simply swap the endpoint's <c>http</c>/<c>https</c> scheme for <c>ws</c>/<c>wss</c> and
    /// append <see cref="WebSocketClusterMessageBusOptions.ListenPath"/> — every node in the cluster
    /// is assumed to listen on the same path.
    /// </summary>
    internal static class WebSocketAddressing
    {
        /// <summary>
        /// The <c>http://</c>/<c>https://</c> prefix this node's own listener binds to (an
        /// <see cref="System.Net.HttpListener"/> prefix, not a WebSocket URL — the WebSocket upgrade
        /// handshake itself is always negotiated over plain HTTP).
        /// </summary>
        internal static string ListenPrefix(Uri nodeEndpoint, string listenPath)
        {
            var path = NormalizePath(listenPath);
            return $"{nodeEndpoint.Scheme}://{nodeEndpoint.Authority}{path}";
        }

        /// <summary>The <c>ws://</c>/<c>wss://</c> URL used to open an outbound connection to <paramref name="peerEndpoint"/>.</summary>
        internal static Uri PeerConnectUri(Uri peerEndpoint, string listenPath)
        {
            var scheme = string.Equals(peerEndpoint.Scheme, "https", StringComparison.OrdinalIgnoreCase) ? "wss" : "ws";
            var path = NormalizePath(listenPath);
            return new Uri($"{scheme}://{peerEndpoint.Authority}{path}");
        }

        private static string NormalizePath(string listenPath)
        {
            if (string.IsNullOrEmpty(listenPath))
                return "/";

            var path = listenPath.StartsWith('/') ? listenPath : $"/{listenPath}";
            return path.EndsWith('/') ? path : $"{path}/";
        }
    }
}
