using System.Net.WebSockets;

namespace ThunderPropagator.ClusterMessageBuses.WebSocket
{
    /// <summary>
    /// Configuration for <see cref="WebSocketClusterMessageBus"/>.
    /// </summary>
    public sealed class WebSocketClusterMessageBusOptions
    {
        /// <summary>
        /// Path this node's own WebSocket listener accepts peer connections on, and that every
        /// outbound connection to a peer is opened against. Default: <c>"/thunderpropagator/cluster/ws"</c>.
        /// </summary>
        public string ListenPath { get; set; } = "/thunderpropagator/cluster/ws";

        /// <summary>
        /// How long <see cref="WebSocketClusterMessageBus"/> waits for a request/reply round trip
        /// (<c>RestoreFromLeaderAsync</c>, <c>SyncDeltaFromLeaderAsync</c>,
        /// <c>FetchPeerSubscriptionsAsync</c>) to complete before giving up. Default: 30 seconds.
        /// </summary>
        public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(30);

        /// <summary>
        /// Receive buffer size, in bytes, used when reading frames off any WebSocket connection
        /// (inbound-accepted or outbound-to-peer). Default: 16 KiB.
        /// </summary>
        public int ReceiveBufferSize { get; set; } = 16 * 1024;

        /// <summary>
        /// Optional hook to fine-tune a <see cref="ClientWebSocket"/>'s <see cref="ClientWebSocketOptions"/>
        /// before it connects out to a peer (e.g. TLS validation, sub-protocols, keep-alive interval).
        /// </summary>
        public Action<ClientWebSocketOptions>? ConfigureClientWebSocket { get; set; }
    }
}
