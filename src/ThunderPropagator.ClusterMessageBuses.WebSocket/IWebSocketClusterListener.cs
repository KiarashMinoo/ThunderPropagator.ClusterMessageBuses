using System.Runtime.CompilerServices;

namespace ThunderPropagator.ClusterMessageBuses.WebSocket
{
    /// <summary>
    /// Narrow seam around this node's own WebSocket server endpoint, which every peer connects to
    /// in order to push fan-out/subscription-event frames and requests to this node.
    /// <see cref="System.Net.HttpListener"/> (the production implementation's real listener) is a
    /// concrete class with no virtual members, so — like the narrow transport-seam interfaces used
    /// for NATS.Net/DotPulsar/MQTTnet elsewhere in this repo — this interface exists purely so tests
    /// can substitute a fake listener instead of binding a real socket.
    /// </summary>
    internal interface IWebSocketClusterListener : IAsyncDisposable
    {
        /// <summary>
        /// Yields one accepted <see cref="System.Net.WebSockets.WebSocket"/> per inbound peer
        /// connection, for as long as the listener is running. Completes when
        /// <paramref name="cancellationToken"/> is cancelled or the listener is disposed.
        /// </summary>
        IAsyncEnumerable<System.Net.WebSockets.WebSocket> AcceptConnectionsAsync(CancellationToken cancellationToken);
    }
}
