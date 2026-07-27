using System.Runtime.CompilerServices;

namespace ThunderPropagator.ClusterMessageBuses.TcpSocket
{
    /// <summary>
    /// Narrow seam around this node's own TCP listener, which every peer connects to in order to
    /// push fan-out/subscription-event frames and requests to this node.
    /// <see cref="System.Net.Sockets.TcpListener"/> is a concrete class with no virtual members, so
    /// this interface exists purely so tests can substitute a fake listener instead of binding a
    /// real socket.
    /// </summary>
    internal interface ITcpClusterListener : IAsyncDisposable
    {
        /// <summary>
        /// Yields one accepted <see cref="ITcpClusterConnection"/> per inbound peer connection, for
        /// as long as the listener is running. Completes when <paramref name="cancellationToken"/>
        /// is cancelled or the listener is disposed.
        /// </summary>
        IAsyncEnumerable<ITcpClusterConnection> AcceptConnectionsAsync([EnumeratorCancellation] CancellationToken cancellationToken);
    }
}
