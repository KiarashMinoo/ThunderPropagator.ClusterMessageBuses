using System.Net;

namespace ThunderPropagator.ClusterMessageBuses.Swim
{
    /// <summary>
    /// Narrow seam around this node's single shared UDP socket -- used for both sending to every
    /// peer and receiving from every peer, since UDP is connectionless and there is nothing to open
    /// "per peer" the way the TCP/WebSocket transports do. Mirrors the sibling UDP transport
    /// project's <c>IUdpClusterSocket</c> exactly (a separate assembly, so that interface cannot
    /// simply be reused instead of redeclared here).
    /// <see cref="System.Net.Sockets.UdpClient"/> is a concrete class with no virtual members, so
    /// this interface exists purely so tests can substitute a fake socket instead of binding a real
    /// one.
    /// </summary>
    internal interface ISwimClusterSocket : IAsyncDisposable
    {
        /// <summary>Sends one whole datagram to <paramref name="remoteEndpoint"/>.</summary>
        Task SendDatagramAsync(byte[] datagram, IPEndPoint remoteEndpoint, CancellationToken cancellationToken);

        /// <summary>
        /// Yields one <see cref="SwimReceivedDatagram"/> per datagram received on this socket, for as
        /// long as the socket is open. Completes when <paramref name="cancellationToken"/> is
        /// cancelled or the socket is disposed.
        /// </summary>
        IAsyncEnumerable<SwimReceivedDatagram> ReceiveDatagramsAsync(CancellationToken cancellationToken);
    }
}
