namespace ThunderPropagator.ClusterMessageBuses.Srd
{
    /// <summary>
    /// Narrow seam around this node's single shared libfabric RDM endpoint — used for both sending to
    /// every peer and receiving from every peer, mirroring <c>IUdpClusterSocket</c>'s "one shared
    /// endpoint, no per-peer connection" shape (libfabric's RDM endpoint type over EFA's SRD transport
    /// is connectionless, exactly like UDP is — peers are resolved once into an address-vector handle
    /// rather than requiring a persistent per-peer connection the way TCP/WebSocket's transports do).
    /// Exists purely so tests can substitute a fake endpoint instead of standing up a real libfabric
    /// endpoint (which additionally requires actual EFA hardware to succeed at all).
    /// </summary>
    internal interface ISrdClusterEndpoint : IAsyncDisposable
    {
        /// <summary>
        /// Serializes and sends one <see cref="SrdClusterFrame"/> as a single SRD message to
        /// <paramref name="peerEndpoint"/> — resolving it into a libfabric address-vector handle first
        /// (inserting it on first use and caching the mapping) if it hasn't been resolved yet.
        /// </summary>
        Task SendFrameAsync(SrdClusterFrame frame, Uri peerEndpoint, CancellationToken cancellationToken);

        /// <summary>
        /// Yields one already-deserialized <see cref="SrdClusterFrame"/> per message received on this
        /// endpoint, for as long as the endpoint is open. Completes when
        /// <paramref name="cancellationToken"/> is cancelled or the endpoint is disposed. Plays the
        /// same role <c>IUdpClusterSocket.ReceiveDatagramsAsync</c> plays for the UDP transport,
        /// except frames arrive already parsed (see <see cref="SrdClusterMessageBus"/>'s remarks for
        /// why: unlike a UDP datagram's sender <see cref="System.Net.IPEndPoint"/>, this project's
        /// native-interop scope has no way to recover a completion's source address — see
        /// <c>Wire/SrdClusterRequestEnvelope.cs</c>'s remarks).
        /// </summary>
        IAsyncEnumerable<SrdClusterFrame> ReceiveFramesAsync(CancellationToken cancellationToken);
    }
}
