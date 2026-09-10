namespace ThunderPropagator.ClusterMessageBuses.TcpSocket
{
    /// <summary>
    /// Wraps a single physical TCP connection — whether it is an outbound connection this node
    /// opened to a peer or an inbound connection <see cref="ITcpClusterListener"/> accepted from a
    /// peer — behind one symmetric send/receive surface, framing each <see cref="TcpClusterFrame"/>
    /// with a 4-byte length prefix (TCP is a raw byte stream with no built-in message boundaries,
    /// unlike WebSocket). <see cref="System.Net.Sockets.NetworkStream"/>/<see cref="System.Net.Sockets.TcpClient"/>
    /// have no virtual members suitable for direct substitution, so this interface exists purely so
    /// tests can substitute a fake connection instead of binding a real socket — unlike the WebSocket
    /// transport, which could substitute the BCL's own abstract <c>WebSocket</c> class directly.
    /// </summary>
    internal interface ITcpClusterConnection : IAsyncDisposable
    {
        Task SendFrameAsync(TcpClusterFrame frame, CancellationToken cancellationToken);

        IAsyncEnumerable<TcpClusterFrame> ReceiveFramesAsync(CancellationToken cancellationToken);
    }
}
