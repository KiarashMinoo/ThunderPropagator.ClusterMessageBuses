namespace ThunderPropagator.ClusterMessageBuses.Rdma
{
    /// <summary>
    /// Wraps a single established RDMA reliable-connected (RC) queue pair -- whether it is an
    /// outbound connection this node opened to a peer or an inbound connection
    /// <see cref="IRdmaQueuePairListener"/> accepted from a peer -- behind one symmetric
    /// send/receive surface. Mirrors <c>ITcpClusterConnection</c> exactly, except framing: RDMA's
    /// two-sided SEND/RECV preserves message boundaries on its own (one <c>ibv_post_send</c> lands
    /// in exactly one posted <c>ibv_post_recv</c> buffer), so unlike TCP's raw byte stream there is
    /// no length prefix to apply. The one production implementation,
    /// <see cref="RdmaQueuePairConnection"/>, wraps real native handles that have no virtual
    /// members suitable for direct substitution, so this interface exists purely so tests can
    /// substitute a fake connection instead of touching real RDMA hardware.
    /// </summary>
    internal interface IRdmaQueuePairConnection : IAsyncDisposable
    {
        Task SendFrameAsync(RdmaClusterFrame frame, CancellationToken cancellationToken);

        IAsyncEnumerable<RdmaClusterFrame> ReceiveFramesAsync(CancellationToken cancellationToken);
    }
}
