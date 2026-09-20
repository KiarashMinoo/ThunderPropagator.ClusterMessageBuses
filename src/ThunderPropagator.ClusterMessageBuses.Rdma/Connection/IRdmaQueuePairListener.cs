namespace ThunderPropagator.ClusterMessageBuses.Rdma
{
    /// <summary>
    /// Narrow seam around this node's own RDMA CM listening identifier, which every peer connects
    /// to (via RDMA CM's connect/accept handshake) in order to push fan-out/subscription-event
    /// frames and requests to this node. Mirrors <c>ITcpClusterListener</c> exactly. Not named in
    /// the task's literal required-file list (which only calls out
    /// <c>IRdmaQueuePairConnection</c>/<c>RdmaQueuePairConnection</c>/<c>RdmaConnectionCache</c>
    /// under <c>Connection/</c>) -- added because <see cref="RdmaClusterMessageBus"/> needs an
    /// accept-loop counterpart to its outbound connections, exactly like TcpSocket's
    /// <c>ITcpClusterListener</c>/<c>TcpListenerClusterListener</c> pair, which the task named as
    /// the structural ground truth to mirror.
    /// </summary>
    internal interface IRdmaQueuePairListener : IAsyncDisposable
    {
        /// <summary>
        /// Yields one accepted, fully-established <see cref="IRdmaQueuePairConnection"/> per
        /// inbound peer connection, for as long as the listener is running. Completes when
        /// <paramref name="cancellationToken"/> is cancelled or the listener is disposed.
        /// </summary>
        IAsyncEnumerable<IRdmaQueuePairConnection> AcceptConnectionsAsync(CancellationToken cancellationToken);
    }
}
