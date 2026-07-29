namespace ThunderPropagator.ClusterMessageBuses.ZeroMQ
{
    /// <summary>
    /// Narrow seam around one peer's outbound DEALER socket. <c>NetMQ.Sockets.DealerSocket</c> is a
    /// concrete, non-substitutable type (no interface, no virtual members), so — like every other
    /// non-mockable transport dependency in this repo — this interface exists purely as a test seam;
    /// production always uses <see cref="ZeroMqPeerConnection"/>.
    /// </summary>
    internal interface IZeroMqPeerConnection : IDisposable
    {
        /// <summary>Queues <paramref name="frame"/> for sending to this connection's peer. Safe to call from any thread.</summary>
        void SendFrame(ZeroMqClusterFrame frame);
    }
}
