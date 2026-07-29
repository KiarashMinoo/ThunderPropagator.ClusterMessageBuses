using NetMQ;

namespace ThunderPropagator.ClusterMessageBuses.ZeroMQ
{
    /// <summary>
    /// Narrow seam around this node's own embedded ROUTER socket + <see cref="NetMQPoller"/>.
    /// RouterSocket/NetMQPoller have no interface and cannot be substituted directly, so — like
    /// every other transport's listener seam in this repo — this interface exists purely for tests;
    /// production always uses <see cref="NetMQClusterHost"/>.
    /// </summary>
    internal interface IZeroMqClusterHost : IAsyncDisposable
    {
        /// <summary>Queues a reply frame addressed to <paramref name="identity"/> (the routing frame the ROUTER read the original request with) for sending on the ROUTER socket. Safe to call from any thread.</summary>
        void SendReply(byte[] identity, ZeroMqClusterFrame frame);

        /// <summary>Registers a peer connection's DEALER socket/outbound queue on the shared poller this host owns.</summary>
        void AddPollable(ISocketPollable pollable);

        /// <summary>Unregisters a previously-added pollable (called when a peer connection is disposed/invalidated).</summary>
        void RemovePollable(ISocketPollable pollable);

        Task StartAsync(CancellationToken cancellationToken);
    }
}
