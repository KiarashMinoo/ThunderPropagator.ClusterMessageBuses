using NetMQ;
using NetMQ.Sockets;
using ThunderPropagator.BuildingBlocks.Application.Helpers;

namespace ThunderPropagator.ClusterMessageBuses.ZeroMQ
{
    /// <summary>
    /// Real <see cref="IZeroMqPeerConnection"/>: one outbound DEALER socket connected to a single
    /// peer's ROUTER, registered on the shared <see cref="NetMQPoller"/> the bus's
    /// <see cref="IZeroMqClusterHost"/> owns. Outbound sends are handed off via a
    /// <see cref="NetMQQueue{T}"/> (safe from any calling thread) rather than touching the socket
    /// directly, since DEALER sockets are not thread-safe and may only be used from the poller
    /// thread — the queue's own <c>ReceiveReady</c> event fires on that same thread.
    /// </summary>
    internal sealed class ZeroMqPeerConnection : IZeroMqPeerConnection
    {
        private readonly IZeroMqClusterHost _host;
        private readonly DealerSocket _dealerSocket;
        private readonly NetMQQueue<string> _outboundQueue = new();

        internal ZeroMqPeerConnection(Uri peerEndpoint, ClusterZeroMqOptions options, IZeroMqClusterHost host, Action<string> onFrameReceived)
        {
            _host = host;

            _dealerSocket = new DealerSocket();
            NetMQSocketConfigurator.Apply(_dealerSocket, options);
            _dealerSocket.ReceiveReady += (_, e) =>
            {
                while (e.Socket.TryReceiveFrameString(out var json))
                {
                    onFrameReceived(json);
                }
            };
            _dealerSocket.Connect(ZeroMqAddressing.ConnectAddress(peerEndpoint));

            _outboundQueue.ReceiveReady += OnOutboundQueueReady;

            _host.AddPollable(_dealerSocket);
            _host.AddPollable(_outboundQueue);
        }

        public void SendFrame(ZeroMqClusterFrame frame) => _outboundQueue.Enqueue(frame.ToNJson());

        private void OnOutboundQueueReady(object? sender, NetMQQueueEventArgs<string> e)
        {
            while (e.Queue.TryDequeue(out var json, TimeSpan.Zero))
            {
                _dealerSocket.SendFrame(json);
            }
        }

        public void Dispose()
        {
            _host.RemovePollable(_dealerSocket);
            _host.RemovePollable(_outboundQueue);

            _outboundQueue.ReceiveReady -= OnOutboundQueueReady;

            _outboundQueue.Dispose();
            _dealerSocket.Close();
            _dealerSocket.Dispose();
        }
    }
}
