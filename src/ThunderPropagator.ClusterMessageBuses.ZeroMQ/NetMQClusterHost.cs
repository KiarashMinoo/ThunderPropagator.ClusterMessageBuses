using NetMQ;
using NetMQ.Sockets;
using ThunderPropagator.BuildingBlocks.Application.Helpers;

namespace ThunderPropagator.ClusterMessageBuses.ZeroMQ
{
    /// <summary>
    /// Real <see cref="IZeroMqClusterHost"/>: owns this node's own bound <see cref="RouterSocket"/>
    /// and the single shared <see cref="NetMQPoller"/> every peer connection's DEALER socket is also
    /// registered on — ROUTER/DEALER sockets are not thread-safe and require single-thread affinity,
    /// so every send/receive in this transport is funneled through this one poller thread via
    /// <see cref="NetMQQueue{T}"/>, the standard NetMQ cross-thread-messaging pattern.
    /// </summary>
    internal sealed class NetMQClusterHost : IZeroMqClusterHost
    {
        private readonly RouterSocket _routerSocket;
        private readonly NetMQQueue<(byte[] Identity, string FrameJson)> _outboundReplies = new();
        private readonly NetMQPoller _poller;
        private readonly EventHandler<NetMQSocketEventArgs> _onReceiveReady;

        internal NetMQClusterHost(Uri nodeEndpoint, ClusterZeroMqOptions options, EventHandler<NetMQSocketEventArgs> onReceiveReady)
        {
            _onReceiveReady = onReceiveReady;

            _routerSocket = new RouterSocket();
            NetMQSocketConfigurator.Apply(_routerSocket, options);
            _routerSocket.ReceiveReady += _onReceiveReady;

            _outboundReplies.ReceiveReady += OnOutboundRepliesReady;

            _poller = new NetMQPoller { _routerSocket, _outboundReplies };

            _routerSocket.Bind(ZeroMqAddressing.BindAddress(nodeEndpoint));
        }

        public void SendReply(byte[] identity, ZeroMqClusterFrame frame) => _outboundReplies.Enqueue((identity, frame.ToNJson()));

        private void OnOutboundRepliesReady(object? sender, NetMQQueueEventArgs<(byte[] Identity, string FrameJson)> e)
        {
            while (e.Queue.TryDequeue(out var item, TimeSpan.Zero))
            {
                var message = new NetMQMessage();
                message.Append(item.Identity);
                message.Append(item.FrameJson);
                _routerSocket.SendMultipartMessage(message);
            }
        }

        public void AddPollable(ISocketPollable pollable) => _poller.Add(pollable);

        public void RemovePollable(ISocketPollable pollable) => _poller.Remove(pollable);

        public Task StartAsync(CancellationToken cancellationToken)
        {
            _poller.RunAsync();
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            _poller.Stop();

            _routerSocket.ReceiveReady -= _onReceiveReady;
            _outboundReplies.ReceiveReady -= OnOutboundRepliesReady;

            _poller.Dispose();
            _outboundReplies.Dispose();
            _routerSocket.Close();
            _routerSocket.Dispose();

            return ValueTask.CompletedTask;
        }
    }
}
