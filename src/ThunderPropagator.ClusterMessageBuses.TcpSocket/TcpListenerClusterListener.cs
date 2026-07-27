using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;

namespace ThunderPropagator.ClusterMessageBuses.TcpSocket
{
    /// <summary>
    /// Production <see cref="ITcpClusterListener"/>: wraps a real <see cref="TcpListener"/> bound to
    /// this node's own <see cref="TcpClusterMessageBusOptions.Port"/> on every interface.
    /// </summary>
    internal sealed class TcpListenerClusterListener : ITcpClusterListener
    {
        private readonly TcpListener _listener;
        private readonly int _maxFrameSize;

        internal TcpListenerClusterListener(int port, int maxFrameSize)
        {
            _listener = new TcpListener(IPAddress.Any, port);
            _maxFrameSize = maxFrameSize;
            _listener.Start();
        }

        public async IAsyncEnumerable<ITcpClusterConnection> AcceptConnectionsAsync([EnumeratorCancellation] CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                TcpClient tcpClient;
                try
                {
                    tcpClient = await _listener.AcceptTcpClientAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    yield break;
                }
                catch (SocketException) when (cancellationToken.IsCancellationRequested)
                {
                    yield break;
                }
                catch (ObjectDisposedException)
                {
                    yield break;
                }

                yield return new NetworkStreamTcpClusterConnection(tcpClient, _maxFrameSize);
            }
        }

        public ValueTask DisposeAsync()
        {
            _listener.Stop();
            return ValueTask.CompletedTask;
        }
    }
}
