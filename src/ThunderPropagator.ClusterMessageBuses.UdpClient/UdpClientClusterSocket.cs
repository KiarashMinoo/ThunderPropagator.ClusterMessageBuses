using System.Net;
using System.Runtime.CompilerServices;
using SystemUdpClient = System.Net.Sockets.UdpClient;

namespace ThunderPropagator.ClusterMessageBuses.UdpClient
{
    /// <summary>
    /// Production <see cref="IUdpClusterSocket"/>: wraps a real <see cref="System.Net.Sockets.UdpClient"/>
    /// bound to this node's own <see cref="UdpClusterMessageBusOptions.Port"/> on every interface.
    /// </summary>
    /// <remarks>
    /// Aliased as <c>SystemUdpClient</c> throughout this file: this project's own namespace segment
    /// is (deliberately, to match the transport's public name) also "UdpClient", and while C#'s type
    /// vs. namespace lookup rules keep an unqualified <c>UdpClient</c> resolving correctly to the BCL
    /// type even from inside this namespace, the alias removes any ambiguity for a human reader
    /// rather than relying on that resolution rule silently doing the right thing.
    /// </remarks>
    internal sealed class UdpClientClusterSocket : IUdpClusterSocket
    {
        private readonly SystemUdpClient _client;

        internal UdpClientClusterSocket(int port)
        {
            _client = new SystemUdpClient(port);
        }

        public async Task SendDatagramAsync(byte[] datagram, IPEndPoint remoteEndpoint, CancellationToken cancellationToken)
        {
            await _client.SendAsync(datagram, remoteEndpoint, cancellationToken).ConfigureAwait(false);
        }

        public async IAsyncEnumerable<UdpReceivedDatagram> ReceiveDatagramsAsync([EnumeratorCancellation] CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                System.Net.Sockets.UdpReceiveResult result;
                try
                {
                    result = await _client.ReceiveAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    yield break;
                }
                catch (System.Net.Sockets.SocketException) when (cancellationToken.IsCancellationRequested)
                {
                    yield break;
                }
                catch (ObjectDisposedException)
                {
                    yield break;
                }

                yield return new UdpReceivedDatagram(result.Buffer, result.RemoteEndPoint);
            }
        }

        public ValueTask DisposeAsync()
        {
            _client.Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
