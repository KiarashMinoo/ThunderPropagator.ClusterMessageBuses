using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;

namespace ThunderPropagator.ClusterMessageBuses.Swim
{
    /// <summary>
    /// Production <see cref="ISwimClusterSocket"/>: wraps a real <see cref="UdpClient"/> bound to
    /// this node's own <see cref="SwimClusterMessageBusOptions.Port"/> on every interface. Mirrors
    /// the sibling UDP transport project's <c>UdpClientClusterSocket</c>; unlike that project, this
    /// project's own namespace segment is "Swim", not "UdpClient", so there is no ambiguity between
    /// this type's own name and the BCL's <see cref="UdpClient"/> that would call for an alias here.
    /// </summary>
    internal sealed class UdpSwimClusterSocket : ISwimClusterSocket
    {
        private readonly UdpClient _client;

        internal UdpSwimClusterSocket(int port)
        {
            _client = new UdpClient(port);
        }

        public async Task SendDatagramAsync(byte[] datagram, IPEndPoint remoteEndpoint, CancellationToken cancellationToken)
        {
            await _client.SendAsync(datagram, remoteEndpoint, cancellationToken).ConfigureAwait(false);
        }

        public async IAsyncEnumerable<SwimReceivedDatagram> ReceiveDatagramsAsync([EnumeratorCancellation] CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                UdpReceiveResult result;
                try
                {
                    result = await _client.ReceiveAsync(cancellationToken).ConfigureAwait(false);
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

                yield return new SwimReceivedDatagram(result.Buffer, result.RemoteEndPoint);
            }
        }

        public ValueTask DisposeAsync()
        {
            _client.Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
