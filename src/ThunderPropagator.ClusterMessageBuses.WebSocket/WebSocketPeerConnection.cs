using System.Net.WebSockets;
using System.Runtime.CompilerServices;
using System.Text;
using ThunderPropagator.BuildingBlocks.Application.Helpers;

namespace ThunderPropagator.ClusterMessageBuses.WebSocket
{
    /// <summary>
    /// Wraps a single physical <see cref="System.Net.WebSockets.WebSocket"/> — whether it is an
    /// outbound connection this node opened to a peer (a <see cref="ClientWebSocket"/>) or an
    /// inbound connection <see cref="IWebSocketClusterListener"/> accepted from a peer — behind one
    /// symmetric send/receive surface. Both directions carry the same <see cref="WebSocketClusterFrame"/>
    /// envelope, so one class serves both roles rather than two separate wrappers.
    /// </summary>
    internal sealed class WebSocketPeerConnection : IAsyncDisposable
    {
        private readonly System.Net.WebSockets.WebSocket _socket;
        private readonly int _receiveBufferSize;
        private readonly SemaphoreSlim _sendLock = new(1, 1);

        internal WebSocketPeerConnection(System.Net.WebSockets.WebSocket socket, int receiveBufferSize)
        {
            _socket = socket;
            _receiveBufferSize = receiveBufferSize;
        }

        /// <summary>
        /// Sends <paramref name="frame"/> as a single UTF8 text WebSocket message. Serializes access
        /// with a lock — <see cref="System.Net.WebSockets.WebSocket.SendAsync(ArraySegment{byte},WebSocketMessageType,bool,CancellationToken)"/>
        /// does not support concurrent callers on the same connection, and fan-out publishes,
        /// requests, and outbound responses can all originate from different callers at once.
        /// </summary>
        internal async Task SendFrameAsync(WebSocketClusterFrame frame, CancellationToken cancellationToken)
        {
            var bytes = Encoding.UTF8.GetBytes(frame.ToNJson());

            await _sendLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                await _socket.SendAsync(new ArraySegment<byte>(bytes), WebSocketMessageType.Text, endOfMessage: true, cancellationToken)
                    .ConfigureAwait(false);
            }
            finally
            {
                _sendLock.Release();
            }
        }

        /// <summary>
        /// Reads whole messages off the connection (reassembling fragments) and yields each as a
        /// deserialized <see cref="WebSocketClusterFrame"/>. A single malformed message is skipped
        /// rather than tearing down the connection; the loop ends when the peer closes the socket,
        /// <paramref name="cancellationToken"/> is cancelled, or the socket faults.
        /// </summary>
        internal async IAsyncEnumerable<WebSocketClusterFrame> ReceiveFramesAsync([EnumeratorCancellation] CancellationToken cancellationToken)
        {
            var buffer = new byte[_receiveBufferSize];

            while (_socket.State == WebSocketState.Open && !cancellationToken.IsCancellationRequested)
            {
                using var messageStream = new MemoryStream();
                WebSocketReceiveResult result;

                do
                {
                    try
                    {
                        result = await _socket.ReceiveAsync(new ArraySegment<byte>(buffer), cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception) when (cancellationToken.IsCancellationRequested)
                    {
                        yield break;
                    }
                    catch (WebSocketException)
                    {
                        yield break;
                    }
                    catch (ObjectDisposedException)
                    {
                        yield break;
                    }

                    if (result.MessageType == WebSocketMessageType.Close)
                        yield break;

                    messageStream.Write(buffer, 0, result.Count);
                }
                while (!result.EndOfMessage);

                var payload = Encoding.UTF8.GetString(messageStream.ToArray());

                WebSocketClusterFrame? frame = null;
                var parsed = false;
                try
                {
                    frame = payload.FromNJson<WebSocketClusterFrame>();
                    parsed = true;
                }
                catch (Exception)
                {
                    // A single malformed message must not take down an otherwise-healthy connection
                    // — skip it and keep reading, mirroring every broker transport's consume loops.
                }

                if (parsed && frame is not null)
                    yield return frame;
            }
        }

        public async ValueTask DisposeAsync()
        {
            try
            {
                if (_socket.State is WebSocketState.Open or WebSocketState.CloseReceived)
                {
                    await _socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "closing", CancellationToken.None).ConfigureAwait(false);
                }
            }
            catch (Exception)
            {
                // Best-effort: the connection is torn down regardless.
            }
            finally
            {
                _socket.Dispose();
                _sendLock.Dispose();
            }
        }
    }
}
