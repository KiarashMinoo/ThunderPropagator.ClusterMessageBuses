using System.Buffers.Binary;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Text;
using ThunderPropagator.BuildingBlocks.Application.Helpers;

namespace ThunderPropagator.ClusterMessageBuses.TcpSocket
{
    /// <summary>
    /// Production <see cref="ITcpClusterConnection"/>: wraps a real <see cref="TcpClient"/>'s
    /// <see cref="NetworkStream"/>, framing every <see cref="TcpClusterFrame"/> as a 4-byte
    /// big-endian length prefix followed by its UTF8 NJson bytes.
    /// </summary>
    internal sealed class NetworkStreamTcpClusterConnection : ITcpClusterConnection
    {
        private readonly TcpClient _tcpClient;
        private readonly Stream _stream;
        private readonly int _maxFrameSize;
        private readonly SemaphoreSlim _sendLock = new(1, 1);

        internal NetworkStreamTcpClusterConnection(TcpClient tcpClient, int maxFrameSize)
        {
            _tcpClient = tcpClient;
            _stream = tcpClient.GetStream();
            _maxFrameSize = maxFrameSize;
        }

        public async Task SendFrameAsync(TcpClusterFrame frame, CancellationToken cancellationToken)
        {
            var payloadBytes = Encoding.UTF8.GetBytes(frame.ToNJson());
            var lengthPrefix = new byte[4];
            BinaryPrimitives.WriteInt32BigEndian(lengthPrefix, payloadBytes.Length);

            await _sendLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                await _stream.WriteAsync(lengthPrefix, cancellationToken).ConfigureAwait(false);
                await _stream.WriteAsync(payloadBytes, cancellationToken).ConfigureAwait(false);
                await _stream.FlushAsync(cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                _sendLock.Release();
            }
        }

        public async IAsyncEnumerable<TcpClusterFrame> ReceiveFramesAsync([EnumeratorCancellation] CancellationToken cancellationToken)
        {
            var lengthBuffer = new byte[4];

            while (!cancellationToken.IsCancellationRequested)
            {
                var gotLength = await TryReadExactAsync(_stream, lengthBuffer, cancellationToken).ConfigureAwait(false);
                if (!gotLength)
                    yield break;

                var length = BinaryPrimitives.ReadInt32BigEndian(lengthBuffer);
                if (length < 0 || length > _maxFrameSize)
                    yield break; // corrupt or hostile length prefix — close the connection

                var payloadBuffer = new byte[length];
                var gotPayload = await TryReadExactAsync(_stream, payloadBuffer, cancellationToken).ConfigureAwait(false);
                if (!gotPayload)
                    yield break;

                var payload = Encoding.UTF8.GetString(payloadBuffer);

                TcpClusterFrame? frame = null;
                var parsed = false;
                try
                {
                    frame = payload.FromNJson<TcpClusterFrame>();
                    parsed = true;
                }
                catch (Exception)
                {
                    // A single malformed message must not take down an otherwise-healthy connection
                    // — skip it and keep reading, mirroring every other transport's consume loops.
                }

                if (parsed && frame is not null)
                    yield return frame;
            }
        }

        /// <summary>Reads exactly <paramref name="buffer"/>'s length, or returns <see langword="false"/> if the peer closes the connection first.</summary>
        private static async Task<bool> TryReadExactAsync(Stream stream, byte[] buffer, CancellationToken cancellationToken)
        {
            var offset = 0;
            while (offset < buffer.Length)
            {
                int read;
                try
                {
                    read = await stream.ReadAsync(buffer.AsMemory(offset, buffer.Length - offset), cancellationToken).ConfigureAwait(false);
                }
                catch (Exception) when (cancellationToken.IsCancellationRequested)
                {
                    return false;
                }
                catch (IOException)
                {
                    return false;
                }
                catch (ObjectDisposedException)
                {
                    return false;
                }

                if (read == 0)
                    return false; // peer closed the connection

                offset += read;
            }

            return true;
        }

        public async ValueTask DisposeAsync()
        {
            try
            {
                await _stream.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception)
            {
                // Best-effort: the connection is torn down regardless.
            }
            finally
            {
                _tcpClient.Dispose();
                _sendLock.Dispose();
            }
        }
    }
}
