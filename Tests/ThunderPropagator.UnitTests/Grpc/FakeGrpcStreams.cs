using System.Threading.Channels;
using Grpc.Core;

namespace ThunderPropagator.UnitTests.Grpc;

/// <summary>
/// In-memory <see cref="IClientStreamWriter{T}"/> a test can inspect afterward — lets a test assert
/// on exactly what <see cref="ThunderPropagator.ClusterMessageBuses.Grpc.GrpcPeerConnection"/> wrote without a real gRPC channel.
/// </summary>
internal sealed class FakeClientStreamWriter<T> : IClientStreamWriter<T>
{
    private readonly List<T> _written = [];

    public WriteOptions? WriteOptions { get; set; }

    internal IReadOnlyList<T> Written => _written;
    internal bool Completed { get; private set; }

    public Task WriteAsync(T message)
    {
        _written.Add(message);
        return Task.CompletedTask;
    }

    public Task CompleteAsync()
    {
        Completed = true;
        return Task.CompletedTask;
    }
}

/// <summary>
/// In-memory <see cref="IAsyncStreamReader{T}"/> a test can push items into (simulating the peer
/// writing back) or fault/complete on demand (simulating a broken or closed stream).
/// </summary>
internal sealed class FakeAsyncStreamReader<T> : IAsyncStreamReader<T>
{
    private readonly Channel<T> _channel = System.Threading.Channels.Channel.CreateUnbounded<T>();

    public T Current { get; private set; } = default!;

    internal void Write(T item) => _channel.Writer.TryWrite(item);

    internal void Complete() => _channel.Writer.TryComplete();

    internal void Fault(Exception exception) => _channel.Writer.TryComplete(exception);

    public async Task<bool> MoveNext(CancellationToken cancellationToken)
    {
        if (!await _channel.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
            return false;

        if (_channel.Reader.TryRead(out var item))
        {
            Current = item;
            return true;
        }

        return false;
    }
}
