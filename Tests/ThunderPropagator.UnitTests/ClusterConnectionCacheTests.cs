using FluentAssertions;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.UnitTests;

public class ClusterConnectionCacheTests
{
    private sealed class FakeConnection : IAsyncDisposable
    {
        public bool Disposed { get; private set; }

        public ValueTask DisposeAsync()
        {
            Disposed = true;
            return ValueTask.CompletedTask;
        }
    }

    [Fact]
    public async Task GetOrCreateAsync_CalledConcurrentlyForSameKey_InvokesFactoryOnce()
    {
        // Arrange
        var factoryCalls = 0;
        await using var cache = new ClusterConnectionCache<FakeConnection>(async (_, _) =>
        {
            Interlocked.Increment(ref factoryCalls);
            await Task.Delay(20);
            return new FakeConnection();
        });

        // Act
        var results = await Task.WhenAll(Enumerable.Range(0, 10).Select(_ => cache.GetOrCreateAsync("peer-1")));

        // Assert
        factoryCalls.Should().Be(1);
        results.Should().OnlyContain(connection => ReferenceEquals(connection, results[0]));
    }

    [Fact]
    public async Task GetOrCreateAsync_DifferentKeys_ReturnsDistinctConnections()
    {
        // Arrange
        await using var cache = new ClusterConnectionCache<FakeConnection>((_, _) => Task.FromResult(new FakeConnection()));

        // Act
        var first = await cache.GetOrCreateAsync("peer-1");
        var second = await cache.GetOrCreateAsync("peer-2");

        // Assert
        first.Should().NotBeSameAs(second);
    }

    [Fact]
    public async Task GetOrCreateAsync_FirstAttemptFaults_RetriesTransparentlyWithoutCachingFailure()
    {
        // Arrange
        var attempt = 0;
        await using var cache = new ClusterConnectionCache<FakeConnection>(async (_, _) =>
        {
            attempt++;
            if (attempt == 1)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(1));
                throw new InvalidOperationException("first attempt fails");
            }

            return new FakeConnection();
        });

        // Act — a single call retries internally; the caller only sees the eventual success.
        var connection = await cache.GetOrCreateAsync("peer-1");

        // Assert
        connection.Should().NotBeNull();
        attempt.Should().Be(2);
    }

    [Fact]
    public async Task GetOrCreateAsync_AlwaysFaultsAndCallerCancels_StopsRetryingAndThrows()
    {
        // Arrange — a factory that never succeeds would otherwise retry forever; the caller's own
        // cancellation token is what bounds it.
        await using var cache = new ClusterConnectionCache<FakeConnection>(async (_, _) =>
        {
            await Task.Delay(TimeSpan.FromMilliseconds(1));
            throw new InvalidOperationException("never connects");
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(30));

        // Act
        var act = async () => await cache.GetOrCreateAsync("peer-1", cts.Token);

        // Assert
        await act.Should().ThrowAsync<Exception>();
    }

    [Fact]
    public async Task DisposeAsync_DisposesEveryCompletedConnection()
    {
        // Arrange
        var cache = new ClusterConnectionCache<FakeConnection>((_, _) => Task.FromResult(new FakeConnection()));
        var first = await cache.GetOrCreateAsync("peer-1");
        var second = await cache.GetOrCreateAsync("peer-2");

        // Act
        await cache.DisposeAsync();

        // Assert
        first.Disposed.Should().BeTrue();
        second.Disposed.Should().BeTrue();
    }

    [Fact]
    public async Task DisposeAsync_ConnectionStillPending_DoesNotThrow()
    {
        // Arrange — connect that never completes, simulating a dispose that races an in-flight connect.
        var neverCompletes = new TaskCompletionSource<FakeConnection>();
        var cache = new ClusterConnectionCache<FakeConnection>((_, _) => neverCompletes.Task);

        // Kick off the connect without awaiting it — GetOrAdd runs synchronously up to the first
        // await, so the cache entry exists (and is still pending) by the time this line returns.
        _ = cache.GetOrCreateAsync("peer-1");

        // Act
        var act = async () => await cache.DisposeAsync();

        // Assert
        await act.Should().NotThrowAsync();
    }
}
