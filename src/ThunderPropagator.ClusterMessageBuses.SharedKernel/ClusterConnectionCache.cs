using System.Collections.Concurrent;

namespace ThunderPropagator.ClusterMessageBuses.SharedKernel
{
    /// <summary>
    /// Generic keyed cache for expensive-to-construct cluster transport connections/clients (a gRPC
    /// channel, a ZeroMQ socket, a broker client, ...), shared across every
    /// <c>IClusterMessageBus</c> instance pointed at the same peer or endpoint instead of connecting
    /// once per instance. Generalizes the dedup-by-key, retry-after-failure pattern established by
    /// <c>RedisConnectionMultiplexerCache</c> / <c>MongoClientCache</c> in
    /// <c>ThunderPropagator.RecoveryHandlers</c> so every transport project in this repo can reuse it
    /// instead of reimplementing the same cache.
    /// </summary>
    /// <typeparam name="TConnection">The connection or client type being cached.</typeparam>
    public sealed class ClusterConnectionCache<TConnection> : IAsyncDisposable
    {
        private readonly Func<string, CancellationToken, Task<TConnection>> _connect;
        private readonly ConcurrentDictionary<string, Lazy<Task<TConnection>>> _connections = new(StringComparer.Ordinal);

        /// <summary>
        /// Creates a cache that constructs connections via <paramref name="connect" /> the first
        /// time each key is requested.
        /// </summary>
        /// <param name="connect">
        /// Builds a new connection for a given key (typically a peer endpoint or connection string).
        /// Invoked at most once per key unless a previous attempt faulted or was cancelled.
        /// </param>
        public ClusterConnectionCache(Func<string, CancellationToken, Task<TConnection>> connect)
        {
            ArgumentNullException.ThrowIfNull(connect);

            _connect = connect;
        }

        /// <summary>
        /// Returns the shared connection for <paramref name="key" />, connecting it on first use.
        /// Concurrent callers for the same key are coalesced onto a single in-flight connect
        /// operation. If a previous attempt for this key faulted or was cancelled, the failure is not
        /// cached — this call retries instead of permanently returning the same failure.
        /// </summary>
        /// <param name="key">Identifies the connection — typically a peer endpoint or connection string.</param>
        /// <param name="cancellationToken">
        /// Cancels waiting on the shared connect operation. If the connect that started it was kicked
        /// off by a different caller, cancelling here does not stop it for that caller.
        /// </param>
        public async Task<TConnection> GetOrCreateAsync(string key, CancellationToken cancellationToken = default)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(key);

            while (true)
            {
                var lazy = _connections.GetOrAdd(key, k => new Lazy<Task<TConnection>>(() => _connect(k, cancellationToken)));

                try
                {
                    return await lazy.Value.WaitAsync(cancellationToken).ConfigureAwait(false);
                }
                catch when (lazy.Value.IsFaulted || lazy.Value.IsCanceled)
                {
                    // Don't let a transient connect failure permanently poison the cache entry —
                    // remove it (only if it's still the same faulted/cancelled Lazy) so the next
                    // iteration retries.
                    _connections.TryRemove(new KeyValuePair<string, Lazy<Task<TConnection>>>(key, lazy));

                    if (cancellationToken.IsCancellationRequested)
                        throw;
                }
            }
        }

        /// <summary>
        /// Disposes every successfully-created cached connection that implements
        /// <see cref="IAsyncDisposable" /> or <see cref="IDisposable" />, and clears the cache.
        /// Connections whose construction never completed successfully (still in flight, faulted, or
        /// cancelled) are skipped — there is nothing to dispose.
        /// </summary>
        public async ValueTask DisposeAsync()
        {
            foreach (var lazy in _connections.Values)
            {
                if (!lazy.IsValueCreated || !lazy.Value.IsCompletedSuccessfully)
                    continue;

                try
                {
                    var connection = await lazy.Value.ConfigureAwait(false);

                    switch (connection)
                    {
                        case IAsyncDisposable asyncDisposable:
                            await asyncDisposable.DisposeAsync().ConfigureAwait(false);
                            break;
                        case IDisposable disposable:
                            disposable.Dispose();
                            break;
                    }
                }
                catch
                {
                    // Best-effort cleanup during shutdown — one failed disposal must not stop the others.
                }
            }

            _connections.Clear();
        }
    }
}
