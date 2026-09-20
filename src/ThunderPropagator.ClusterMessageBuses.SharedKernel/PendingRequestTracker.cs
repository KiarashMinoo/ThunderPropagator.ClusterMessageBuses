using System.Collections.Concurrent;

namespace ThunderPropagator.ClusterMessageBuses.SharedKernel
{
    /// <summary>
    /// Generic correlation-id-keyed pending-request tracker backing every transport's
    /// request/reply plumbing for the <see cref="ClusterRequestKind"/> operations. Consolidates what
    /// used to be a hand-rolled <c>ConcurrentDictionary&lt;Guid, TaskCompletionSource&lt;TResponse&gt;&gt;</c>
    /// plus matching add/complete/remove logic, repeated near-identically across every transport in
    /// this repo that answers a request asynchronously (i.e. everything except WebApi and Grpc,
    /// whose request/reply is already synchronously correlated by the HTTP call/gRPC unary-call
    /// stack itself, with no separate reply channel needing a correlation id at all).
    /// </summary>
    /// <typeparam name="TResponse">Typically <see cref="ClusterResponseEnvelope"/>.</typeparam>
    public sealed class PendingRequestTracker<TResponse>
    {
        private readonly ConcurrentDictionary<Guid, TaskCompletionSource<TResponse>> _pending = new();

        /// <summary>
        /// Registers a new pending wait for <paramref name="correlationId"/> and returns its
        /// <see cref="TaskCompletionSource{TResult}"/> (constructed with
        /// <see cref="TaskCreationOptions.RunContinuationsAsynchronously"/>, matching every existing
        /// transport's convention so a synchronous <c>TrySetResult</c> call from a receive loop never
        /// runs the awaiter's continuation inline on that loop's thread).
        /// </summary>
        /// <exception cref="InvalidOperationException">
        /// <paramref name="correlationId"/> is already pending -- callers generate a fresh
        /// <see cref="Guid.NewGuid"/> per request, so this indicates a real bug, not a race to
        /// tolerate silently.
        /// </exception>
        public TaskCompletionSource<TResponse> Register(Guid correlationId)
        {
            var tcs = new TaskCompletionSource<TResponse>(TaskCreationOptions.RunContinuationsAsynchronously);

            if (!_pending.TryAdd(correlationId, tcs))
                throw new InvalidOperationException($"Duplicate cluster request correlation id '{correlationId}'.");

            return tcs;
        }

        /// <summary>
        /// Completes the pending wait for <paramref name="correlationId"/> with
        /// <paramref name="getResponse"/>'s result, if one is still waiting. Returns
        /// <see langword="false"/> with no side effect if nothing is pending for that id (already
        /// completed, already timed out and removed, or a stray/duplicate reply) -- matches every
        /// existing transport's tolerant "a retried request may be answered more than once; only the
        /// first matching reply completes it" behavior.
        /// </summary>
        public bool TryComplete(Guid correlationId, Func<TResponse> getResponse)
        {
            if (!_pending.TryRemove(correlationId, out var pending))
                return false;

            return pending.TrySetResult(getResponse());
        }

        /// <summary>Removes the pending entry for <paramref name="correlationId"/> without completing it -- call from a request's own <c>finally</c> block once it's done waiting (success, failure, or timeout).</summary>
        public void Remove(Guid correlationId) => _pending.TryRemove(correlationId, out _);

        /// <summary>Cancels and removes every still-pending wait -- called from a transport's own <c>DisposeAsync</c>.</summary>
        public void CancelAll()
        {
            foreach (var pending in _pending.Values)
            {
                pending.TrySetCanceled();
            }

            _pending.Clear();
        }
    }
}
