using System.Collections.Concurrent;

namespace ThunderPropagator.ClusterMessageBuses.SharedKernel
{
    /// <summary>
    /// Generic per-channel-key handler registry backing every transport's <c>SubscribeAsync</c>
    /// overload (for <c>ClusterFanOutMessage</c>, <c>ClusterSubscriptionEvent</c>, and
    /// <c>ClusterByteMessage</c> alike). Consolidates what used to be a hand-rolled
    /// <c>ConcurrentDictionary&lt;Guid, Func&lt;TMessage, CancellationToken, Task&gt;&gt;</c> plus a
    /// private nested <c>IAsyncDisposable</c> subscription-token class, repeated three times per
    /// transport across all nineteen transports in this repo (roughly fifty-seven near-identical
    /// copies of the same ~15 lines).
    /// </summary>
    /// <typeparam name="TMessage">
    /// <c>ClusterFanOutMessage</c>, <c>ClusterSubscriptionEvent</c>, or <c>ClusterByteMessage</c>.
    /// </typeparam>
    public sealed class ClusterHandlerRegistry<TMessage>
    {
        private readonly ConcurrentDictionary<Guid, Func<TMessage, CancellationToken, Task>> _handlers = new();

        /// <summary>
        /// Registers <paramref name="handler"/> for <paramref name="channelKey"/>, replacing any
        /// previous handler for the same key (matches every transport's existing "last subscriber
        /// wins" behavior -- <c>SubscribeAsync</c> was never documented as supporting multiple
        /// concurrent subscribers per channel key). Disposing the returned token unregisters it.
        /// </summary>
        public IAsyncDisposable Register(Guid channelKey, Func<TMessage, CancellationToken, Task> handler)
        {
            ArgumentNullException.ThrowIfNull(handler);

            _handlers[channelKey] = handler;
            return new Registration(_handlers, channelKey);
        }

        /// <summary>Looks up the handler currently registered for <paramref name="channelKey"/>, if any.</summary>
        public bool TryGetHandler(Guid channelKey, out Func<TMessage, CancellationToken, Task>? handler) =>
            _handlers.TryGetValue(channelKey, out handler);

        /// <summary>
        /// Invokes the handler registered for <paramref name="channelKey"/>, if one is registered --
        /// a no-op otherwise (matches every transport's existing "no subscriber, drop it silently"
        /// behavior for a delivery against a channel key nothing has subscribed to yet/anymore).
        /// </summary>
        public async Task InvokeAsync(Guid channelKey, TMessage message, CancellationToken cancellationToken)
        {
            if (_handlers.TryGetValue(channelKey, out var handler))
            {
                await handler(message, cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>Removes every registered handler -- called from a transport's own <c>DisposeAsync</c>.</summary>
        public void Clear() => _handlers.Clear();

        private sealed class Registration : IAsyncDisposable
        {
            private readonly ConcurrentDictionary<Guid, Func<TMessage, CancellationToken, Task>> _handlers;
            private readonly Guid _channelKey;

            internal Registration(ConcurrentDictionary<Guid, Func<TMessage, CancellationToken, Task>> handlers, Guid channelKey)
            {
                _handlers = handlers;
                _channelKey = channelKey;
            }

            public ValueTask DisposeAsync()
            {
                _handlers.TryRemove(_channelKey, out _);
                return ValueTask.CompletedTask;
            }
        }
    }
}
