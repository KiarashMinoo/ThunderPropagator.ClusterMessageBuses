namespace ThunderPropagator.ClusterMessageBuses.WebApi
{
    /// <summary>
    /// Narrow seam around this node's own embedded HTTP server endpoint, which every peer calls in
    /// order to push fan-out/subscription-event frames and pull leader/peer state from this node.
    /// <see cref="System.Net.HttpListener"/> (the production implementation's real listener) is a
    /// concrete class with no virtual members, so this interface exists purely so tests can
    /// substitute a fake listener instead of binding a real socket.
    /// </summary>
    internal interface IWebApiClusterListener : IAsyncDisposable
    {
        /// <summary>
        /// Yields one <see cref="WebApiIncomingRequest"/> per inbound HTTP request, for as long as
        /// the listener is running. Completes when <paramref name="cancellationToken"/> is cancelled
        /// or the listener is disposed.
        /// </summary>
        IAsyncEnumerable<WebApiIncomingRequest> AcceptRequestsAsync(CancellationToken cancellationToken);
    }
}
