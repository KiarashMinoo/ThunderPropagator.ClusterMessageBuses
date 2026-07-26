namespace ThunderPropagator.ClusterMessageBuses.NATS
{
    /// <summary>
    /// Narrow seam over <c>NATS.Net</c>'s own client surface, exposing only the three primitives
    /// <see cref="NatsClusterMessageBus"/> needs: publish, subscribe (as an async stream), and
    /// request/reply. Kept deliberately independent of exactly which <c>NATS.Net</c> types/overloads
    /// are involved under the hood, for two reasons: it keeps all real client library surface
    /// contained in one small production implementation
    /// (<see cref="NatsClusterTransport"/>), and it lets every test substitute this interface
    /// directly with NSubstitute rather than needing to fake concrete <c>NATS.Net</c> client types.
    /// </summary>
    internal interface INatsClusterTransport : IAsyncDisposable
    {
        ValueTask PublishAsync(string subject, string payload, CancellationToken cancellationToken);

        /// <summary>
        /// Subscribes to <paramref name="subject"/> and streams every delivery until
        /// <paramref name="cancellationToken"/> is cancelled.
        /// </summary>
        IAsyncEnumerable<NatsClusterDelivery> SubscribeAsync(string subject, CancellationToken cancellationToken);

        /// <summary>
        /// Sends a NATS request to <paramref name="subject"/> and awaits the reply payload, or
        /// <see langword="null"/> if no reply arrives before <paramref name="cancellationToken"/> is
        /// cancelled.
        /// </summary>
        ValueTask<string?> RequestAsync(string subject, string payload, CancellationToken cancellationToken);
    }
}
