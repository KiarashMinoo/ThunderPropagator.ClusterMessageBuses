namespace ThunderPropagator.ClusterMessageBuses.Pulsar
{
    /// <summary>
    /// Narrow seam over <c>DotPulsar</c>'s own client surface, exposing only the two primitives
    /// <see cref="PulsarClusterMessageBus"/> needs: publish, and subscribe (as an async stream).
    /// Kept deliberately independent of exactly which <c>DotPulsar</c> types/overloads are involved
    /// under the hood, for two reasons: it keeps all real client library surface contained in one
    /// small production implementation (<see cref="PulsarClusterTransport"/>), and it lets every test
    /// substitute this interface directly with NSubstitute rather than needing to fake concrete
    /// <c>DotPulsar</c> client types.
    /// </summary>
    internal interface IPulsarClusterTransport : IAsyncDisposable
    {
        ValueTask PublishAsync(string topic, string payload, CancellationToken cancellationToken);

        /// <summary>
        /// Subscribes to <paramref name="topic"/> under <paramref name="subscriptionName"/> and
        /// streams every message's payload until <paramref name="cancellationToken"/> is cancelled.
        /// Each distinct <paramref name="subscriptionName"/> receives its own full copy of the
        /// topic's messages (Pulsar delivers independently per subscription name), so every node
        /// passes its own unique name to get its own full broadcast copy.
        /// </summary>
        IAsyncEnumerable<string> SubscribeAsync(string topic, string subscriptionName, CancellationToken cancellationToken);
    }
}
