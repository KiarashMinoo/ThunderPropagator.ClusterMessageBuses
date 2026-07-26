using System.Runtime.CompilerServices;
using NATS.Net;

namespace ThunderPropagator.ClusterMessageBuses.NATS
{
    /// <summary>
    /// Real <see cref="INatsClusterTransport"/> implementation, backed by <c>NATS.Net</c>'s
    /// <see cref="NatsClient"/>. Every payload this transport ever sends/receives is a JSON string
    /// this project already serialized itself via <c>NJsonHelper</c> (matching the manual-
    /// serialization approach used by <c>KafkaClusterMessageBus</c> / <c>RabbitMqClusterMessageBus</c>),
    /// so this class always uses <c>NatsClient</c>'s <c>string</c>-typed overloads rather than its
    /// generic POCO (de)serialization support.
    /// </summary>
    internal sealed class NatsClusterTransport : INatsClusterTransport
    {
        private readonly NatsClient _client;

        internal NatsClusterTransport(string url)
        {
            _client = new NatsClient(url);
        }

        public ValueTask PublishAsync(string subject, string payload, CancellationToken cancellationToken)
            => _client.PublishAsync(subject, payload, cancellationToken: cancellationToken);

        public async IAsyncEnumerable<NatsClusterDelivery> SubscribeAsync(string subject, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await foreach (var message in _client.SubscribeAsync<string>(subject, cancellationToken: cancellationToken).ConfigureAwait(false))
            {
                yield return new NatsClusterDelivery(message.Data ?? string.Empty, message.ReplyTo);
            }
        }

        // NATS.Net's own NatsSubOpts.Timeout (used internally when no explicit reply-wait timeout is
        // supplied) surfaces as an OperationCanceledException from the awaited call, consistent with
        // every other cancellation-driven timeout in this library's API — so no reply within time is
        // expected to propagate as OperationCanceledException here, which
        // NatsClusterMessageBus.SendRequestAsync's own try/catch already maps to its own
        // TimeoutException (or rethrows unchanged if the caller's own token was what cancelled it).
        public async ValueTask<string?> RequestAsync(string subject, string payload, CancellationToken cancellationToken)
        {
            var reply = await _client.RequestAsync<string, string>(subject, payload, cancellationToken: cancellationToken).ConfigureAwait(false);
            return reply.Data;
        }

        public ValueTask DisposeAsync() => _client.DisposeAsync();
    }
}
