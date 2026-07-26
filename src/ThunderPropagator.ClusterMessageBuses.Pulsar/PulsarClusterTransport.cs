using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Extensions;

namespace ThunderPropagator.ClusterMessageBuses.Pulsar
{
    /// <summary>
    /// Real <see cref="IPulsarClusterTransport"/> implementation, backed by <c>DotPulsar</c>'s
    /// <see cref="IPulsarClient"/>. Every payload this transport ever sends/receives is a JSON string
    /// this project already serialized itself via <c>NJsonHelper</c> (matching the manual-
    /// serialization approach used by the other broker transports), so this class always uses
    /// <see cref="Schema.String"/> rather than DotPulsar's other schema types.
    /// </summary>
    /// <remarks>
    /// Producers are expensive to create and are reused per topic (mirroring
    /// <c>RedisConnectionMultiplexerCache</c>/<c>MongoClientCache</c>'s dedup-by-key caching pattern
    /// from <c>ThunderPropagator.RecoveryHandlers</c>); consumers are created fresh per
    /// <see cref="SubscribeAsync"/> call and disposed when the returned stream stops being enumerated
    /// (cancelled or disposed), since each call represents an independent subscription.
    /// </remarks>
    internal sealed class PulsarClusterTransport : IPulsarClusterTransport
    {
        private readonly IPulsarClient _client;
        private readonly ConcurrentDictionary<string, Lazy<Task<IProducer<string>>>> _producers = new();

        internal PulsarClusterTransport(string serviceUrl)
        {
            _client = PulsarClient.Builder().ServiceUrl(new Uri(serviceUrl)).Build();
        }

        public async ValueTask PublishAsync(string topic, string payload, CancellationToken cancellationToken)
        {
            var producer = await GetOrCreateProducerAsync(topic).ConfigureAwait(false);
            await producer.Send(payload, cancellationToken).ConfigureAwait(false);
        }

        private Task<IProducer<string>> GetOrCreateProducerAsync(string topic)
        {
            return _producers.GetOrAdd(topic, static (t, client) => new Lazy<Task<IProducer<string>>>(
                () => client.NewProducer(Schema.String).Topic(t).Create()), _client).Value;
        }

        public async IAsyncEnumerable<string> SubscribeAsync(string topic, string subscriptionName, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await using var consumer = await _client.NewConsumer(Schema.String)
                .Topic(topic)
                .SubscriptionName(subscriptionName)
                .InitialPosition(SubscriptionInitialPosition.Latest)
                .Create();

            await foreach (var message in consumer.Messages(cancellationToken).ConfigureAwait(false))
            {
                yield return message.Value();
                await consumer.Acknowledge(message).ConfigureAwait(false);
            }
        }

        public async ValueTask DisposeAsync()
        {
            foreach (var lazyProducer in _producers.Values)
            {
                if (!lazyProducer.IsValueCreated)
                    continue;

                var producer = await lazyProducer.Value.ConfigureAwait(false);
                await producer.DisposeAsync().ConfigureAwait(false);
            }

            await _client.DisposeAsync().ConfigureAwait(false);
        }
    }
}
