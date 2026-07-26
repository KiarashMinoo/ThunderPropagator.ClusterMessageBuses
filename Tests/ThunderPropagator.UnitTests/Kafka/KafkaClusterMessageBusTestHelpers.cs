using Confluent.Kafka;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using ThunderPropagator.Application.Channels.Cluster;
using ThunderPropagator.ClusterMessageBuses.Kafka;

namespace ThunderPropagator.UnitTests.Kafka;

/// <summary>
/// Shared construction helpers for <see cref="KafkaClusterMessageBus"/> tests. Every test
/// constructs the bus through the same seams the production DI extension uses
/// (<c>IOptions&lt;KafkaClusterMessageBusOptions&gt;</c>, <c>ClusterConfiguration</c>,
/// <c>IKafkaChannelResolver</c>, producer/consumer factory delegates) so no test ever needs a live
/// Kafka broker: <see cref="Confluent.Kafka.IProducer{TKey,TValue}"/> and
/// <see cref="Confluent.Kafka.IConsumer{TKey,TValue}"/> are both plain interfaces, substituted
/// directly with NSubstitute.
/// </summary>
internal static class KafkaClusterMessageBusTestHelpers
{
    internal static readonly Uri DefaultNodeEndpoint = new("https://node1:5000/");

    /// <summary>
    /// Builds a consumer factory whose every returned <see cref="IConsumer{TKey,TValue}"/>
    /// immediately throws <see cref="OperationCanceledException"/> from <c>Consume</c> — the safe
    /// default for the bus's own request/reply listener loops (started eagerly in the constructor)
    /// when a test isn't exercising them directly.
    /// </summary>
    internal static Func<ConsumerConfig, IConsumer<string, string>> AlwaysCancelledConsumerFactory()
    {
        return _ =>
        {
            var consumer = Substitute.For<IConsumer<string, string>>();
            consumer.Consume(Arg.Any<CancellationToken>()).Returns(_ => throw new OperationCanceledException());
            return consumer;
        };
    }

    internal static KafkaClusterMessageBus CreateBus(
        IProducer<string, string>? producer = null,
        IKafkaChannelResolver? channelResolver = null,
        Uri? nodeEndpoint = null,
        TimeSpan? requestTimeout = null,
        Func<ConsumerConfig, IConsumer<string, string>>? consumerFactory = null)
    {
        producer ??= Substitute.For<IProducer<string, string>>();
        channelResolver ??= Substitute.For<IKafkaChannelResolver>();

        var options = Options.Create(new KafkaClusterMessageBusOptions
        {
            BootstrapServers = "localhost:9092",
            RequestTimeout = requestTimeout ?? TimeSpan.FromSeconds(30),
        });

        var clusterConfiguration = new ClusterConfiguration { NodeEndpoint = nodeEndpoint ?? DefaultNodeEndpoint };

        return new KafkaClusterMessageBus(
            options,
            clusterConfiguration,
            channelResolver,
            NullLoggerFactory.Instance,
            _ => producer,
            consumerFactory ?? AlwaysCancelledConsumerFactory());
    }

    /// <summary>Builds a <see cref="ConsumeResult{TKey,TValue}"/> wrapping <paramref name="value"/> as the message body.</summary>
    internal static ConsumeResult<string, string> ConsumeResultWithValue(string value)
        => new() { Message = new Message<string, string> { Value = value } };
}
