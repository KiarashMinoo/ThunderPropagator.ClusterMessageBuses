using Confluent.Kafka;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using ThunderPropagator.Application.Channels.Cluster;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.ClusterMessageBuses.Kafka;

using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.UnitTests.Kafka;

public class KafkaClusterMessageBusConstructionTests
{
    [Fact]
    public void Constructor_NodeEndpointNotSet_ThrowsInvalidOperationException()
    {
        var options = Options.Create(new KafkaClusterMessageBusOptions { BootstrapServers = "localhost:9092" });
        var clusterConfiguration = new ClusterConfiguration { NodeEndpoint = null };

        var act = () => new KafkaClusterMessageBus(
            options,
            clusterConfiguration,
            Substitute.For<IClusterChannelResolver>(),
            NullLoggerFactory.Instance,
            _ => Substitute.For<IProducer<string, string>>(),
            KafkaClusterMessageBusTestHelpers.AlwaysCancelledConsumerFactory());

        act.Should().Throw<InvalidOperationException>()
            .WithMessage($"*{nameof(ClusterConfiguration.NodeEndpoint)}*");
    }

    [Fact]
    public async Task Constructor_SubscribesRequestAndReplyConsumersToThisNodesOwnTopics()
    {
        var createdConsumers = new List<IConsumer<string, string>>();

        IConsumer<string, string> Factory(ConsumerConfig _)
        {
            var consumer = Substitute.For<IConsumer<string, string>>();
            consumer.Consume(Arg.Any<CancellationToken>()).Returns(_ => throw new OperationCanceledException());
            createdConsumers.Add(consumer);
            return consumer;
        }

        var nodeEndpoint = new Uri("https://node7:6001/");
        await using var bus = KafkaClusterMessageBusTestHelpers.CreateBus(nodeEndpoint: nodeEndpoint, consumerFactory: Factory);

        createdConsumers.Should().HaveCount(2, "the constructor creates one consumer for its own request topic and one for its own reply topic");
        createdConsumers[0].Received(1).Subscribe(KafkaTopicNaming.RequestTopic("thunderpropagator.cluster", nodeEndpoint));
        createdConsumers[1].Received(1).Subscribe(KafkaTopicNaming.ReplyTopic("thunderpropagator.cluster", nodeEndpoint));
    }

    [Fact]
    public async Task DisposeAsync_ClosesAndDisposesRequestAndReplyConsumersAndTheProducer()
    {
        var createdConsumers = new List<IConsumer<string, string>>();

        IConsumer<string, string> Factory(ConsumerConfig _)
        {
            var consumer = Substitute.For<IConsumer<string, string>>();
            consumer.Consume(Arg.Any<CancellationToken>()).Returns(_ => throw new OperationCanceledException());
            createdConsumers.Add(consumer);
            return consumer;
        }

        var producer = Substitute.For<IProducer<string, string>>();
        var bus = KafkaClusterMessageBusTestHelpers.CreateBus(producer: producer, consumerFactory: Factory);

        await bus.DisposeAsync();

        foreach (var consumer in createdConsumers)
        {
            consumer.Received(1).Close();
            consumer.Received(1).Dispose();
        }

        producer.Received(1).Dispose();
    }

    [Fact]
    public async Task DisposeAsync_AlsoClosesAndDisposesConsumersOfSubscriptionsTheCallerNeverDisposedItself()
    {
        var createdConsumers = new List<IConsumer<string, string>>();

        IConsumer<string, string> Factory(ConsumerConfig _)
        {
            var consumer = Substitute.For<IConsumer<string, string>>();
            consumer.Consume(Arg.Any<CancellationToken>()).Returns(_ => throw new OperationCanceledException());
            createdConsumers.Add(consumer);
            return consumer;
        }

        var bus = KafkaClusterMessageBusTestHelpers.CreateBus(consumerFactory: Factory);

        // Neither subscription handle below is disposed by the test itself — DisposeAsync must
        // still close/dispose their consumers rather than leaking them. Explicitly typed delegates
        // avoid ambiguity between IClusterMessageBus's two same-named SubscribeAsync overloads.
        Func<ClusterFanOutMessage, CancellationToken, Task> fanOutHandler = (_, _) => Task.CompletedTask;
        Func<ClusterSubscriptionEvent, CancellationToken, Task> subscriptionHandler = (_, _) => Task.CompletedTask;

        _ = await bus.SubscribeAsync(Guid.NewGuid(), fanOutHandler);
        _ = await bus.SubscribeAsync(Guid.NewGuid(), subscriptionHandler);

        // index 0/1 are the bus's own request/reply consumers; 2/3 are the two SubscribeAsync calls above.
        var fanOutConsumer = createdConsumers[2];
        var subscriptionConsumer = createdConsumers[3];

        await bus.DisposeAsync();

        fanOutConsumer.Received(1).Close();
        fanOutConsumer.Received(1).Dispose();
        subscriptionConsumer.Received(1).Close();
        subscriptionConsumer.Received(1).Dispose();
    }
}
