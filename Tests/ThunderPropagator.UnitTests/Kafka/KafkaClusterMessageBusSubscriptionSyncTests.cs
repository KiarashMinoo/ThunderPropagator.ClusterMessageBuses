using Confluent.Kafka;
using FluentAssertions;
using NSubstitute;
using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.BuildingBlocks.Application.Helpers;

namespace ThunderPropagator.UnitTests.Kafka;

public class KafkaClusterMessageBusSubscriptionSyncTests
{
    // Explicitly typed so it binds to IClusterMessageBus's ClusterSubscriptionEvent SubscribeAsync
    // overload unambiguously — a bare "(_, _) => Task.CompletedTask" lambda is ambiguous between
    // that overload and the ClusterFanOutMessage one.
    private static readonly Func<ClusterSubscriptionEvent, CancellationToken, Task> NoOpHandler = (_, _) => Task.CompletedTask;

    private static ClusterSubscriptionEvent SampleEvent(Guid originId) =>
        new(originId, "https://origin-node:5000/", ClusterSubscriptionEventKind.Added,
            [new ClusterSubscriptionDescriptor("sub-1", "req-1", "conn-1")], DateTimeOffset.UtcNow);

    [Fact]
    public async Task PublishAsync_StampsOwnOriginIdAndProducesToTheChannelsSubscriptionTopic()
    {
        var producer = Substitute.For<IProducer<string, string>>();
        string? capturedTopic = null;
        Message<string, string>? capturedMessage = null;
        producer.ProduceAsync(
                Arg.Do<string>(t => capturedTopic = t),
                Arg.Do<Message<string, string>>(m => capturedMessage = m),
                Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<DeliveryResult<string, string>>(null!));

        await using var bus = KafkaClusterMessageBusTestHelpers.CreateBus(producer: producer);

        var channelKey = Guid.NewGuid();
        await bus.PublishAsync(channelKey, SampleEvent(Guid.NewGuid()));

        capturedTopic.Should().Be(KafkaTopicNaming.SubscriptionEventTopic("thunderpropagator.cluster", channelKey));
        var stamped = capturedMessage!.Value.FromNJson<ClusterSubscriptionEvent>();
        stamped!.OriginId.Should().Be(bus.SelfId);
        stamped.Subscriptions.Should().ContainSingle(s => s.SubscriptionId == "sub-1");
    }

    [Fact]
    public async Task SubscribeAsync_SubscribesADedicatedConsumerToTheChannelsSubscriptionTopic()
    {
        IConsumer<string, string>? capturedConsumer = null;

        IConsumer<string, string> Factory(ConsumerConfig _)
        {
            var consumer = Substitute.For<IConsumer<string, string>>();
            consumer.Consume(Arg.Any<CancellationToken>()).Returns(_ => throw new OperationCanceledException());
            capturedConsumer = consumer;
            return consumer;
        }

        await using var bus = KafkaClusterMessageBusTestHelpers.CreateBus(consumerFactory: Factory);
        var channelKey = Guid.NewGuid();

        await using var subscription = await bus.SubscribeAsync(channelKey, NoOpHandler);

        capturedConsumer.Should().NotBeNull();
        capturedConsumer!.Received(1).Subscribe(KafkaTopicNaming.SubscriptionEventTopic("thunderpropagator.cluster", channelKey));
    }

    [Fact]
    public async Task RunSubscriptionEventConsumerLoopAsync_RejectsSelfEchoedEvents()
    {
        await using var bus = KafkaClusterMessageBusTestHelpers.CreateBus();

        var consumer = Substitute.For<IConsumer<string, string>>();
        var selfEchoed = SampleEvent(bus.SelfId).ToNJson();
        consumer.Consume(Arg.Any<CancellationToken>())
            .Returns(
                _ => KafkaClusterMessageBusTestHelpers.ConsumeResultWithValue(selfEchoed),
                _ => throw new OperationCanceledException());

        var received = new List<ClusterSubscriptionEvent>();
        await bus.RunSubscriptionEventConsumerLoopAsync(consumer, (evt, _) =>
        {
            received.Add(evt);
            return Task.CompletedTask;
        }, CancellationToken.None);

        received.Should().BeEmpty();
    }

    [Fact]
    public async Task RunSubscriptionEventConsumerLoopAsync_DeliversEventsFromOtherNodes()
    {
        await using var bus = KafkaClusterMessageBusTestHelpers.CreateBus();

        var consumer = Substitute.For<IConsumer<string, string>>();
        var fromAnotherNode = SampleEvent(Guid.NewGuid()).ToNJson();
        consumer.Consume(Arg.Any<CancellationToken>())
            .Returns(
                _ => KafkaClusterMessageBusTestHelpers.ConsumeResultWithValue(fromAnotherNode),
                _ => throw new OperationCanceledException());

        var received = new List<ClusterSubscriptionEvent>();
        await bus.RunSubscriptionEventConsumerLoopAsync(consumer, (evt, _) =>
        {
            received.Add(evt);
            return Task.CompletedTask;
        }, CancellationToken.None);

        received.Should().HaveCount(1);
        received[0].Kind.Should().Be(ClusterSubscriptionEventKind.Added);
    }

    [Fact]
    public async Task RunSubscriptionEventConsumerLoopAsync_SkipsUnparseableEventsWithoutThrowing()
    {
        await using var bus = KafkaClusterMessageBusTestHelpers.CreateBus();

        var consumer = Substitute.For<IConsumer<string, string>>();
        consumer.Consume(Arg.Any<CancellationToken>())
            .Returns(
                _ => KafkaClusterMessageBusTestHelpers.ConsumeResultWithValue("{ not json"),
                _ => throw new OperationCanceledException());

        var received = new List<ClusterSubscriptionEvent>();
        var act = async () => await bus.RunSubscriptionEventConsumerLoopAsync(consumer, (evt, _) =>
        {
            received.Add(evt);
            return Task.CompletedTask;
        }, CancellationToken.None);

        await act.Should().NotThrowAsync();
        received.Should().BeEmpty();
    }
}
