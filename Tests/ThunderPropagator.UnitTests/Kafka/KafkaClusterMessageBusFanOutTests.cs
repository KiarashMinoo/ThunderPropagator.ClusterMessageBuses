using Confluent.Kafka;
using FluentAssertions;
using NSubstitute;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.BuildingBlocks.Application.Enums;
using ThunderPropagator.BuildingBlocks.Application.Helpers;

namespace ThunderPropagator.UnitTests.Kafka;

public class KafkaClusterMessageBusFanOutTests
{
    // Explicitly typed so it binds to IClusterMessageBus's ClusterFanOutMessage SubscribeAsync
    // overload unambiguously — a bare "(_, _) => Task.CompletedTask" lambda is ambiguous between
    // that overload and the ClusterSubscriptionEvent one.
    private static readonly Func<ClusterFanOutMessage, CancellationToken, Task> NoOpHandler = (_, _) => Task.CompletedTask;

    private static ClusterFanOutMessage SampleMessage(Guid originId) =>
        new(originId, HashKey: 42, CastType.Multicast, new Dictionary<string, object?> { ["Price"] = 1.23m });

    [Fact]
    public async Task PublishAsync_StampsOwnOriginIdAndProducesToTheChannelsFanOutTopic()
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
        var originalMessage = SampleMessage(Guid.NewGuid());

        await bus.PublishAsync(channelKey, originalMessage);

        capturedTopic.Should().Be(KafkaTopicNaming.FanOutTopic("thunderpropagator.cluster", channelKey));
        var stamped = capturedMessage!.Value.FromNJson<ClusterFanOutMessage>();
        stamped!.OriginId.Should().Be(bus.SelfId, "the bus always overwrites OriginId with its own self-echo identity before publishing");
        stamped.HashKey.Should().Be(originalMessage.HashKey);
        stamped.CastType.Should().Be(originalMessage.CastType);
    }

    [Fact]
    public async Task SubscribeAsync_SubscribesADedicatedConsumerToTheChannelsFanOutTopic()
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

        // Factory is used for every consumer the bus creates, including the constructor's own
        // request/reply consumers — but SubscribeAsync runs after construction, so by the time
        // we get here capturedConsumer holds the most recently created one: the fan-out consumer.
        capturedConsumer.Should().NotBeNull();
        capturedConsumer!.Received(1).Subscribe(KafkaTopicNaming.FanOutTopic("thunderpropagator.cluster", channelKey));
    }

    [Fact]
    public async Task RunFanOutConsumerLoopAsync_RejectsSelfEchoedMessages()
    {
        await using var bus = KafkaClusterMessageBusTestHelpers.CreateBus();

        var consumer = Substitute.For<IConsumer<string, string>>();
        var selfEchoed = SampleMessage(bus.SelfId).ToNJson();
        consumer.Consume(Arg.Any<CancellationToken>())
            .Returns(
                _ => KafkaClusterMessageBusTestHelpers.ConsumeResultWithValue(selfEchoed),
                _ => throw new OperationCanceledException());

        var received = new List<ClusterFanOutMessage>();
        await bus.RunFanOutConsumerLoopAsync(consumer, (message, _) =>
        {
            received.Add(message);
            return Task.CompletedTask;
        }, CancellationToken.None);

        received.Should().BeEmpty("a message whose OriginId matches this node's own SelfId is a self-echo and must not be delivered");
    }

    [Fact]
    public async Task RunFanOutConsumerLoopAsync_DeliversMessagesFromOtherNodes()
    {
        await using var bus = KafkaClusterMessageBusTestHelpers.CreateBus();

        var consumer = Substitute.For<IConsumer<string, string>>();
        var fromAnotherNode = SampleMessage(Guid.NewGuid()).ToNJson();
        consumer.Consume(Arg.Any<CancellationToken>())
            .Returns(
                _ => KafkaClusterMessageBusTestHelpers.ConsumeResultWithValue(fromAnotherNode),
                _ => throw new OperationCanceledException());

        var received = new List<ClusterFanOutMessage>();
        await bus.RunFanOutConsumerLoopAsync(consumer, (message, _) =>
        {
            received.Add(message);
            return Task.CompletedTask;
        }, CancellationToken.None);

        received.Should().HaveCount(1);
        received[0].HashKey.Should().Be(42);
    }

    [Fact]
    public async Task RunFanOutConsumerLoopAsync_SkipsUnparseableMessagesWithoutThrowing()
    {
        await using var bus = KafkaClusterMessageBusTestHelpers.CreateBus();

        var consumer = Substitute.For<IConsumer<string, string>>();
        consumer.Consume(Arg.Any<CancellationToken>())
            .Returns(
                _ => KafkaClusterMessageBusTestHelpers.ConsumeResultWithValue("not valid json"),
                _ => throw new OperationCanceledException());

        var received = new List<ClusterFanOutMessage>();
        var act = async () => await bus.RunFanOutConsumerLoopAsync(consumer, (message, _) =>
        {
            received.Add(message);
            return Task.CompletedTask;
        }, CancellationToken.None);

        await act.Should().NotThrowAsync();
        received.Should().BeEmpty();
    }

    [Fact]
    public async Task DisposingTheSubscriptionHandleClosesAndDisposesItsConsumer()
    {
        var consumers = new List<IConsumer<string, string>>();

        IConsumer<string, string> Factory(ConsumerConfig _)
        {
            var consumer = Substitute.For<IConsumer<string, string>>();
            consumer.Consume(Arg.Any<CancellationToken>()).Returns(_ => throw new OperationCanceledException());
            consumers.Add(consumer);
            return consumer;
        }

        await using var bus = KafkaClusterMessageBusTestHelpers.CreateBus(consumerFactory: Factory);
        var subscription = await bus.SubscribeAsync(Guid.NewGuid(), NoOpHandler);

        // consumers[0]/[1] are the bus's own request/reply consumers; [2] is SubscribeAsync's.
        var fanOutConsumer = consumers[^1];

        await subscription.DisposeAsync();

        fanOutConsumer.Received(1).Close();
        fanOutConsumer.Received(1).Dispose();
    }
}
