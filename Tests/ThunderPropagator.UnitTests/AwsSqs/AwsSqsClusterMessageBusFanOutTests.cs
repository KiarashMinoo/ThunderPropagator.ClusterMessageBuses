using Amazon.SimpleNotificationService.Model;
using Amazon.SQS.Model;
using FluentAssertions;
using NSubstitute;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.BuildingBlocks.Application.Enums;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.AwsSqs;

namespace ThunderPropagator.UnitTests.AwsSqs;

public class AwsSqsClusterMessageBusFanOutTests
{
    [Fact]
    public async Task PublishAsync_StampsOriginId_AndPublishesToTheChannelsTopic()
    {
        var sns = AwsSqsClusterMessageBusTestHelpers.CreateSnsSubstitute();
        await using var bus = await AwsSqsClusterMessageBusTestHelpers.CreateBusAsync(sns: sns);

        var channelKey = Guid.NewGuid();
        var message = new ClusterFanOutMessage(Guid.Empty, 1, CastType.Broadcast, new Dictionary<string, object?>());

        await bus.PublishAsync(channelKey, message);

        var expectedTopicArn = AwsSqsClusterMessageBusTestHelpers.TopicArnFor($"tp-cluster-fanout-{channelKey:N}");

        await sns.Received(1).PublishAsync(
            Arg.Is<PublishRequest>(r => r.TopicArn == expectedTopicArn &&
                r.Message.FromNJson<ClusterFanOutMessage>()!.OriginId == bus.SelfId),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task PublishAsync_SnsThrows_DoesNotThrow()
    {
        var sns = AwsSqsClusterMessageBusTestHelpers.CreateSnsSubstitute();
        sns.PublishAsync(Arg.Any<PublishRequest>(), Arg.Any<CancellationToken>())
            .Returns(_ => throw new InvalidOperationException("simulated SNS failure"));

        await using var bus = await AwsSqsClusterMessageBusTestHelpers.CreateBusAsync(sns: sns);

        var message = new ClusterFanOutMessage(Guid.Empty, 1, CastType.Broadcast, new Dictionary<string, object?>());
        var act = async () => await bus.PublishAsync(Guid.NewGuid(), message);

        await act.Should().NotThrowAsync("a publish failure must not propagate out of PublishAsync");
    }

    [Fact]
    public async Task SubscribeAsync_CreatesTheChannelsQueue_AndSubscribesItToTheTopicWithRawMessageDelivery()
    {
        var sqs = AwsSqsClusterMessageBusTestHelpers.CreateSqsSubstitute();
        var sns = AwsSqsClusterMessageBusTestHelpers.CreateSnsSubstitute();
        await using var bus = await AwsSqsClusterMessageBusTestHelpers.CreateBusAsync(sqs: sqs, sns: sns);

        var channelKey = Guid.NewGuid();
        Func<ClusterFanOutMessage, CancellationToken, Task> handler = (_, _) => Task.CompletedTask;

        await using var subscription = await bus.SubscribeAsync(channelKey, handler);

        var expectedQueueName = $"tp-cluster-fanout-node1-5000-{channelKey:N}";
        var expectedTopicArn = AwsSqsClusterMessageBusTestHelpers.TopicArnFor($"tp-cluster-fanout-{channelKey:N}");
        var expectedQueueArn = AwsSqsClusterMessageBusTestHelpers.QueueArnFor(expectedQueueName);

        await sqs.Received(1).CreateQueueAsync(Arg.Is<CreateQueueRequest>(r => r.QueueName == expectedQueueName), Arg.Any<CancellationToken>());
        await sqs.Received(1).SetQueueAttributesAsync(
            Arg.Is<SetQueueAttributesRequest>(r => r.Attributes.ContainsKey("Policy")),
            Arg.Any<CancellationToken>());
        await sns.Received(1).SubscribeAsync(
            Arg.Is<SubscribeRequest>(r => r.TopicArn == expectedTopicArn && r.Endpoint == expectedQueueArn &&
                r.Attributes["RawMessageDelivery"] == "true"),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task SubscribeAsync_DisposedHandle_UnsubscribesAndDeletesTheQueue()
    {
        var sqs = AwsSqsClusterMessageBusTestHelpers.CreateSqsSubstitute();
        var sns = AwsSqsClusterMessageBusTestHelpers.CreateSnsSubstitute();
        await using var bus = await AwsSqsClusterMessageBusTestHelpers.CreateBusAsync(sqs: sqs, sns: sns);

        var channelKey = Guid.NewGuid();
        Func<ClusterFanOutMessage, CancellationToken, Task> handler = (_, _) => Task.CompletedTask;

        var subscription = await bus.SubscribeAsync(channelKey, handler);
        await subscription.DisposeAsync();

        await sns.Received(1).UnsubscribeAsync(Arg.Any<UnsubscribeRequest>(), Arg.Any<CancellationToken>());
        await sqs.Received(1).DeleteQueueAsync(Arg.Any<DeleteQueueRequest>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task HandleFanOutDeliveryAsync_SelfEcho_IsRejected()
    {
        await using var bus = await AwsSqsClusterMessageBusTestHelpers.CreateBusAsync();

        var invoked = false;
        Func<ClusterFanOutMessage, CancellationToken, Task> handler = (_, _) => { invoked = true; return Task.CompletedTask; };

        var selfMessage = new ClusterFanOutMessage(bus.SelfId, 1, CastType.Broadcast, new Dictionary<string, object?>());

        await bus.HandleFanOutDeliveryAsync(selfMessage.ToNJson(), handler, CancellationToken.None);

        invoked.Should().BeFalse("a node must never re-process its own fan-out broadcast");
    }

    [Fact]
    public async Task HandleFanOutDeliveryAsync_ForeignMessage_IsDelivered()
    {
        await using var bus = await AwsSqsClusterMessageBusTestHelpers.CreateBusAsync();

        ClusterFanOutMessage? received = null;
        Func<ClusterFanOutMessage, CancellationToken, Task> handler = (m, _) => { received = m; return Task.CompletedTask; };

        var foreignMessage = new ClusterFanOutMessage(Guid.NewGuid(), 1, CastType.Broadcast, new Dictionary<string, object?>());

        await bus.HandleFanOutDeliveryAsync(foreignMessage.ToNJson(), handler, CancellationToken.None);

        received.Should().NotBeNull();
        received!.OriginId.Should().Be(foreignMessage.OriginId);
    }

    [Fact]
    public async Task HandleFanOutDeliveryAsync_MalformedPayload_IsSkippedWithoutThrowing()
    {
        await using var bus = await AwsSqsClusterMessageBusTestHelpers.CreateBusAsync();

        Func<ClusterFanOutMessage, CancellationToken, Task> handler = (_, _) => Task.CompletedTask;

        var act = async () => await bus.HandleFanOutDeliveryAsync("{ not valid json ", handler, CancellationToken.None);

        await act.Should().NotThrowAsync("a single malformed delivery must not crash the poll loop");
    }

    [Fact]
    public async Task HandleFanOutDeliveryAsync_HandlerThrows_DoesNotPropagate()
    {
        await using var bus = await AwsSqsClusterMessageBusTestHelpers.CreateBusAsync();

        Func<ClusterFanOutMessage, CancellationToken, Task> handler = (_, _) => throw new InvalidOperationException("handler blew up");
        var foreignMessage = new ClusterFanOutMessage(Guid.NewGuid(), 1, CastType.Broadcast, new Dictionary<string, object?>());

        var act = async () => await bus.HandleFanOutDeliveryAsync(foreignMessage.ToNJson(), handler, CancellationToken.None);

        await act.Should().NotThrowAsync();
    }
}
