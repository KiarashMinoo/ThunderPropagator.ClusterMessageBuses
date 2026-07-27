using Amazon.SimpleNotificationService.Model;
using Amazon.SQS.Model;
using FluentAssertions;
using NSubstitute;
using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.AwsSqs;

namespace ThunderPropagator.UnitTests.AwsSqs;

public class AwsSqsClusterMessageBusSubscriptionSyncTests
{
    private static ClusterSubscriptionEvent CreateEvent(Guid originId) =>
        new(originId, "https://origin-node:5000/", ClusterSubscriptionEventKind.Added, [], DateTimeOffset.UtcNow);

    [Fact]
    public async Task PublishAsync_StampsOriginId_AndPublishesToTheChannelsTopic()
    {
        var sns = AwsSqsClusterMessageBusTestHelpers.CreateSnsSubstitute();
        await using var bus = await AwsSqsClusterMessageBusTestHelpers.CreateBusAsync(sns: sns);

        var channelKey = Guid.NewGuid();
        var subscriptionEvent = CreateEvent(Guid.Empty);

        await bus.PublishAsync(channelKey, subscriptionEvent);

        var expectedTopicArn = AwsSqsClusterMessageBusTestHelpers.TopicArnFor($"tp-cluster-subscriptions-{channelKey:N}");

        await sns.Received(1).PublishAsync(
            Arg.Is<PublishRequest>(r => r.TopicArn == expectedTopicArn &&
                r.Message.FromNJson<ClusterSubscriptionEvent>()!.OriginId == bus.SelfId),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task PublishAsync_SnsThrows_DoesNotThrow()
    {
        var sns = AwsSqsClusterMessageBusTestHelpers.CreateSnsSubstitute();
        sns.PublishAsync(Arg.Any<PublishRequest>(), Arg.Any<CancellationToken>())
            .Returns(_ => throw new InvalidOperationException("simulated SNS failure"));

        await using var bus = await AwsSqsClusterMessageBusTestHelpers.CreateBusAsync(sns: sns);

        var act = async () => await bus.PublishAsync(Guid.NewGuid(), CreateEvent(Guid.Empty));

        await act.Should().NotThrowAsync("a publish failure must not propagate out of PublishAsync");
    }

    [Fact]
    public async Task SubscribeAsync_CreatesTheChannelsQueue_AndSubscribesItToTheTopicWithRawMessageDelivery()
    {
        var sqs = AwsSqsClusterMessageBusTestHelpers.CreateSqsSubstitute();
        var sns = AwsSqsClusterMessageBusTestHelpers.CreateSnsSubstitute();
        await using var bus = await AwsSqsClusterMessageBusTestHelpers.CreateBusAsync(sqs: sqs, sns: sns);

        var channelKey = Guid.NewGuid();
        Func<ClusterSubscriptionEvent, CancellationToken, Task> handler = (_, _) => Task.CompletedTask;

        await using var subscription = await bus.SubscribeAsync(channelKey, handler);

        var expectedQueueName = $"tp-cluster-subscriptions-node1-5000-{channelKey:N}";
        var expectedTopicArn = AwsSqsClusterMessageBusTestHelpers.TopicArnFor($"tp-cluster-subscriptions-{channelKey:N}");
        var expectedQueueArn = AwsSqsClusterMessageBusTestHelpers.QueueArnFor(expectedQueueName);

        await sqs.Received(1).CreateQueueAsync(Arg.Is<CreateQueueRequest>(r => r.QueueName == expectedQueueName), Arg.Any<CancellationToken>());
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
        Func<ClusterSubscriptionEvent, CancellationToken, Task> handler = (_, _) => Task.CompletedTask;

        var subscription = await bus.SubscribeAsync(channelKey, handler);
        await subscription.DisposeAsync();

        await sns.Received(1).UnsubscribeAsync(Arg.Any<UnsubscribeRequest>(), Arg.Any<CancellationToken>());
        await sqs.Received(1).DeleteQueueAsync(Arg.Any<DeleteQueueRequest>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task HandleSubscriptionEventDeliveryAsync_SelfEcho_IsRejected()
    {
        await using var bus = await AwsSqsClusterMessageBusTestHelpers.CreateBusAsync();

        var invoked = false;
        Func<ClusterSubscriptionEvent, CancellationToken, Task> handler = (_, _) => { invoked = true; return Task.CompletedTask; };

        var selfEvent = CreateEvent(bus.SelfId);

        await bus.HandleSubscriptionEventDeliveryAsync(selfEvent.ToNJson(), handler, CancellationToken.None);

        invoked.Should().BeFalse("a node must never re-process its own subscription event");
    }

    [Fact]
    public async Task HandleSubscriptionEventDeliveryAsync_ForeignEvent_IsDelivered()
    {
        await using var bus = await AwsSqsClusterMessageBusTestHelpers.CreateBusAsync();

        ClusterSubscriptionEvent? received = null;
        Func<ClusterSubscriptionEvent, CancellationToken, Task> handler = (e, _) => { received = e; return Task.CompletedTask; };

        var foreignEvent = CreateEvent(Guid.NewGuid());

        await bus.HandleSubscriptionEventDeliveryAsync(foreignEvent.ToNJson(), handler, CancellationToken.None);

        received.Should().NotBeNull();
        received!.OriginId.Should().Be(foreignEvent.OriginId);
    }

    [Fact]
    public async Task HandleSubscriptionEventDeliveryAsync_MalformedPayload_IsSkippedWithoutThrowing()
    {
        await using var bus = await AwsSqsClusterMessageBusTestHelpers.CreateBusAsync();

        Func<ClusterSubscriptionEvent, CancellationToken, Task> handler = (_, _) => Task.CompletedTask;

        var act = async () => await bus.HandleSubscriptionEventDeliveryAsync("{ not valid json ", handler, CancellationToken.None);

        await act.Should().NotThrowAsync("a single malformed delivery must not crash the poll loop");
    }
}
