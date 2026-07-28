using FluentAssertions;
using Google.Cloud.PubSub.V1;
using NSubstitute;
using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.GcpPubSub;

namespace ThunderPropagator.UnitTests.GcpPubSub;

public class GcpPubSubClusterMessageBusSubscriptionSyncTests
{
    private static ClusterSubscriptionEvent CreateEvent(Guid originId) =>
        new(originId, "https://origin-node:5000/", ClusterSubscriptionEventKind.Added, [], DateTimeOffset.UtcNow);

    [Fact]
    public async Task PublishAsync_StampsOriginId_AndPublishesToTheChannelsTopic()
    {
        var publisher = GcpPubSubClusterMessageBusTestHelpers.CreatePublisherSubstitute();
        await using var bus = await GcpPubSubClusterMessageBusTestHelpers.CreateBusAsync(publisher: publisher);

        var channelKey = Guid.NewGuid();
        var subscriptionEvent = CreateEvent(Guid.Empty);

        await bus.PublishAsync(channelKey, subscriptionEvent);

        var expectedTopicId = $"tp-cluster-subscriptions-{channelKey:N}";

        await publisher.Received(1).PublishAsync(
            Arg.Is<TopicName>(t => t.TopicId == expectedTopicId),
            Arg.Is<IEnumerable<PubsubMessage>>(messages => messages.Single().Data.ToStringUtf8().FromNJson<ClusterSubscriptionEvent>()!.OriginId == bus.SelfId),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task PublishAsync_PublisherThrows_DoesNotThrow()
    {
        var publisher = GcpPubSubClusterMessageBusTestHelpers.CreatePublisherSubstitute();
        publisher.PublishAsync(Arg.Any<TopicName>(), Arg.Any<IEnumerable<PubsubMessage>>(), Arg.Any<CancellationToken>())
            .Returns(_ => throw new InvalidOperationException("simulated Pub/Sub failure"));

        await using var bus = await GcpPubSubClusterMessageBusTestHelpers.CreateBusAsync(publisher: publisher);

        var act = async () => await bus.PublishAsync(Guid.NewGuid(), CreateEvent(Guid.Empty));

        await act.Should().NotThrowAsync("a publish failure must not propagate out of PublishAsync");
    }

    [Fact]
    public async Task SubscribeAsync_CreatesTheChannelsTopicAndThisNodesOwnSubscription()
    {
        var publisher = GcpPubSubClusterMessageBusTestHelpers.CreatePublisherSubstitute();
        var subscriber = GcpPubSubClusterMessageBusTestHelpers.CreateSubscriberSubstitute();
        await using var bus = await GcpPubSubClusterMessageBusTestHelpers.CreateBusAsync(publisher: publisher, subscriber: subscriber);

        var channelKey = Guid.NewGuid();
        Func<ClusterSubscriptionEvent, CancellationToken, Task> handler = (_, _) => Task.CompletedTask;

        await using var subscription = await bus.SubscribeAsync(channelKey, handler);

        var expectedSubscriptionId = $"tp-cluster-subscriptions-node1-5000-{channelKey:N}";
        var expectedTopicId = $"tp-cluster-subscriptions-{channelKey:N}";

        await subscriber.Received(1).CreateSubscriptionAsync(
            Arg.Is<SubscriptionName>(s => s.SubscriptionId == expectedSubscriptionId),
            Arg.Is<TopicName>(t => t.TopicId == expectedTopicId),
            Arg.Any<PushConfig>(), Arg.Any<int>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task SubscribeAsync_DisposedHandle_DeletesTheSubscription()
    {
        var publisher = GcpPubSubClusterMessageBusTestHelpers.CreatePublisherSubstitute();
        var subscriber = GcpPubSubClusterMessageBusTestHelpers.CreateSubscriberSubstitute();
        await using var bus = await GcpPubSubClusterMessageBusTestHelpers.CreateBusAsync(publisher: publisher, subscriber: subscriber);

        var channelKey = Guid.NewGuid();
        Func<ClusterSubscriptionEvent, CancellationToken, Task> handler = (_, _) => Task.CompletedTask;

        var subscription = await bus.SubscribeAsync(channelKey, handler);
        await subscription.DisposeAsync();

        await subscriber.Received(1).DeleteSubscriptionAsync(Arg.Any<SubscriptionName>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task HandleSubscriptionEventDeliveryAsync_SelfEcho_IsRejected()
    {
        await using var bus = await GcpPubSubClusterMessageBusTestHelpers.CreateBusAsync();

        var invoked = false;
        Func<ClusterSubscriptionEvent, CancellationToken, Task> handler = (_, _) => { invoked = true; return Task.CompletedTask; };

        var selfEvent = CreateEvent(bus.SelfId);

        await bus.HandleSubscriptionEventDeliveryAsync(selfEvent.ToNJson(), handler, CancellationToken.None);

        invoked.Should().BeFalse("a node must never re-process its own subscription event");
    }

    [Fact]
    public async Task HandleSubscriptionEventDeliveryAsync_ForeignEvent_IsDelivered()
    {
        await using var bus = await GcpPubSubClusterMessageBusTestHelpers.CreateBusAsync();

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
        await using var bus = await GcpPubSubClusterMessageBusTestHelpers.CreateBusAsync();

        Func<ClusterSubscriptionEvent, CancellationToken, Task> handler = (_, _) => Task.CompletedTask;

        var act = async () => await bus.HandleSubscriptionEventDeliveryAsync("{ not valid json ", handler, CancellationToken.None);

        await act.Should().NotThrowAsync("a single malformed delivery must not crash the poll loop");
    }
}
