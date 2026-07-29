using FluentAssertions;
using Google.Cloud.PubSub.V1;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.BuildingBlocks.Application.Enums;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.GcpPubSub;

namespace ThunderPropagator.UnitTests.GcpPubSub;

public class GcpPubSubClusterMessageBusFanOutTests
{
    [Fact]
    public async Task PublishAsync_StampsOriginId_AndPublishesToTheChannelsTopic()
    {
        var publisher = GcpPubSubClusterMessageBusTestHelpers.CreatePublisherSubstitute();
        await using var bus = await GcpPubSubClusterMessageBusTestHelpers.CreateBusAsync(publisher: publisher);

        var channelKey = Guid.NewGuid();
        var message = new ClusterFanOutMessage(Guid.Empty, 1, CastType.Broadcast, new Dictionary<string, object?>());

        await bus.PublishAsync(channelKey, message);

        var expectedTopicId = $"tp-cluster-fanout-{channelKey:N}";

        await publisher.Received(1).PublishAsync(
            Arg.Is<TopicName>(t => t.TopicId == expectedTopicId),
            Arg.Is<IEnumerable<PubsubMessage>>(messages => messages.Single().Data.ToStringUtf8().FromNJson<ClusterFanOutMessage>()!.OriginId == bus.SelfId),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task PublishAsync_PublisherThrows_DoesNotThrow()
    {
        var publisher = GcpPubSubClusterMessageBusTestHelpers.CreatePublisherSubstitute();
        publisher.PublishAsync(Arg.Any<TopicName>(), Arg.Any<IEnumerable<PubsubMessage>>(), Arg.Any<CancellationToken>())
            .Throws(new InvalidOperationException("simulated Pub/Sub failure"));

        await using var bus = await GcpPubSubClusterMessageBusTestHelpers.CreateBusAsync(publisher: publisher);

        var message = new ClusterFanOutMessage(Guid.Empty, 1, CastType.Broadcast, new Dictionary<string, object?>());
        var act = async () => await bus.PublishAsync(Guid.NewGuid(), message);

        await act.Should().NotThrowAsync("a publish failure must not propagate out of PublishAsync");
    }

    [Fact]
    public async Task SubscribeAsync_CreatesTheChannelsTopicAndThisNodesOwnSubscription()
    {
        var publisher = GcpPubSubClusterMessageBusTestHelpers.CreatePublisherSubstitute();
        var subscriber = GcpPubSubClusterMessageBusTestHelpers.CreateSubscriberSubstitute();
        await using var bus = await GcpPubSubClusterMessageBusTestHelpers.CreateBusAsync(publisher: publisher, subscriber: subscriber);

        var channelKey = Guid.NewGuid();
        Func<ClusterFanOutMessage, CancellationToken, Task> handler = (_, _) => Task.CompletedTask;

        await using var subscription = await bus.SubscribeAsync(channelKey, handler);

        var expectedSubscriptionId = $"tp-cluster-fanout-node1-5000-{channelKey:N}";
        var expectedTopicId = $"tp-cluster-fanout-{channelKey:N}";

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
        Func<ClusterFanOutMessage, CancellationToken, Task> handler = (_, _) => Task.CompletedTask;

        var subscription = await bus.SubscribeAsync(channelKey, handler);
        await subscription.DisposeAsync();

        await subscriber.Received(1).DeleteSubscriptionAsync(Arg.Any<SubscriptionName>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task HandleFanOutDeliveryAsync_SelfEcho_IsRejected()
    {
        await using var bus = await GcpPubSubClusterMessageBusTestHelpers.CreateBusAsync();

        var invoked = false;
        Func<ClusterFanOutMessage, CancellationToken, Task> handler = (_, _) => { invoked = true; return Task.CompletedTask; };

        var selfMessage = new ClusterFanOutMessage(bus.SelfId, 1, CastType.Broadcast, new Dictionary<string, object?>());

        await bus.HandleFanOutDeliveryAsync(selfMessage.ToNJson(), handler, CancellationToken.None);

        invoked.Should().BeFalse("a node must never re-process its own fan-out broadcast");
    }

    [Fact]
    public async Task HandleFanOutDeliveryAsync_ForeignMessage_IsDelivered()
    {
        await using var bus = await GcpPubSubClusterMessageBusTestHelpers.CreateBusAsync();

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
        await using var bus = await GcpPubSubClusterMessageBusTestHelpers.CreateBusAsync();

        Func<ClusterFanOutMessage, CancellationToken, Task> handler = (_, _) => Task.CompletedTask;

        var act = async () => await bus.HandleFanOutDeliveryAsync("{ not valid json ", handler, CancellationToken.None);

        await act.Should().NotThrowAsync("a single malformed delivery must not crash the poll loop");
    }

    [Fact]
    public async Task HandleFanOutDeliveryAsync_HandlerThrows_DoesNotPropagate()
    {
        await using var bus = await GcpPubSubClusterMessageBusTestHelpers.CreateBusAsync();

        Func<ClusterFanOutMessage, CancellationToken, Task> handler = (_, _) => throw new InvalidOperationException("handler blew up");
        var foreignMessage = new ClusterFanOutMessage(Guid.NewGuid(), 1, CastType.Broadcast, new Dictionary<string, object?>());

        var act = async () => await bus.HandleFanOutDeliveryAsync(foreignMessage.ToNJson(), handler, CancellationToken.None);

        await act.Should().NotThrowAsync();
    }
}
