using FluentAssertions;
using NSubstitute;
using StackExchange.Redis;
using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.RedisPubSub;

namespace ThunderPropagator.UnitTests.RedisPubSub;

public class RedisPubSubClusterMessageBusSubscriptionSyncTests
{
    private static readonly Func<ClusterSubscriptionEvent, CancellationToken, Task> NoOpHandler = (_, _) => Task.CompletedTask;

    private static ClusterSubscriptionEvent CreateEvent(Guid originId) => new(
        originId,
        "https://node1:5000/",
        ClusterSubscriptionEventKind.Added,
        [new ClusterSubscriptionDescriptor("sub-1", "req-1", "conn-1")],
        DateTimeOffset.UtcNow);

    [Fact]
    public async Task PublishAsync_StampsOriginId_AndPublishesToTheSubscriptionEventChannel()
    {
        var subscriber = RedisPubSubClusterMessageBusTestHelpers.CreateSubscriberSubstitute();
        var connection = RedisPubSubClusterMessageBusTestHelpers.CreateSubstituteConnection(subscriber);
        await using var bus = await RedisPubSubClusterMessageBusTestHelpers.CreateBusAsync(connection: connection);

        var channelKey = Guid.NewGuid();
        var subscriptionEvent = CreateEvent(Guid.Empty);

        await bus.PublishAsync(channelKey, subscriptionEvent);

        var expectedChannel = RedisChannelNaming.SubscriptionEventChannel("thunderpropagator:cluster", channelKey);

        await subscriber.Received(1).PublishAsync(
            RedisChannel.Literal(expectedChannel),
            Arg.Is<RedisValue>(v => v.ToString().FromNJson<ClusterSubscriptionEvent>()!.OriginId == bus.SelfId));
    }

    [Fact]
    public async Task SubscribeAsync_SubscribesToTheChannelsSubscriptionEventChannel()
    {
        var subscriber = RedisPubSubClusterMessageBusTestHelpers.CreateSubscriberSubstitute();
        var connection = RedisPubSubClusterMessageBusTestHelpers.CreateSubstituteConnection(subscriber);
        await using var bus = await RedisPubSubClusterMessageBusTestHelpers.CreateBusAsync(connection: connection);

        var channelKey = Guid.NewGuid();
        var expectedChannel = RedisChannelNaming.SubscriptionEventChannel("thunderpropagator:cluster", channelKey);

        await using var subscription = await bus.SubscribeAsync(channelKey, NoOpHandler);

        await subscriber.Received(1).SubscribeAsync(RedisChannel.Literal(expectedChannel), Arg.Any<Action<RedisChannel, RedisValue>>());
    }

    [Fact]
    public async Task HandleSubscriptionEventDeliveryAsync_SelfEcho_IsRejected()
    {
        await using var bus = await RedisPubSubClusterMessageBusTestHelpers.CreateBusAsync();

        var invoked = false;
        Func<ClusterSubscriptionEvent, CancellationToken, Task> handler = (_, _) => { invoked = true; return Task.CompletedTask; };

        await bus.HandleSubscriptionEventDeliveryAsync(CreateEvent(bus.SelfId).ToNJson(), handler, CancellationToken.None);

        invoked.Should().BeFalse("a node must never re-process its own subscription-event broadcast");
    }

    [Fact]
    public async Task HandleSubscriptionEventDeliveryAsync_ForeignEvent_IsDelivered()
    {
        await using var bus = await RedisPubSubClusterMessageBusTestHelpers.CreateBusAsync();

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
        await using var bus = await RedisPubSubClusterMessageBusTestHelpers.CreateBusAsync();

        var invoked = false;
        Func<ClusterSubscriptionEvent, CancellationToken, Task> handler = (_, _) => { invoked = true; return Task.CompletedTask; };

        var act = async () => await bus.HandleSubscriptionEventDeliveryAsync("not json at all", handler, CancellationToken.None);

        await act.Should().NotThrowAsync("a single malformed delivery must not crash the subscription");
        invoked.Should().BeFalse();
    }

    [Fact]
    public async Task SubscribeAsync_DisposedHandle_UnsubscribesItsOwnChannel()
    {
        var subscriber = RedisPubSubClusterMessageBusTestHelpers.CreateSubscriberSubstitute();
        var connection = RedisPubSubClusterMessageBusTestHelpers.CreateSubstituteConnection(subscriber);
        await using var bus = await RedisPubSubClusterMessageBusTestHelpers.CreateBusAsync(connection: connection);

        var channelKey = Guid.NewGuid();
        var expectedChannel = RedisChannelNaming.SubscriptionEventChannel("thunderpropagator:cluster", channelKey);

        var subscription = await bus.SubscribeAsync(channelKey, NoOpHandler);

        await subscription.DisposeAsync();

        await subscriber.Received(1).UnsubscribeAsync(RedisChannel.Literal(expectedChannel), Arg.Any<Action<RedisChannel, RedisValue>>());
    }
}
