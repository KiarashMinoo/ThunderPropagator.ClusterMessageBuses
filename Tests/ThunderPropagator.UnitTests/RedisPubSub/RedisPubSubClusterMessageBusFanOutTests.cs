using FluentAssertions;
using NSubstitute;
using StackExchange.Redis;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.BuildingBlocks.Application.Enums;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.RedisPubSub;

namespace ThunderPropagator.UnitTests.RedisPubSub;

public class RedisPubSubClusterMessageBusFanOutTests
{
    private static readonly Func<ClusterFanOutMessage, CancellationToken, Task> NoOpHandler = (_, _) => Task.CompletedTask;

    [Fact]
    public async Task PublishAsync_StampsOriginId_AndPublishesToTheFanOutChannel()
    {
        var subscriber = RedisPubSubClusterMessageBusTestHelpers.CreateSubscriberSubstitute();
        var connection = RedisPubSubClusterMessageBusTestHelpers.CreateSubstituteConnection(subscriber);
        await using var bus = await RedisPubSubClusterMessageBusTestHelpers.CreateBusAsync(connection: connection);

        var channelKey = Guid.NewGuid();
        var message = new ClusterFanOutMessage(Guid.Empty, 1, CastType.Broadcast, new Dictionary<string, object?>());

        await bus.PublishAsync(channelKey, message);

        var expectedChannel = RedisChannelNaming.FanOutChannel("thunderpropagator:cluster", channelKey);

        await subscriber.Received(1).PublishAsync(
            RedisChannel.Literal(expectedChannel),
            Arg.Is<RedisValue>(v => v.ToString().FromNJson<ClusterFanOutMessage>()!.OriginId == bus.SelfId));
    }

    [Fact]
    public async Task SubscribeAsync_SubscribesToTheChannelsFanOutChannel()
    {
        var subscriber = RedisPubSubClusterMessageBusTestHelpers.CreateSubscriberSubstitute();
        var connection = RedisPubSubClusterMessageBusTestHelpers.CreateSubstituteConnection(subscriber);
        await using var bus = await RedisPubSubClusterMessageBusTestHelpers.CreateBusAsync(connection: connection);

        var channelKey = Guid.NewGuid();
        var expectedChannel = RedisChannelNaming.FanOutChannel("thunderpropagator:cluster", channelKey);

        await using var subscription = await bus.SubscribeAsync(channelKey, NoOpHandler);

        await subscriber.Received(1).SubscribeAsync(RedisChannel.Literal(expectedChannel), Arg.Any<Action<RedisChannel, RedisValue>>());
    }

    [Fact]
    public async Task HandleFanOutDeliveryAsync_SelfEcho_IsRejected()
    {
        await using var bus = await RedisPubSubClusterMessageBusTestHelpers.CreateBusAsync();

        var invoked = false;
        Func<ClusterFanOutMessage, CancellationToken, Task> handler = (_, _) => { invoked = true; return Task.CompletedTask; };

        var selfMessage = new ClusterFanOutMessage(bus.SelfId, 1, CastType.Broadcast, new Dictionary<string, object?>());

        await bus.HandleFanOutDeliveryAsync(selfMessage.ToNJson(), handler, CancellationToken.None);

        invoked.Should().BeFalse("a node must never re-process its own fan-out broadcast");
    }

    [Fact]
    public async Task HandleFanOutDeliveryAsync_ForeignMessage_IsDelivered()
    {
        await using var bus = await RedisPubSubClusterMessageBusTestHelpers.CreateBusAsync();

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
        await using var bus = await RedisPubSubClusterMessageBusTestHelpers.CreateBusAsync();

        var invoked = false;
        Func<ClusterFanOutMessage, CancellationToken, Task> handler = (_, _) => { invoked = true; return Task.CompletedTask; };

        var act = async () => await bus.HandleFanOutDeliveryAsync("{ not valid json ", handler, CancellationToken.None);

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
        var expectedChannel = RedisChannelNaming.FanOutChannel("thunderpropagator:cluster", channelKey);

        var subscription = await bus.SubscribeAsync(channelKey, NoOpHandler);

        await subscription.DisposeAsync();

        await subscriber.Received(1).UnsubscribeAsync(RedisChannel.Literal(expectedChannel), Arg.Any<Action<RedisChannel, RedisValue>>());
    }
}
