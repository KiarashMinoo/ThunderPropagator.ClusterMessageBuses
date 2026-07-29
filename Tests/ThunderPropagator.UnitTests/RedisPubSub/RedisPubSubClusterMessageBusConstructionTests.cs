using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using StackExchange.Redis;
using ThunderPropagator.Application.Channels.Cluster;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.ClusterMessageBuses.RedisPubSub;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.UnitTests.RedisPubSub;

public class RedisPubSubClusterMessageBusConstructionTests
{
    [Fact]
    public void Constructor_NodeEndpointNotSet_Throws()
    {
        var options = Options.Create(new RedisPubSubClusterMessageBusOptions());
        var clusterConfiguration = new ThunderPropagator.Application.Channels.Cluster.ClusterConfiguration { NodeEndpoint = null };
        var channelResolver = Substitute.For<IClusterChannelResolver>();
        var connection = RedisPubSubClusterMessageBusTestHelpers.CreateSubstituteConnection();

        var act = () => new RedisPubSubClusterMessageBus(
            options, clusterConfiguration, channelResolver, NullLoggerFactory.Instance,
            RedisPubSubClusterMessageBusTestHelpers.ConnectionFactoryReturning(connection));

        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public async Task EnsureInitializedAsync_SubscribesRequestAndReplyChannelsNamedFromNodeEndpoint()
    {
        var subscriber = RedisPubSubClusterMessageBusTestHelpers.CreateSubscriberSubstitute();
        var connection = RedisPubSubClusterMessageBusTestHelpers.CreateSubstituteConnection(subscriber);
        var nodeEndpoint = new Uri("https://node1:5001/");

        await using var bus = await RedisPubSubClusterMessageBusTestHelpers.CreateBusAsync(connection: connection, nodeEndpoint: nodeEndpoint);

        var requestChannel = RedisChannelNaming.RequestChannel("thunderpropagator:cluster", nodeEndpoint);
        var replyChannel = RedisChannelNaming.ReplyChannel("thunderpropagator:cluster", nodeEndpoint);

        await subscriber.Received(1).SubscribeAsync(RedisChannel.Literal(requestChannel), Arg.Any<Action<RedisChannel, RedisValue>>());
        await subscriber.Received(1).SubscribeAsync(RedisChannel.Literal(replyChannel), Arg.Any<Action<RedisChannel, RedisValue>>());
    }

    [Fact]
    public async Task EnsureInitializedAsync_CalledMoreThanOnce_OnlyInitializesOnce()
    {
        var connection = RedisPubSubClusterMessageBusTestHelpers.CreateSubstituteConnection();
        await using var bus = await RedisPubSubClusterMessageBusTestHelpers.CreateBusAsync(connection: connection);

        await bus.EnsureInitializedAsync(CancellationToken.None);
        await bus.EnsureInitializedAsync(CancellationToken.None);

        connection.Received(1).GetSubscriber();
    }

    [Fact]
    public async Task DisposeAsync_UnsubscribesRequestAndReplyChannelsAndClosesTheConnection()
    {
        var subscriber = RedisPubSubClusterMessageBusTestHelpers.CreateSubscriberSubstitute();
        var connection = RedisPubSubClusterMessageBusTestHelpers.CreateSubstituteConnection(subscriber);
        var bus = await RedisPubSubClusterMessageBusTestHelpers.CreateBusAsync(connection: connection);

        await bus.DisposeAsync();

        await subscriber.Received(2).UnsubscribeAsync(Arg.Any<RedisChannel>(), Arg.Any<Action<RedisChannel, RedisValue>>());
        await connection.Received(1).CloseAsync();
    }

    [Fact]
    public async Task DisposeAsync_AlsoUnsubscribesFanOutSubscriptionsTheCallerNeverDisposedItself()
    {
        var connection = RedisPubSubClusterMessageBusTestHelpers.CreateSubstituteConnection();
        var bus = await RedisPubSubClusterMessageBusTestHelpers.CreateBusAsync(connection: connection);

        Func<ClusterFanOutMessage, CancellationToken, Task> noOpHandler = (_, _) => Task.CompletedTask;
        var subscriptionHandle = await bus.SubscribeAsync(Guid.NewGuid(), noOpHandler);

        // Deliberately NOT disposing subscriptionHandle — simulating a host shutdown where the bus
        // is disposed before every individual channel subscription is (mirrors the regression tests
        // added for every other broker transport in this repo after the same leak was found there
        // first for Kafka).
        _ = subscriptionHandle;

        var act = async () => await bus.DisposeAsync();

        await act.Should().NotThrowAsync("DisposeAsync must clean up orphaned per-channel subscriptions rather than leaking them");
    }
}
