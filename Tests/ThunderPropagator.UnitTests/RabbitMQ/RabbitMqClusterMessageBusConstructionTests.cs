using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using RabbitMQ.Client;
using FluentAssertions;
using ThunderPropagator.Application.Channels.Cluster;
using ThunderPropagator.ClusterMessageBuses.RabbitMQ;

namespace ThunderPropagator.UnitTests.RabbitMQ;

public class RabbitMqClusterMessageBusConstructionTests
{
    [Fact]
    public void Constructor_NodeEndpointNotSet_Throws()
    {
        var options = Options.Create(new RabbitMqClusterMessageBusOptions());
        var clusterConfiguration = new ClusterConfiguration { NodeEndpoint = null };
        var channelResolver = Substitute.For<ThunderPropagator.ClusterMessageBuses.SharedKernel.IClusterChannelResolver>();

        var act = () => new RabbitMqClusterMessageBus(options, clusterConfiguration, channelResolver, NullLoggerFactory.Instance);

        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public async Task EnsureInitializedAsync_DeclaresRequestAndReplyQueuesNamedFromNodeEndpoint()
    {
        var connection = RabbitMqClusterMessageBusTestHelpers.CreateTrackedConnection(out var channels);
        var nodeEndpoint = new Uri("https://node1:5001/");

        await using var bus = await RabbitMqClusterMessageBusTestHelpers.CreateBusAsync(connection: connection, nodeEndpoint: nodeEndpoint);

        channels.Should().HaveCount(3, "one publish channel plus one request-listener channel plus one reply-listener channel are created eagerly during initialization");

        var requestQueue = RabbitMqTopicNaming.RequestQueue("thunderpropagator.cluster", nodeEndpoint);
        var replyQueue = RabbitMqTopicNaming.ReplyQueue("thunderpropagator.cluster", nodeEndpoint);

        await channels[1].Received(1).QueueDeclareAsync(requestQueue, false, true, true, null);
        await channels[1].Received(1).BasicConsumeAsync(requestQueue, true, Arg.Any<IAsyncBasicConsumer>());

        await channels[2].Received(1).QueueDeclareAsync(replyQueue, false, true, true, null);
        await channels[2].Received(1).BasicConsumeAsync(replyQueue, true, Arg.Any<IAsyncBasicConsumer>());
    }

    [Fact]
    public async Task EnsureInitializedAsync_CalledMoreThanOnce_OnlyInitializesOnce()
    {
        var connection = RabbitMqClusterMessageBusTestHelpers.CreateTrackedConnection(out var channels);
        await using var bus = await RabbitMqClusterMessageBusTestHelpers.CreateBusAsync(connection: connection);

        await bus.EnsureInitializedAsync(CancellationToken.None);
        await bus.EnsureInitializedAsync(CancellationToken.None);

        channels.Should().HaveCount(3, "a second EnsureInitializedAsync call must be a no-op once initialization has already completed");
    }

    [Fact]
    public async Task DisposeAsync_ClosesAndDisposesEveryChannelAndTheConnection()
    {
        var connection = RabbitMqClusterMessageBusTestHelpers.CreateTrackedConnection(out var channels);
        var bus = await RabbitMqClusterMessageBusTestHelpers.CreateBusAsync(connection: connection);

        await bus.DisposeAsync();

        foreach (var channel in channels)
        {
            await channel.Received(1).CloseAsync();
            await channel.Received(1).DisposeAsync();
        }

        await connection.Received(1).CloseAsync();
        await connection.Received(1).DisposeAsync();
    }

    [Fact]
    public async Task DisposeAsync_AlsoClosesConsumersOfSubscriptionsTheCallerNeverDisposedItself()
    {
        var connection = RabbitMqClusterMessageBusTestHelpers.CreateSubstituteConnection();
        var bus = await RabbitMqClusterMessageBusTestHelpers.CreateBusAsync(connection: connection);

        Func<ThunderPropagator.Application.Channels.Cluster.MessageBus.ClusterFanOutMessage, CancellationToken, Task> noOpHandler = (_, _) => Task.CompletedTask;
        var subscriptionHandle = await bus.SubscribeAsync(Guid.NewGuid(), noOpHandler);

        // Deliberately NOT disposing subscriptionHandle — simulating a host shutdown where the bus
        // is disposed before every individual channel subscription is (mirrors the regression test
        // added for KafkaClusterMessageBus after the same leak was found there).
        _ = subscriptionHandle;

        var act = async () => await bus.DisposeAsync();

        await act.Should().NotThrowAsync("DisposeAsync must clean up orphaned per-channel subscriptions rather than leaking their consumers");
    }
}
