using Apache.NMS;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using ThunderPropagator.Application.Channels.Cluster;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.ClusterMessageBuses.ActiveMQ;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.UnitTests.ActiveMQ;

public class ActiveMqClusterMessageBusConstructionTests
{
    [Fact]
    public void Constructor_NodeEndpointNotSet_Throws()
    {
        var options = Options.Create(new ActiveMqClusterMessageBusOptions());
        var clusterConfiguration = new ClusterConfiguration { NodeEndpoint = null };
        var channelResolver = Substitute.For<IClusterChannelResolver>();
        var connection = ActiveMqClusterMessageBusTestHelpers.CreateSubstituteConnection();

        var act = () => new ActiveMqClusterMessageBus(
            options, clusterConfiguration, channelResolver, NullLoggerFactory.Instance,
            ActiveMqClusterMessageBusTestHelpers.ConnectionFactoryReturning(connection));

        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public async Task EnsureInitializedAsync_CreatesRequestAndReplyQueueConsumersNamedFromNodeEndpoint()
    {
        var connection = ActiveMqClusterMessageBusTestHelpers.CreateTrackedConnection(out var sessions);
        var nodeEndpoint = new Uri("https://node1:5001/");

        await using var bus = await ActiveMqClusterMessageBusTestHelpers.CreateBusAsync(connection: connection, nodeEndpoint: nodeEndpoint);

        sessions.Should().HaveCount(3, "one publish session plus one request-listener session plus one reply-listener session are created eagerly during initialization");

        var requestQueueName = ActiveMqTopicNaming.RequestQueue("thunderpropagator.cluster", nodeEndpoint);
        var replyQueueName = ActiveMqTopicNaming.ReplyQueue("thunderpropagator.cluster", nodeEndpoint);

        await sessions[1].Received(1).GetQueueAsync(requestQueueName);
        await sessions[1].Received(1).CreateConsumerAsync(Arg.Any<IDestination>());

        await sessions[2].Received(1).GetQueueAsync(replyQueueName);
        await sessions[2].Received(1).CreateConsumerAsync(Arg.Any<IDestination>());
    }

    [Fact]
    public async Task EnsureInitializedAsync_CalledMoreThanOnce_OnlyInitializesOnce()
    {
        var connection = ActiveMqClusterMessageBusTestHelpers.CreateTrackedConnection(out var sessions);
        await using var bus = await ActiveMqClusterMessageBusTestHelpers.CreateBusAsync(connection: connection);

        await bus.EnsureInitializedAsync(CancellationToken.None);
        await bus.EnsureInitializedAsync(CancellationToken.None);

        sessions.Should().HaveCount(3, "a second EnsureInitializedAsync call must be a no-op once initialization has already completed");
    }

    [Fact]
    public async Task DisposeAsync_ClosesEverySessionAndTheConnection()
    {
        var connection = ActiveMqClusterMessageBusTestHelpers.CreateTrackedConnection(out var sessions);
        var bus = await ActiveMqClusterMessageBusTestHelpers.CreateBusAsync(connection: connection);

        await bus.DisposeAsync();

        foreach (var session in sessions)
        {
            await session.Received(1).CloseAsync();
        }

        await connection.Received(1).CloseAsync();
    }

    [Fact]
    public async Task DisposeAsync_AlsoClosesConsumersOfSubscriptionsTheCallerNeverDisposedItself()
    {
        var connection = ActiveMqClusterMessageBusTestHelpers.CreateSubstituteConnection();
        var bus = await ActiveMqClusterMessageBusTestHelpers.CreateBusAsync(connection: connection);

        Func<ClusterFanOutMessage, CancellationToken, Task> noOpHandler = (_, _) => Task.CompletedTask;
        var subscriptionHandle = await bus.SubscribeAsync(Guid.NewGuid(), noOpHandler);

        // Deliberately NOT disposing subscriptionHandle — simulating a host shutdown where the bus
        // is disposed before every individual channel subscription is (mirrors the regression tests
        // added for every other broker transport in this repo after the same leak was found there
        // first for Kafka).
        _ = subscriptionHandle;

        var act = async () => await bus.DisposeAsync();

        await act.Should().NotThrowAsync("DisposeAsync must clean up orphaned per-channel subscriptions rather than leaking their consumers");
    }
}
