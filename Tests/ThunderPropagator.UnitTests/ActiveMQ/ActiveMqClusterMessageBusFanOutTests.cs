using Apache.NMS;
using FluentAssertions;
using NSubstitute;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.BuildingBlocks.Application.Enums;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.ActiveMQ;

namespace ThunderPropagator.UnitTests.ActiveMQ;

public class ActiveMqClusterMessageBusFanOutTests
{
    private static readonly Func<ClusterFanOutMessage, CancellationToken, Task> NoOpHandler = (_, _) => Task.CompletedTask;

    [Fact]
    public async Task PublishAsync_StampsOriginId_AndSendsToTheFanOutTopic()
    {
        var connection = ActiveMqClusterMessageBusTestHelpers.CreateTrackedConnection(out var sessions);
        await using var bus = await ActiveMqClusterMessageBusTestHelpers.CreateBusAsync(connection: connection);

        // sessions[0] is the shared publish session (created first, during EnsureInitializedAsync).
        var publishSession = sessions[0];
        var producer = await publishSession.CreateProducerAsync();

        var channelKey = Guid.NewGuid();
        var message = new ClusterFanOutMessage(Guid.Empty, 1, CastType.Broadcast, new Dictionary<string, object?>());

        await bus.PublishAsync(channelKey, message);

        var expectedTopic = ActiveMqTopicNaming.FanOutTopic("thunderpropagator.cluster", channelKey);

        await publishSession.Received(1).GetTopicAsync(expectedTopic);
        await producer.Received(1).SendAsync(
            Arg.Any<ITopic>(),
            Arg.Is<IMessage>(m => ((ITextMessage)m).Text!.FromNJson<ClusterFanOutMessage>()!.OriginId == bus.SelfId));
    }

    [Fact]
    public async Task SubscribeAsync_CreatesAConsumerOnTheChannelsFanOutTopic()
    {
        var connection = ActiveMqClusterMessageBusTestHelpers.CreateTrackedConnection(out var sessions);
        await using var bus = await ActiveMqClusterMessageBusTestHelpers.CreateBusAsync(connection: connection);

        var channelKey = Guid.NewGuid();
        var expectedTopic = ActiveMqTopicNaming.FanOutTopic("thunderpropagator.cluster", channelKey);

        await using var subscription = await bus.SubscribeAsync(channelKey, NoOpHandler);

        // sessions[0..2] are the init-time sessions (publish, request-listener, reply-listener);
        // sessions[3] is the one this SubscribeAsync call created for itself.
        var subscriptionSession = sessions[3];

        await subscriptionSession.Received(1).GetTopicAsync(expectedTopic);
        await subscriptionSession.Received(1).CreateConsumerAsync(Arg.Any<IDestination>());
    }

    [Fact]
    public async Task HandleFanOutDeliveryAsync_SelfEcho_IsRejected()
    {
        await using var bus = await ActiveMqClusterMessageBusTestHelpers.CreateBusAsync();

        var invoked = false;
        Func<ClusterFanOutMessage, CancellationToken, Task> handler = (_, _) => { invoked = true; return Task.CompletedTask; };

        var selfMessage = new ClusterFanOutMessage(bus.SelfId, 1, CastType.Broadcast, new Dictionary<string, object?>());

        await bus.HandleFanOutDeliveryAsync(selfMessage.ToNJson(), handler, CancellationToken.None);

        invoked.Should().BeFalse("a node must never re-process its own fan-out broadcast");
    }

    [Fact]
    public async Task HandleFanOutDeliveryAsync_ForeignMessage_IsDelivered()
    {
        await using var bus = await ActiveMqClusterMessageBusTestHelpers.CreateBusAsync();

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
        await using var bus = await ActiveMqClusterMessageBusTestHelpers.CreateBusAsync();

        var invoked = false;
        Func<ClusterFanOutMessage, CancellationToken, Task> handler = (_, _) => { invoked = true; return Task.CompletedTask; };

        var act = async () => await bus.HandleFanOutDeliveryAsync("{ not valid json ", handler, CancellationToken.None);

        await act.Should().NotThrowAsync("a single malformed delivery must not crash the subscription");
        invoked.Should().BeFalse();
    }

    [Fact]
    public async Task SubscribeAsync_DisposedHandle_ClosesItsOwnConsumerAndSession()
    {
        var connection = ActiveMqClusterMessageBusTestHelpers.CreateTrackedConnection(out var sessions);
        await using var bus = await ActiveMqClusterMessageBusTestHelpers.CreateBusAsync(connection: connection);

        var subscription = await bus.SubscribeAsync(Guid.NewGuid(), NoOpHandler);
        var subscriptionSession = sessions[3];

        await subscription.DisposeAsync();

        await subscriptionSession.Received(1).CloseAsync();
    }
}
