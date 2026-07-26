using Apache.NMS;
using FluentAssertions;
using NSubstitute;
using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.ActiveMQ;

namespace ThunderPropagator.UnitTests.ActiveMQ;

public class ActiveMqClusterMessageBusSubscriptionSyncTests
{
    private static readonly Func<ClusterSubscriptionEvent, CancellationToken, Task> NoOpHandler = (_, _) => Task.CompletedTask;

    private static ClusterSubscriptionEvent CreateEvent(Guid originId) => new(
        originId,
        "https://node1:5000/",
        ClusterSubscriptionEventKind.Added,
        [new ClusterSubscriptionDescriptor("sub-1", "req-1", "conn-1")],
        DateTimeOffset.UtcNow);

    [Fact]
    public async Task PublishAsync_StampsOriginId_AndSendsToTheSubscriptionEventTopic()
    {
        var connection = ActiveMqClusterMessageBusTestHelpers.CreateTrackedConnection(out var sessions);
        await using var bus = await ActiveMqClusterMessageBusTestHelpers.CreateBusAsync(connection: connection);

        var publishSession = sessions[0];
        var producer = await publishSession.CreateProducerAsync();

        var channelKey = Guid.NewGuid();
        var subscriptionEvent = CreateEvent(Guid.Empty);

        await bus.PublishAsync(channelKey, subscriptionEvent);

        var expectedTopic = ActiveMqTopicNaming.SubscriptionEventTopic("thunderpropagator.cluster", channelKey);

        await publishSession.Received(1).GetTopicAsync(expectedTopic);
        await producer.Received(1).SendAsync(
            Arg.Any<ITopic>(),
            Arg.Is<IMessage>(m => ((ITextMessage)m).Text!.FromNJson<ClusterSubscriptionEvent>()!.OriginId == bus.SelfId));
    }

    [Fact]
    public async Task SubscribeAsync_CreatesAConsumerOnTheChannelsSubscriptionEventTopic()
    {
        var connection = ActiveMqClusterMessageBusTestHelpers.CreateTrackedConnection(out var sessions);
        await using var bus = await ActiveMqClusterMessageBusTestHelpers.CreateBusAsync(connection: connection);

        var channelKey = Guid.NewGuid();
        var expectedTopic = ActiveMqTopicNaming.SubscriptionEventTopic("thunderpropagator.cluster", channelKey);

        await using var subscription = await bus.SubscribeAsync(channelKey, NoOpHandler);

        var subscriptionSession = sessions[3];

        await subscriptionSession.Received(1).GetTopicAsync(expectedTopic);
        await subscriptionSession.Received(1).CreateConsumerAsync(Arg.Any<IDestination>());
    }

    [Fact]
    public async Task HandleSubscriptionEventDeliveryAsync_SelfEcho_IsRejected()
    {
        await using var bus = await ActiveMqClusterMessageBusTestHelpers.CreateBusAsync();

        var invoked = false;
        Func<ClusterSubscriptionEvent, CancellationToken, Task> handler = (_, _) => { invoked = true; return Task.CompletedTask; };

        await bus.HandleSubscriptionEventDeliveryAsync(CreateEvent(bus.SelfId).ToNJson(), handler, CancellationToken.None);

        invoked.Should().BeFalse("a node must never re-process its own subscription-event broadcast");
    }

    [Fact]
    public async Task HandleSubscriptionEventDeliveryAsync_ForeignEvent_IsDelivered()
    {
        await using var bus = await ActiveMqClusterMessageBusTestHelpers.CreateBusAsync();

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
        await using var bus = await ActiveMqClusterMessageBusTestHelpers.CreateBusAsync();

        var invoked = false;
        Func<ClusterSubscriptionEvent, CancellationToken, Task> handler = (_, _) => { invoked = true; return Task.CompletedTask; };

        var act = async () => await bus.HandleSubscriptionEventDeliveryAsync("not json at all", handler, CancellationToken.None);

        await act.Should().NotThrowAsync("a single malformed delivery must not crash the subscription");
        invoked.Should().BeFalse();
    }

    [Fact]
    public async Task SubscribeAsync_DisposedHandle_ClosesItsOwnSession()
    {
        var connection = ActiveMqClusterMessageBusTestHelpers.CreateTrackedConnection(out var sessions);
        await using var bus = await ActiveMqClusterMessageBusTestHelpers.CreateBusAsync(connection: connection);

        var subscription = await bus.SubscribeAsync(Guid.NewGuid(), NoOpHandler);
        var subscriptionSession = sessions[3];

        await subscription.DisposeAsync();

        await subscriptionSession.Received(1).CloseAsync();
    }
}
