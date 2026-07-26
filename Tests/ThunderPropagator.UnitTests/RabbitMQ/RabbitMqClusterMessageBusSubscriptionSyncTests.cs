using System.Text;
using FluentAssertions;
using NSubstitute;
using RabbitMQ.Client;
using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.RabbitMQ;

namespace ThunderPropagator.UnitTests.RabbitMQ;

public class RabbitMqClusterMessageBusSubscriptionSyncTests
{
    private static readonly Func<ClusterSubscriptionEvent, CancellationToken, Task> NoOpHandler = (_, _) => Task.CompletedTask;

    private static ClusterSubscriptionEvent CreateEvent(Guid originId) => new(
        originId,
        "https://node1:5000/",
        ClusterSubscriptionEventKind.Added,
        [new ClusterSubscriptionDescriptor("sub-1", "req-1", "conn-1")],
        DateTimeOffset.UtcNow);

    [Fact]
    public async Task PublishAsync_StampsOriginId_AndPublishesToTheSubscriptionEventExchange()
    {
        var connection = RabbitMqClusterMessageBusTestHelpers.CreateTrackedConnection(out var channels);
        await using var bus = await RabbitMqClusterMessageBusTestHelpers.CreateBusAsync(connection: connection);

        var publishChannel = channels[0];
        var channelKey = Guid.NewGuid();
        var subscriptionEvent = CreateEvent(Guid.Empty);

        await bus.PublishAsync(channelKey, subscriptionEvent);

        var expectedExchange = RabbitMqTopicNaming.SubscriptionEventExchange("thunderpropagator.cluster", channelKey);

        await publishChannel.Received(1).ExchangeDeclareAsync(expectedExchange, ExchangeType.Fanout);
        await publishChannel.Received(1).BasicPublishAsync(
            expectedExchange,
            string.Empty,
            false,
            Arg.Any<BasicProperties>(),
            Arg.Is<ReadOnlyMemory<byte>>(body => Encoding.UTF8.GetString(body.Span).FromNJson<ClusterSubscriptionEvent>()!.OriginId == bus.SelfId));
    }

    [Fact]
    public async Task SubscribeAsync_DeclaresExchangeAndBindsAndConsumesAnAnonymousQueue()
    {
        var connection = RabbitMqClusterMessageBusTestHelpers.CreateTrackedConnection(out var channels);
        await using var bus = await RabbitMqClusterMessageBusTestHelpers.CreateBusAsync(connection: connection);

        var channelKey = Guid.NewGuid();
        var expectedExchange = RabbitMqTopicNaming.SubscriptionEventExchange("thunderpropagator.cluster", channelKey);

        await using var subscription = await bus.SubscribeAsync(channelKey, NoOpHandler);
        var subscriptionChannel = channels[3];

        await subscriptionChannel.Received(1).ExchangeDeclareAsync(expectedExchange, ExchangeType.Fanout);
        await subscriptionChannel.Received(1).QueueDeclareAsync(string.Empty, false, true, true, null);
        await subscriptionChannel.Received(1).QueueBindAsync(Arg.Any<string>(), expectedExchange, string.Empty, null);
        await subscriptionChannel.Received(1).BasicConsumeAsync(Arg.Any<string>(), true, Arg.Any<IAsyncBasicConsumer>());
    }

    [Fact]
    public async Task HandleSubscriptionEventDeliveryAsync_SelfEcho_IsRejected()
    {
        await using var bus = await RabbitMqClusterMessageBusTestHelpers.CreateBusAsync();

        var invoked = false;
        Func<ClusterSubscriptionEvent, CancellationToken, Task> handler = (_, _) => { invoked = true; return Task.CompletedTask; };

        var body = Encoding.UTF8.GetBytes(CreateEvent(bus.SelfId).ToNJson());

        await bus.HandleSubscriptionEventDeliveryAsync(body, handler, CancellationToken.None);

        invoked.Should().BeFalse("a node must never re-process its own subscription-event broadcast");
    }

    [Fact]
    public async Task HandleSubscriptionEventDeliveryAsync_ForeignEvent_IsDelivered()
    {
        await using var bus = await RabbitMqClusterMessageBusTestHelpers.CreateBusAsync();

        ClusterSubscriptionEvent? received = null;
        Func<ClusterSubscriptionEvent, CancellationToken, Task> handler = (e, _) => { received = e; return Task.CompletedTask; };

        var foreignEvent = CreateEvent(Guid.NewGuid());
        var body = Encoding.UTF8.GetBytes(foreignEvent.ToNJson());

        await bus.HandleSubscriptionEventDeliveryAsync(body, handler, CancellationToken.None);

        received.Should().NotBeNull();
        received!.OriginId.Should().Be(foreignEvent.OriginId);
    }

    [Fact]
    public async Task HandleSubscriptionEventDeliveryAsync_MalformedBody_IsSkippedWithoutThrowing()
    {
        await using var bus = await RabbitMqClusterMessageBusTestHelpers.CreateBusAsync();

        var invoked = false;
        Func<ClusterSubscriptionEvent, CancellationToken, Task> handler = (_, _) => { invoked = true; return Task.CompletedTask; };

        var malformedBody = Encoding.UTF8.GetBytes("not json at all");

        var act = async () => await bus.HandleSubscriptionEventDeliveryAsync(malformedBody, handler, CancellationToken.None);

        await act.Should().NotThrowAsync("a single malformed delivery must not crash the subscription");
        invoked.Should().BeFalse();
    }

    [Fact]
    public async Task SubscribeAsync_DisposedHandle_CancelsAndClosesItsOwnChannel()
    {
        var connection = RabbitMqClusterMessageBusTestHelpers.CreateTrackedConnection(out var channels);
        await using var bus = await RabbitMqClusterMessageBusTestHelpers.CreateBusAsync(connection: connection);

        var subscription = await bus.SubscribeAsync(Guid.NewGuid(), NoOpHandler);
        var subscriptionChannel = channels[3];

        await subscription.DisposeAsync();

        await subscriptionChannel.Received(1).BasicCancelAsync(Arg.Any<string>());
        await subscriptionChannel.Received(1).CloseAsync();
        await subscriptionChannel.Received(1).DisposeAsync();
    }
}
