using System.Text;
using FluentAssertions;
using NSubstitute;
using RabbitMQ.Client;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.BuildingBlocks.Application.Enums;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.RabbitMQ;

namespace ThunderPropagator.UnitTests.RabbitMQ;

public class RabbitMqClusterMessageBusFanOutTests
{
    private static readonly Func<ClusterFanOutMessage, CancellationToken, Task> NoOpHandler = (_, _) => Task.CompletedTask;

    [Fact]
    public async Task PublishAsync_StampsOriginId_AndPublishesToTheFanOutExchange()
    {
        var connection = RabbitMqClusterMessageBusTestHelpers.CreateTrackedConnection(out var channels);
        await using var bus = await RabbitMqClusterMessageBusTestHelpers.CreateBusAsync(connection: connection);

        // channels[0] is the shared publish channel (created first, during EnsureInitializedAsync).
        var publishChannel = channels[0];

        var channelKey = Guid.NewGuid();
        var message = new ClusterFanOutMessage(Guid.Empty, 1, CastType.Broadcast, new Dictionary<string, object?>());

        await bus.PublishAsync(channelKey, message);

        var expectedExchange = RabbitMqTopicNaming.FanOutExchange("thunderpropagator.cluster", channelKey);

        await publishChannel.Received(1).ExchangeDeclareAsync(expectedExchange, ExchangeType.Fanout);
        await publishChannel.Received(1).BasicPublishAsync(
            expectedExchange,
            string.Empty,
            false,
            Arg.Any<BasicProperties>(),
            Arg.Is<ReadOnlyMemory<byte>>(body => Encoding.UTF8.GetString(body.Span).FromNJson<ClusterFanOutMessage>()!.OriginId == bus.SelfId));
    }

    [Fact]
    public async Task SubscribeAsync_DeclaresExchangeAndBindsAndConsumesAnAnonymousQueue()
    {
        var connection = RabbitMqClusterMessageBusTestHelpers.CreateTrackedConnection(out var channels);
        await using var bus = await RabbitMqClusterMessageBusTestHelpers.CreateBusAsync(connection: connection);

        var channelKey = Guid.NewGuid();
        var expectedExchange = RabbitMqTopicNaming.FanOutExchange("thunderpropagator.cluster", channelKey);

        await using var subscription = await bus.SubscribeAsync(channelKey, NoOpHandler);

        // channels[0..2] are the init-time channels (publish, request-listener, reply-listener);
        // channels[3] is the one this SubscribeAsync call created for itself.
        var subscriptionChannel = channels[3];

        await subscriptionChannel.Received(1).ExchangeDeclareAsync(expectedExchange, ExchangeType.Fanout);
        await subscriptionChannel.Received(1).QueueDeclareAsync(string.Empty, false, true, true, null);
        await subscriptionChannel.Received(1).QueueBindAsync(Arg.Any<string>(), expectedExchange, string.Empty, null);
        await subscriptionChannel.Received(1).BasicConsumeAsync(Arg.Any<string>(), true, Arg.Any<IAsyncBasicConsumer>());
    }

    [Fact]
    public async Task HandleFanOutDeliveryAsync_SelfEcho_IsRejected()
    {
        await using var bus = await RabbitMqClusterMessageBusTestHelpers.CreateBusAsync();

        var invoked = false;
        Func<ClusterFanOutMessage, CancellationToken, Task> handler = (_, _) => { invoked = true; return Task.CompletedTask; };

        var selfMessage = new ClusterFanOutMessage(bus.SelfId, 1, CastType.Broadcast, new Dictionary<string, object?>());
        var body = Encoding.UTF8.GetBytes(selfMessage.ToNJson());

        await bus.HandleFanOutDeliveryAsync(body, handler, CancellationToken.None);

        invoked.Should().BeFalse("a node must never re-process its own fan-out broadcast");
    }

    [Fact]
    public async Task HandleFanOutDeliveryAsync_ForeignMessage_IsDelivered()
    {
        await using var bus = await RabbitMqClusterMessageBusTestHelpers.CreateBusAsync();

        ClusterFanOutMessage? received = null;
        Func<ClusterFanOutMessage, CancellationToken, Task> handler = (m, _) => { received = m; return Task.CompletedTask; };

        var foreignMessage = new ClusterFanOutMessage(Guid.NewGuid(), 1, CastType.Broadcast, new Dictionary<string, object?>());
        var body = Encoding.UTF8.GetBytes(foreignMessage.ToNJson());

        await bus.HandleFanOutDeliveryAsync(body, handler, CancellationToken.None);

        received.Should().NotBeNull();
        received!.OriginId.Should().Be(foreignMessage.OriginId);
    }

    [Fact]
    public async Task HandleFanOutDeliveryAsync_MalformedBody_IsSkippedWithoutThrowing()
    {
        await using var bus = await RabbitMqClusterMessageBusTestHelpers.CreateBusAsync();

        var invoked = false;
        Func<ClusterFanOutMessage, CancellationToken, Task> handler = (_, _) => { invoked = true; return Task.CompletedTask; };

        var malformedBody = Encoding.UTF8.GetBytes("{ not valid json ");

        var act = async () => await bus.HandleFanOutDeliveryAsync(malformedBody, handler, CancellationToken.None);

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
