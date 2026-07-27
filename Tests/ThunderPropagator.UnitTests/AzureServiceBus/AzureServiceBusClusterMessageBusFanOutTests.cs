using Azure.Messaging.ServiceBus;
using FluentAssertions;
using NSubstitute;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.BuildingBlocks.Application.Enums;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.AzureServiceBus;

namespace ThunderPropagator.UnitTests.AzureServiceBus;

public class AzureServiceBusClusterMessageBusFanOutTests
{
    [Fact]
    public async Task PublishAsync_StampsOriginId_AndPublishesToTheChannelsTopic()
    {
        var sender = AzureServiceBusClusterMessageBusTestHelpers.CreateSenderSubstitute();
        var client = AzureServiceBusClusterMessageBusTestHelpers.CreateClientSubstitute(senderFactory: _ => sender);
        await using var bus = await AzureServiceBusClusterMessageBusTestHelpers.CreateBusAsync(client: client);

        var channelKey = Guid.NewGuid();
        var message = new ClusterFanOutMessage(Guid.Empty, 1, CastType.Broadcast, new Dictionary<string, object?>());

        await bus.PublishAsync(channelKey, message);

        var expectedTopicName = $"tp-cluster-fanout-{channelKey:N}";

        await sender.Received(1).SendMessageAsync(
            Arg.Is<ServiceBusMessage>(m => m.Body.ToString().FromNJson<ClusterFanOutMessage>()!.OriginId == bus.SelfId),
            Arg.Any<CancellationToken>());

        client.Received().CreateSender(expectedTopicName);
    }

    [Fact]
    public async Task PublishAsync_SendThrows_DoesNotThrow()
    {
        var sender = AzureServiceBusClusterMessageBusTestHelpers.CreateSenderSubstitute();
        sender.SendMessageAsync(Arg.Any<ServiceBusMessage>(), Arg.Any<CancellationToken>())
            .Returns(_ => throw new InvalidOperationException("simulated Service Bus failure"));
        var client = AzureServiceBusClusterMessageBusTestHelpers.CreateClientSubstitute(senderFactory: _ => sender);

        await using var bus = await AzureServiceBusClusterMessageBusTestHelpers.CreateBusAsync(client: client);

        var message = new ClusterFanOutMessage(Guid.Empty, 1, CastType.Broadcast, new Dictionary<string, object?>());
        var act = async () => await bus.PublishAsync(Guid.NewGuid(), message);

        await act.Should().NotThrowAsync("a publish failure must not propagate out of PublishAsync");
    }

    [Fact]
    public async Task SubscribeAsync_CreatesTheChannelsTopicAndThisNodesSubscription()
    {
        var admin = AzureServiceBusClusterMessageBusTestHelpers.CreateAdminSubstitute();
        await using var bus = await AzureServiceBusClusterMessageBusTestHelpers.CreateBusAsync(admin: admin);

        var channelKey = Guid.NewGuid();
        Func<ClusterFanOutMessage, CancellationToken, Task> handler = (_, _) => Task.CompletedTask;

        await using var subscription = await bus.SubscribeAsync(channelKey, handler);

        var expectedTopicName = $"tp-cluster-fanout-{channelKey:N}";

        await admin.Received(1).CreateTopicAsync(expectedTopicName, Arg.Any<CancellationToken>());
        await admin.Received(1).CreateSubscriptionAsync(expectedTopicName, Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task SubscribeAsync_DisposedHandle_DeletesTheSubscription()
    {
        var admin = AzureServiceBusClusterMessageBusTestHelpers.CreateAdminSubstitute();
        await using var bus = await AzureServiceBusClusterMessageBusTestHelpers.CreateBusAsync(admin: admin);

        var channelKey = Guid.NewGuid();
        Func<ClusterFanOutMessage, CancellationToken, Task> handler = (_, _) => Task.CompletedTask;

        var subscription = await bus.SubscribeAsync(channelKey, handler);
        await subscription.DisposeAsync();

        await admin.Received(1).DeleteSubscriptionAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task HandleFanOutDeliveryAsync_SelfEcho_IsRejected()
    {
        await using var bus = await AzureServiceBusClusterMessageBusTestHelpers.CreateBusAsync();

        var invoked = false;
        Func<ClusterFanOutMessage, CancellationToken, Task> handler = (_, _) => { invoked = true; return Task.CompletedTask; };

        var selfMessage = new ClusterFanOutMessage(bus.SelfId, 1, CastType.Broadcast, new Dictionary<string, object?>());

        await bus.HandleFanOutDeliveryAsync(selfMessage.ToNJson(), handler, CancellationToken.None);

        invoked.Should().BeFalse("a node must never re-process its own fan-out broadcast");
    }

    [Fact]
    public async Task HandleFanOutDeliveryAsync_ForeignMessage_IsDelivered()
    {
        await using var bus = await AzureServiceBusClusterMessageBusTestHelpers.CreateBusAsync();

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
        await using var bus = await AzureServiceBusClusterMessageBusTestHelpers.CreateBusAsync();

        Func<ClusterFanOutMessage, CancellationToken, Task> handler = (_, _) => Task.CompletedTask;

        var act = async () => await bus.HandleFanOutDeliveryAsync("{ not valid json ", handler, CancellationToken.None);

        await act.Should().NotThrowAsync("a single malformed delivery must not crash the poll loop");
    }

    [Fact]
    public async Task HandleFanOutDeliveryAsync_HandlerThrows_DoesNotPropagate()
    {
        await using var bus = await AzureServiceBusClusterMessageBusTestHelpers.CreateBusAsync();

        Func<ClusterFanOutMessage, CancellationToken, Task> handler = (_, _) => throw new InvalidOperationException("handler blew up");
        var foreignMessage = new ClusterFanOutMessage(Guid.NewGuid(), 1, CastType.Broadcast, new Dictionary<string, object?>());

        var act = async () => await bus.HandleFanOutDeliveryAsync(foreignMessage.ToNJson(), handler, CancellationToken.None);

        await act.Should().NotThrowAsync();
    }
}
