using Azure.Messaging.ServiceBus;
using FluentAssertions;
using NSubstitute;
using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.AzureServiceBus;

namespace ThunderPropagator.UnitTests.AzureServiceBus;

public class AzureServiceBusClusterMessageBusSubscriptionSyncTests
{
    private static ClusterSubscriptionEvent CreateEvent(Guid originId) =>
        new(originId, "https://origin-node:5000/", ClusterSubscriptionEventKind.Added, [], DateTimeOffset.UtcNow);

    [Fact]
    public async Task PublishAsync_StampsOriginId_AndPublishesToTheChannelsTopic()
    {
        var sender = AzureServiceBusClusterMessageBusTestHelpers.CreateSenderSubstitute();
        var client = AzureServiceBusClusterMessageBusTestHelpers.CreateClientSubstitute(senderFactory: _ => sender);
        await using var bus = await AzureServiceBusClusterMessageBusTestHelpers.CreateBusAsync(client: client);

        var channelKey = Guid.NewGuid();
        var subscriptionEvent = CreateEvent(Guid.Empty);

        await bus.PublishAsync(channelKey, subscriptionEvent);

        var expectedTopicName = $"tp-cluster-subscriptions-{channelKey:N}";

        await sender.Received(1).SendMessageAsync(
            Arg.Is<ServiceBusMessage>(m => m.Body.ToString().FromNJson<ClusterSubscriptionEvent>()!.OriginId == bus.SelfId),
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

        var act = async () => await bus.PublishAsync(Guid.NewGuid(), CreateEvent(Guid.Empty));

        await act.Should().NotThrowAsync("a publish failure must not propagate out of PublishAsync");
    }

    [Fact]
    public async Task SubscribeAsync_CreatesTheChannelsTopicAndThisNodesSubscription()
    {
        var admin = AzureServiceBusClusterMessageBusTestHelpers.CreateAdminSubstitute();
        await using var bus = await AzureServiceBusClusterMessageBusTestHelpers.CreateBusAsync(admin: admin);

        var channelKey = Guid.NewGuid();
        Func<ClusterSubscriptionEvent, CancellationToken, Task> handler = (_, _) => Task.CompletedTask;

        await using var subscription = await bus.SubscribeAsync(channelKey, handler);

        var expectedTopicName = $"tp-cluster-subscriptions-{channelKey:N}";

        await admin.Received(1).CreateTopicAsync(expectedTopicName, Arg.Any<CancellationToken>());
        await admin.Received(1).CreateSubscriptionAsync(expectedTopicName, Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task SubscribeAsync_DisposedHandle_DeletesTheSubscription()
    {
        var admin = AzureServiceBusClusterMessageBusTestHelpers.CreateAdminSubstitute();
        await using var bus = await AzureServiceBusClusterMessageBusTestHelpers.CreateBusAsync(admin: admin);

        var channelKey = Guid.NewGuid();
        Func<ClusterSubscriptionEvent, CancellationToken, Task> handler = (_, _) => Task.CompletedTask;

        var subscription = await bus.SubscribeAsync(channelKey, handler);
        await subscription.DisposeAsync();

        await admin.Received(1).DeleteSubscriptionAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task HandleSubscriptionEventDeliveryAsync_SelfEcho_IsRejected()
    {
        await using var bus = await AzureServiceBusClusterMessageBusTestHelpers.CreateBusAsync();

        var invoked = false;
        Func<ClusterSubscriptionEvent, CancellationToken, Task> handler = (_, _) => { invoked = true; return Task.CompletedTask; };

        var selfEvent = CreateEvent(bus.SelfId);

        await bus.HandleSubscriptionEventDeliveryAsync(selfEvent.ToNJson(), handler, CancellationToken.None);

        invoked.Should().BeFalse("a node must never re-process its own subscription event");
    }

    [Fact]
    public async Task HandleSubscriptionEventDeliveryAsync_ForeignEvent_IsDelivered()
    {
        await using var bus = await AzureServiceBusClusterMessageBusTestHelpers.CreateBusAsync();

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
        await using var bus = await AzureServiceBusClusterMessageBusTestHelpers.CreateBusAsync();

        Func<ClusterSubscriptionEvent, CancellationToken, Task> handler = (_, _) => Task.CompletedTask;

        var act = async () => await bus.HandleSubscriptionEventDeliveryAsync("{ not valid json ", handler, CancellationToken.None);

        await act.Should().NotThrowAsync("a single malformed delivery must not crash the poll loop");
    }
}
