using FluentAssertions;
using NSubstitute;
using ThunderPropagator.Application.Channels.Cluster.Discovery;
using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.TcpSocket;

namespace ThunderPropagator.UnitTests.TcpSocket;

public class TcpClusterMessageBusSubscriptionSyncTests
{
    private static ClusterSubscriptionEvent CreateEvent(Guid originId) =>
        new(originId, "https://origin-node:5000/", ClusterSubscriptionEventKind.Added, [], DateTimeOffset.UtcNow);

    [Fact]
    public async Task PublishAsync_StampsOriginId_AndSendsToEveryDiscoveredPeer()
    {
        var peerConnection = TcpClusterMessageBusTestHelpers.CreateConnectionSubstitute();
        var discovery = TcpClusterMessageBusTestHelpers.CreateDiscoverySubstitute(
            [new ClusterNodeEntry(new Uri("https://peer1:5001/"), IsLeader: false)]);

        await using var bus = await TcpClusterMessageBusTestHelpers.CreateBusAsync(
            discovery: discovery,
            outboundConnectionFactory: TcpClusterMessageBusTestHelpers.OutboundConnectionFactoryReturning(peerConnection));

        var channelKey = Guid.NewGuid();
        var subscriptionEvent = CreateEvent(Guid.Empty);

        await bus.PublishAsync(channelKey, subscriptionEvent);

        await peerConnection.Received(1).SendFrameAsync(
            Arg.Is<TcpClusterFrame>(f => f.Kind == TcpClusterFrameKind.SubscriptionEvent &&
                f.PayloadJson.FromNJson<TcpSubscriptionEventPayload>()!.ChannelKey == channelKey &&
                f.PayloadJson.FromNJson<TcpSubscriptionEventPayload>()!.Event.OriginId == bus.SelfId),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task PublishAsync_OnePeerUnreachable_DoesNotThrow()
    {
        var discovery = TcpClusterMessageBusTestHelpers.CreateDiscoverySubstitute(
            [new ClusterNodeEntry(new Uri("https://unreachable-peer:5001/"), IsLeader: false)]);

        await using var bus = await TcpClusterMessageBusTestHelpers.CreateBusAsync(
            discovery: discovery,
            outboundConnectionFactory: (_, _) => throw new InvalidOperationException("simulated connect failure"));

        var act = async () => await bus.PublishAsync(Guid.NewGuid(), CreateEvent(Guid.Empty));

        await act.Should().NotThrowAsync("one unreachable peer must not fail the whole subscription-event publish");
    }

    [Fact]
    public async Task HandleSubscriptionEventDeliveryAsync_SelfEcho_IsRejected()
    {
        await using var bus = await TcpClusterMessageBusTestHelpers.CreateBusAsync();

        var invoked = false;
        Func<ClusterSubscriptionEvent, CancellationToken, Task> handler = (_, _) => { invoked = true; return Task.CompletedTask; };

        var channelKey = Guid.NewGuid();
        await bus.SubscribeAsync(channelKey, handler);

        var payload = new TcpSubscriptionEventPayload(channelKey, CreateEvent(bus.SelfId));

        await bus.HandleSubscriptionEventDeliveryAsync(payload.ToNJson(), CancellationToken.None);

        invoked.Should().BeFalse("a node must never re-process its own subscription event");
    }

    [Fact]
    public async Task HandleSubscriptionEventDeliveryAsync_ForeignEventWithARegisteredHandler_IsDelivered()
    {
        await using var bus = await TcpClusterMessageBusTestHelpers.CreateBusAsync();

        ClusterSubscriptionEvent? received = null;
        Func<ClusterSubscriptionEvent, CancellationToken, Task> handler = (e, _) => { received = e; return Task.CompletedTask; };

        var channelKey = Guid.NewGuid();
        await bus.SubscribeAsync(channelKey, handler);

        var foreignEvent = CreateEvent(Guid.NewGuid());
        var payload = new TcpSubscriptionEventPayload(channelKey, foreignEvent);

        await bus.HandleSubscriptionEventDeliveryAsync(payload.ToNJson(), CancellationToken.None);

        received.Should().NotBeNull();
        received!.OriginId.Should().Be(foreignEvent.OriginId);
    }

    [Fact]
    public async Task HandleSubscriptionEventDeliveryAsync_NoRegisteredHandlerForTheChannel_DoesNotThrow()
    {
        await using var bus = await TcpClusterMessageBusTestHelpers.CreateBusAsync();

        var payload = new TcpSubscriptionEventPayload(Guid.NewGuid(), CreateEvent(Guid.NewGuid()));

        var act = async () => await bus.HandleSubscriptionEventDeliveryAsync(payload.ToNJson(), CancellationToken.None);

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task HandleSubscriptionEventDeliveryAsync_MalformedPayload_IsSkippedWithoutThrowing()
    {
        await using var bus = await TcpClusterMessageBusTestHelpers.CreateBusAsync();

        var act = async () => await bus.HandleSubscriptionEventDeliveryAsync("{ not valid json ", CancellationToken.None);

        await act.Should().NotThrowAsync("a single malformed delivery must not crash the connection");
    }

    [Fact]
    public async Task SubscribeAsync_DisposedHandle_StopsFurtherDelivery()
    {
        await using var bus = await TcpClusterMessageBusTestHelpers.CreateBusAsync();

        var invoked = false;
        Func<ClusterSubscriptionEvent, CancellationToken, Task> handler = (_, _) => { invoked = true; return Task.CompletedTask; };

        var channelKey = Guid.NewGuid();
        var subscription = await bus.SubscribeAsync(channelKey, handler);
        await subscription.DisposeAsync();

        var payload = new TcpSubscriptionEventPayload(channelKey, CreateEvent(Guid.NewGuid()));
        await bus.HandleSubscriptionEventDeliveryAsync(payload.ToNJson(), CancellationToken.None);

        invoked.Should().BeFalse("a disposed subscription must not receive further deliveries");
    }
}
