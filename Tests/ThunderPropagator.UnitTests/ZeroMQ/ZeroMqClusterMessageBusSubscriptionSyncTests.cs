using FluentAssertions;
using NSubstitute;
using ThunderPropagator.Application.Channels.Cluster.Discovery;
using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.ZeroMQ;

namespace ThunderPropagator.UnitTests.ZeroMQ;

public class ZeroMqClusterMessageBusSubscriptionSyncTests
{
    private static ClusterSubscriptionEvent CreateEvent(Guid originId) =>
        new(originId, "https://origin-node:5000/", ClusterSubscriptionEventKind.Added, [], DateTimeOffset.UtcNow);

    [Fact]
    public async Task PublishAsync_StampsOriginId_AndSendsToEveryDiscoveredPeer()
    {
        var connection = ZeroMqClusterMessageBusTestHelpers.CreatePeerConnectionSubstitute();
        var discovery = ZeroMqClusterMessageBusTestHelpers.CreateDiscoverySubstitute(
            [new ClusterNodeEntry(new Uri("https://peer1:5001/"), IsLeader: false)]);

        await using var bus = await ZeroMqClusterMessageBusTestHelpers.CreateBusAsync(
            discovery: discovery,
            peerConnectionFactory: (_, _) => Task.FromResult(connection));

        var channelKey = Guid.NewGuid();
        await bus.PublishAsync(channelKey, CreateEvent(Guid.Empty));

        connection.Received(1).SendFrame(Arg.Is<ZeroMqClusterFrame>(f =>
            f.Kind == ZeroMqClusterFrameKind.SubscriptionEvent &&
            f.PayloadJson.FromNJson<ZeroMqSubscriptionEventPayload>()!.ChannelKey == channelKey &&
            f.PayloadJson.FromNJson<ZeroMqSubscriptionEventPayload>()!.Event.OriginId == bus.SelfId));
    }

    [Fact]
    public async Task PublishAsync_OnePeerUnreachable_DoesNotThrow()
    {
        var discovery = ZeroMqClusterMessageBusTestHelpers.CreateDiscoverySubstitute(
            [new ClusterNodeEntry(new Uri("https://unreachable-peer:5001/"), IsLeader: false)]);

        await using var bus = await ZeroMqClusterMessageBusTestHelpers.CreateBusAsync(
            discovery: discovery,
            peerConnectionFactory: (_, _) => throw new InvalidOperationException("simulated connect failure"));

        var act = async () => await bus.PublishAsync(Guid.NewGuid(), CreateEvent(Guid.Empty));

        await act.Should().NotThrowAsync("one unreachable peer must not fail the whole subscription-sync publish");
    }

    [Fact]
    public async Task HandleSubscriptionEventDeliveryAsync_SelfEcho_IsRejected()
    {
        await using var bus = await ZeroMqClusterMessageBusTestHelpers.CreateBusAsync();

        var invoked = false;
        Func<ClusterSubscriptionEvent, CancellationToken, Task> handler = (_, _) => { invoked = true; return Task.CompletedTask; };

        var channelKey = Guid.NewGuid();
        await bus.SubscribeAsync(channelKey, handler);

        var selfEvent = CreateEvent(bus.SelfId);
        var payload = new ZeroMqSubscriptionEventPayload(channelKey, selfEvent);

        await bus.HandleSubscriptionEventDeliveryAsync(payload.ToNJson(), CancellationToken.None);

        invoked.Should().BeFalse("a node must never re-process its own subscription event");
    }

    [Fact]
    public async Task HandleSubscriptionEventDeliveryAsync_ForeignEvent_IsDelivered()
    {
        await using var bus = await ZeroMqClusterMessageBusTestHelpers.CreateBusAsync();

        ClusterSubscriptionEvent? received = null;
        Func<ClusterSubscriptionEvent, CancellationToken, Task> handler = (e, _) => { received = e; return Task.CompletedTask; };

        var channelKey = Guid.NewGuid();
        await bus.SubscribeAsync(channelKey, handler);

        var foreignEvent = CreateEvent(Guid.NewGuid());
        var payload = new ZeroMqSubscriptionEventPayload(channelKey, foreignEvent);

        await bus.HandleSubscriptionEventDeliveryAsync(payload.ToNJson(), CancellationToken.None);

        received.Should().NotBeNull();
        received!.OriginId.Should().Be(foreignEvent.OriginId);
    }

    [Fact]
    public async Task HandleSubscriptionEventDeliveryAsync_MalformedPayload_IsSkippedWithoutThrowing()
    {
        await using var bus = await ZeroMqClusterMessageBusTestHelpers.CreateBusAsync();

        var act = async () => await bus.HandleSubscriptionEventDeliveryAsync("{ not valid json ", CancellationToken.None);

        await act.Should().NotThrowAsync("a single malformed delivery must not crash the connection");
    }

    [Fact]
    public async Task SubscribeAsync_DisposedHandle_StopsFurtherDelivery()
    {
        await using var bus = await ZeroMqClusterMessageBusTestHelpers.CreateBusAsync();

        var invoked = false;
        Func<ClusterSubscriptionEvent, CancellationToken, Task> handler = (_, _) => { invoked = true; return Task.CompletedTask; };

        var channelKey = Guid.NewGuid();
        var subscription = await bus.SubscribeAsync(channelKey, handler);
        await subscription.DisposeAsync();

        var payload = new ZeroMqSubscriptionEventPayload(channelKey, CreateEvent(Guid.NewGuid()));

        await bus.HandleSubscriptionEventDeliveryAsync(payload.ToNJson(), CancellationToken.None);

        invoked.Should().BeFalse("a disposed subscription must not receive further deliveries");
    }
}
