using System.Net;
using System.Text;
using FluentAssertions;
using NSubstitute;
using ThunderPropagator.Application.Channels.Cluster.Discovery;
using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.UdpClient;

namespace ThunderPropagator.UnitTests.UdpClient;

public class UdpClusterMessageBusSubscriptionSyncTests
{
    private static ClusterSubscriptionEvent CreateEvent(Guid originId) =>
        new(originId, "https://origin-node:5000/", ClusterSubscriptionEventKind.Added, [], DateTimeOffset.UtcNow);

    [Fact]
    public async Task PublishAsync_StampsOriginId_AndSendsToEveryDiscoveredPeer()
    {
        var socket = UdpClusterMessageBusTestHelpers.CreateSocketSubstitute();
        var discovery = UdpClusterMessageBusTestHelpers.CreateDiscoverySubstitute(
            [new ClusterNodeEntry(new Uri("https://peer1:5001/"), IsLeader: false)]);

        await using var bus = await UdpClusterMessageBusTestHelpers.CreateBusAsync(socket: socket, discovery: discovery);

        var channelKey = Guid.NewGuid();
        var subscriptionEvent = CreateEvent(Guid.Empty);

        await bus.PublishAsync(channelKey, subscriptionEvent);

        await socket.Received(1).SendDatagramAsync(
            Arg.Is<byte[]>(bytes => DatagramMatches(bytes, channelKey, bus.SelfId)),
            Arg.Any<IPEndPoint>(),
            Arg.Any<CancellationToken>());
    }

    private static bool DatagramMatches(byte[] bytes, Guid expectedChannelKey, Guid expectedSelfId)
    {
        var frame = Encoding.UTF8.GetString(bytes).FromNJson<UdpClusterFrame>();
        if (frame is null || frame.Kind != UdpClusterFrameKind.SubscriptionEvent)
            return false;

        var payload = frame.PayloadJson.FromNJson<UdpSubscriptionEventPayload>();
        return payload is not null && payload.ChannelKey == expectedChannelKey && payload.Event.OriginId == expectedSelfId;
    }

    [Fact]
    public async Task PublishAsync_OnePeerUnresolvable_DoesNotThrow()
    {
        var discovery = UdpClusterMessageBusTestHelpers.CreateDiscoverySubstitute(
            [new ClusterNodeEntry(new Uri("https://unresolvable-peer:5001/"), IsLeader: false)]);

        await using var bus = await UdpClusterMessageBusTestHelpers.CreateBusAsync(
            discovery: discovery,
            peerEndpointResolver: (_, _) => throw new InvalidOperationException("simulated DNS failure"));

        var act = async () => await bus.PublishAsync(Guid.NewGuid(), CreateEvent(Guid.Empty));

        await act.Should().NotThrowAsync("one unresolvable peer must not fail the whole subscription-event publish");
    }

    [Fact]
    public async Task HandleSubscriptionEventDeliveryAsync_SelfEcho_IsRejected()
    {
        await using var bus = await UdpClusterMessageBusTestHelpers.CreateBusAsync();

        var invoked = false;
        Func<ClusterSubscriptionEvent, CancellationToken, Task> handler = (_, _) => { invoked = true; return Task.CompletedTask; };

        var channelKey = Guid.NewGuid();
        await bus.SubscribeAsync(channelKey, handler);

        var payload = new UdpSubscriptionEventPayload(channelKey, CreateEvent(bus.SelfId));

        await bus.HandleSubscriptionEventDeliveryAsync(payload.ToNJson(), CancellationToken.None);

        invoked.Should().BeFalse("a node must never re-process its own subscription event");
    }

    [Fact]
    public async Task HandleSubscriptionEventDeliveryAsync_ForeignEventWithARegisteredHandler_IsDelivered()
    {
        await using var bus = await UdpClusterMessageBusTestHelpers.CreateBusAsync();

        ClusterSubscriptionEvent? received = null;
        Func<ClusterSubscriptionEvent, CancellationToken, Task> handler = (e, _) => { received = e; return Task.CompletedTask; };

        var channelKey = Guid.NewGuid();
        await bus.SubscribeAsync(channelKey, handler);

        var foreignEvent = CreateEvent(Guid.NewGuid());
        var payload = new UdpSubscriptionEventPayload(channelKey, foreignEvent);

        await bus.HandleSubscriptionEventDeliveryAsync(payload.ToNJson(), CancellationToken.None);

        received.Should().NotBeNull();
        received!.OriginId.Should().Be(foreignEvent.OriginId);
    }

    [Fact]
    public async Task HandleSubscriptionEventDeliveryAsync_NoRegisteredHandlerForTheChannel_DoesNotThrow()
    {
        await using var bus = await UdpClusterMessageBusTestHelpers.CreateBusAsync();

        var payload = new UdpSubscriptionEventPayload(Guid.NewGuid(), CreateEvent(Guid.NewGuid()));

        var act = async () => await bus.HandleSubscriptionEventDeliveryAsync(payload.ToNJson(), CancellationToken.None);

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task HandleSubscriptionEventDeliveryAsync_MalformedPayload_IsSkippedWithoutThrowing()
    {
        await using var bus = await UdpClusterMessageBusTestHelpers.CreateBusAsync();

        var act = async () => await bus.HandleSubscriptionEventDeliveryAsync("{ not valid json ", CancellationToken.None);

        await act.Should().NotThrowAsync("a single malformed delivery must not crash the receive loop");
    }

    [Fact]
    public async Task SubscribeAsync_DisposedHandle_StopsFurtherDelivery()
    {
        await using var bus = await UdpClusterMessageBusTestHelpers.CreateBusAsync();

        var invoked = false;
        Func<ClusterSubscriptionEvent, CancellationToken, Task> handler = (_, _) => { invoked = true; return Task.CompletedTask; };

        var channelKey = Guid.NewGuid();
        var subscription = await bus.SubscribeAsync(channelKey, handler);
        await subscription.DisposeAsync();

        var payload = new UdpSubscriptionEventPayload(channelKey, CreateEvent(Guid.NewGuid()));
        await bus.HandleSubscriptionEventDeliveryAsync(payload.ToNJson(), CancellationToken.None);

        invoked.Should().BeFalse("a disposed subscription must not receive further deliveries");
    }
}
