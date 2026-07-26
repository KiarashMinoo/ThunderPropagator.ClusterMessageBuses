using System.Net.WebSockets;
using System.Text;
using FluentAssertions;
using NSubstitute;
using ThunderPropagator.Application.Channels.Cluster.Discovery;
using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.WebSocket;

namespace ThunderPropagator.UnitTests.WebSocket;

public class WebSocketClusterMessageBusSubscriptionSyncTests
{
    private static ClusterSubscriptionEvent CreateEvent(Guid originId) => new(
        originId,
        "https://node1:5000/",
        ClusterSubscriptionEventKind.Added,
        [new ClusterSubscriptionDescriptor("sub-1", "req-1", "conn-1")],
        DateTimeOffset.UtcNow);

    [Fact]
    public async Task PublishAsync_StampsOriginId_AndSendsToEveryDiscoveredPeer()
    {
        var peerSocket = WebSocketClusterMessageBusTestHelpers.CreateSocketSubstitute();
        var discovery = WebSocketClusterMessageBusTestHelpers.CreateDiscoverySubstitute(
            [new ClusterNodeEntry(new Uri("https://peer1:5001/"), IsLeader: false)]);

        await using var bus = await WebSocketClusterMessageBusTestHelpers.CreateBusAsync(
            discovery: discovery,
            outboundSocketFactory: WebSocketClusterMessageBusTestHelpers.OutboundSocketFactoryReturning(peerSocket));

        var channelKey = Guid.NewGuid();
        var subscriptionEvent = CreateEvent(Guid.Empty);

        await bus.PublishAsync(channelKey, subscriptionEvent);

        await peerSocket.Received(1).SendAsync(
            Arg.Is<ArraySegment<byte>>(segment => MatchesSubscriptionEvent(segment, channelKey, bus.SelfId)),
            WebSocketMessageType.Text,
            true,
            Arg.Any<CancellationToken>());
    }

    private static bool MatchesSubscriptionEvent(ArraySegment<byte> segment, Guid expectedChannelKey, Guid expectedSelfId)
    {
        var frame = Encoding.UTF8.GetString(segment).FromNJson<WebSocketClusterFrame>();
        if (frame is null || frame.Kind != WebSocketClusterFrameKind.SubscriptionEvent)
            return false;

        var payload = frame.PayloadJson.FromNJson<WebSocketSubscriptionEventPayload>();
        return payload is not null && payload.ChannelKey == expectedChannelKey && payload.Event.OriginId == expectedSelfId;
    }

    [Fact]
    public async Task HandleSubscriptionEventDeliveryAsync_SelfEcho_IsRejected()
    {
        await using var bus = await WebSocketClusterMessageBusTestHelpers.CreateBusAsync();

        var invoked = false;
        Func<ClusterSubscriptionEvent, CancellationToken, Task> handler = (_, _) => { invoked = true; return Task.CompletedTask; };

        var channelKey = Guid.NewGuid();
        await bus.SubscribeAsync(channelKey, handler);

        var payload = new WebSocketSubscriptionEventPayload(channelKey, CreateEvent(bus.SelfId));

        await bus.HandleSubscriptionEventDeliveryAsync(payload.ToNJson(), CancellationToken.None);

        invoked.Should().BeFalse("a node must never re-process its own subscription-event broadcast");
    }

    [Fact]
    public async Task HandleSubscriptionEventDeliveryAsync_ForeignEventWithARegisteredHandler_IsDelivered()
    {
        await using var bus = await WebSocketClusterMessageBusTestHelpers.CreateBusAsync();

        ClusterSubscriptionEvent? received = null;
        Func<ClusterSubscriptionEvent, CancellationToken, Task> handler = (e, _) => { received = e; return Task.CompletedTask; };

        var channelKey = Guid.NewGuid();
        await bus.SubscribeAsync(channelKey, handler);

        var foreignEvent = CreateEvent(Guid.NewGuid());
        var payload = new WebSocketSubscriptionEventPayload(channelKey, foreignEvent);

        await bus.HandleSubscriptionEventDeliveryAsync(payload.ToNJson(), CancellationToken.None);

        received.Should().NotBeNull();
        received!.OriginId.Should().Be(foreignEvent.OriginId);
    }

    [Fact]
    public async Task HandleSubscriptionEventDeliveryAsync_MalformedPayload_IsSkippedWithoutThrowing()
    {
        await using var bus = await WebSocketClusterMessageBusTestHelpers.CreateBusAsync();

        var act = async () => await bus.HandleSubscriptionEventDeliveryAsync("not json at all", CancellationToken.None);

        await act.Should().NotThrowAsync("a single malformed delivery must not crash the connection");
    }

    [Fact]
    public async Task SubscribeAsync_DisposedHandle_StopsFurtherDelivery()
    {
        await using var bus = await WebSocketClusterMessageBusTestHelpers.CreateBusAsync();

        var invoked = false;
        Func<ClusterSubscriptionEvent, CancellationToken, Task> handler = (_, _) => { invoked = true; return Task.CompletedTask; };

        var channelKey = Guid.NewGuid();
        var subscription = await bus.SubscribeAsync(channelKey, handler);
        await subscription.DisposeAsync();

        var payload = new WebSocketSubscriptionEventPayload(channelKey, CreateEvent(Guid.NewGuid()));

        await bus.HandleSubscriptionEventDeliveryAsync(payload.ToNJson(), CancellationToken.None);

        invoked.Should().BeFalse("a disposed subscription must not receive further deliveries");
    }
}
