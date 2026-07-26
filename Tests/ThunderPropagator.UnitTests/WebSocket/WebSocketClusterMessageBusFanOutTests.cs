using System.Net.WebSockets;
using FluentAssertions;
using NSubstitute;
using ThunderPropagator.Application.Channels.Cluster.Discovery;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.BuildingBlocks.Application.Enums;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.WebSocket;

namespace ThunderPropagator.UnitTests.WebSocket;

public class WebSocketClusterMessageBusFanOutTests
{
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
        var message = new ClusterFanOutMessage(Guid.Empty, 1, CastType.Broadcast, new Dictionary<string, object?>());

        await bus.PublishAsync(channelKey, message);

        await peerSocket.Received(1).SendAsync(
            Arg.Is<ArraySegment<byte>>(segment => MatchesFanOut(segment, channelKey, bus.SelfId)),
            WebSocketMessageType.Text,
            true,
            Arg.Any<CancellationToken>());
    }

    private static bool MatchesFanOut(ArraySegment<byte> segment, Guid expectedChannelKey, Guid expectedSelfId)
    {
        var frame = System.Text.Encoding.UTF8.GetString(segment).FromNJson<WebSocketClusterFrame>();
        if (frame is null || frame.Kind != WebSocketClusterFrameKind.FanOut)
            return false;

        var payload = frame.PayloadJson.FromNJson<WebSocketFanOutPayload>();
        return payload is not null && payload.ChannelKey == expectedChannelKey && payload.Message.OriginId == expectedSelfId;
    }

    [Fact]
    public async Task PublishAsync_OnePeerUnreachable_DoesNotThrow()
    {
        var discovery = WebSocketClusterMessageBusTestHelpers.CreateDiscoverySubstitute(
            [new ClusterNodeEntry(new Uri("https://unreachable-peer:5001/"), IsLeader: false)]);

        await using var bus = await WebSocketClusterMessageBusTestHelpers.CreateBusAsync(
            discovery: discovery,
            outboundSocketFactory: (_, _) => throw new InvalidOperationException("simulated connect failure"));

        var message = new ClusterFanOutMessage(Guid.Empty, 1, CastType.Broadcast, new Dictionary<string, object?>());

        var act = async () => await bus.PublishAsync(Guid.NewGuid(), message);

        await act.Should().NotThrowAsync("one unreachable peer must not fail the whole fan-out publish");
    }

    [Fact]
    public async Task HandleFanOutDeliveryAsync_SelfEcho_IsRejected()
    {
        await using var bus = await WebSocketClusterMessageBusTestHelpers.CreateBusAsync();

        var invoked = false;
        Func<ClusterFanOutMessage, CancellationToken, Task> handler = (_, _) => { invoked = true; return Task.CompletedTask; };

        var channelKey = Guid.NewGuid();
        await bus.SubscribeAsync(channelKey, handler);

        var selfMessage = new ClusterFanOutMessage(bus.SelfId, 1, CastType.Broadcast, new Dictionary<string, object?>());
        var payload = new WebSocketFanOutPayload(channelKey, selfMessage);

        await bus.HandleFanOutDeliveryAsync(payload.ToNJson(), CancellationToken.None);

        invoked.Should().BeFalse("a node must never re-process its own fan-out broadcast");
    }

    [Fact]
    public async Task HandleFanOutDeliveryAsync_ForeignMessageWithARegisteredHandler_IsDelivered()
    {
        await using var bus = await WebSocketClusterMessageBusTestHelpers.CreateBusAsync();

        ClusterFanOutMessage? received = null;
        Func<ClusterFanOutMessage, CancellationToken, Task> handler = (m, _) => { received = m; return Task.CompletedTask; };

        var channelKey = Guid.NewGuid();
        await bus.SubscribeAsync(channelKey, handler);

        var foreignMessage = new ClusterFanOutMessage(Guid.NewGuid(), 1, CastType.Broadcast, new Dictionary<string, object?>());
        var payload = new WebSocketFanOutPayload(channelKey, foreignMessage);

        await bus.HandleFanOutDeliveryAsync(payload.ToNJson(), CancellationToken.None);

        received.Should().NotBeNull();
        received!.OriginId.Should().Be(foreignMessage.OriginId);
    }

    [Fact]
    public async Task HandleFanOutDeliveryAsync_NoRegisteredHandlerForTheChannel_DoesNotThrow()
    {
        await using var bus = await WebSocketClusterMessageBusTestHelpers.CreateBusAsync();

        var foreignMessage = new ClusterFanOutMessage(Guid.NewGuid(), 1, CastType.Broadcast, new Dictionary<string, object?>());
        var payload = new WebSocketFanOutPayload(Guid.NewGuid(), foreignMessage);

        var act = async () => await bus.HandleFanOutDeliveryAsync(payload.ToNJson(), CancellationToken.None);

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task HandleFanOutDeliveryAsync_MalformedPayload_IsSkippedWithoutThrowing()
    {
        await using var bus = await WebSocketClusterMessageBusTestHelpers.CreateBusAsync();

        var act = async () => await bus.HandleFanOutDeliveryAsync("{ not valid json ", CancellationToken.None);

        await act.Should().NotThrowAsync("a single malformed delivery must not crash the connection");
    }

    [Fact]
    public async Task SubscribeAsync_DisposedHandle_StopsFurtherDelivery()
    {
        await using var bus = await WebSocketClusterMessageBusTestHelpers.CreateBusAsync();

        var invoked = false;
        Func<ClusterFanOutMessage, CancellationToken, Task> handler = (_, _) => { invoked = true; return Task.CompletedTask; };

        var channelKey = Guid.NewGuid();
        var subscription = await bus.SubscribeAsync(channelKey, handler);
        await subscription.DisposeAsync();

        var message = new ClusterFanOutMessage(Guid.NewGuid(), 1, CastType.Broadcast, new Dictionary<string, object?>());
        var payload = new WebSocketFanOutPayload(channelKey, message);

        await bus.HandleFanOutDeliveryAsync(payload.ToNJson(), CancellationToken.None);

        invoked.Should().BeFalse("a disposed subscription must not receive further deliveries");
    }
}
