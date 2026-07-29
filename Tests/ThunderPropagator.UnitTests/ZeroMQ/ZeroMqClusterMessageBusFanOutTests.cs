using FluentAssertions;
using NSubstitute;
using ThunderPropagator.Application.Channels.Cluster.Discovery;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.BuildingBlocks.Application.Enums;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.ZeroMQ;

namespace ThunderPropagator.UnitTests.ZeroMQ;

public class ZeroMqClusterMessageBusFanOutTests
{
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
        var message = new ClusterFanOutMessage(Guid.Empty, 1, CastType.Broadcast, new Dictionary<string, object?>());

        await bus.PublishAsync(channelKey, message);

        connection.Received(1).SendFrame(Arg.Is<ZeroMqClusterFrame>(f =>
            f.Kind == ZeroMqClusterFrameKind.FanOut &&
            f.PayloadJson.FromNJson<ZeroMqFanOutPayload>()!.ChannelKey == channelKey &&
            f.PayloadJson.FromNJson<ZeroMqFanOutPayload>()!.Message.OriginId == bus.SelfId));
    }

    [Fact]
    public async Task PublishAsync_OnePeerUnreachable_DoesNotThrow_AndTheOtherPeerStillReceivesIt()
    {
        var goodConnection = ZeroMqClusterMessageBusTestHelpers.CreatePeerConnectionSubstitute();
        var discovery = ZeroMqClusterMessageBusTestHelpers.CreateDiscoverySubstitute(
        [
            new ClusterNodeEntry(new Uri("https://unreachable-peer:5001/"), IsLeader: false),
            new ClusterNodeEntry(new Uri("https://good-peer:5001/"), IsLeader: false),
        ]);

        await using var bus = await ZeroMqClusterMessageBusTestHelpers.CreateBusAsync(
            discovery: discovery,
            peerConnectionFactory: (peer, _) => peer.Host == "unreachable-peer"
                ? throw new InvalidOperationException("simulated connect failure")
                : Task.FromResult(goodConnection));

        var message = new ClusterFanOutMessage(Guid.Empty, 1, CastType.Broadcast, new Dictionary<string, object?>());

        var act = async () => await bus.PublishAsync(Guid.NewGuid(), message);

        await act.Should().NotThrowAsync("one unreachable peer must not fail the whole fan-out publish");
        goodConnection.Received(1).SendFrame(Arg.Any<ZeroMqClusterFrame>());
    }

    [Fact]
    public async Task HandleFanOutDeliveryAsync_SelfEcho_IsRejected()
    {
        await using var bus = await ZeroMqClusterMessageBusTestHelpers.CreateBusAsync();

        var invoked = false;
        Func<ClusterFanOutMessage, CancellationToken, Task> handler = (_, _) => { invoked = true; return Task.CompletedTask; };

        var channelKey = Guid.NewGuid();
        await bus.SubscribeAsync(channelKey, handler);

        var selfMessage = new ClusterFanOutMessage(bus.SelfId, 1, CastType.Broadcast, new Dictionary<string, object?>());
        var payload = new ZeroMqFanOutPayload(channelKey, selfMessage);

        await bus.HandleFanOutDeliveryAsync(payload.ToNJson(), CancellationToken.None);

        invoked.Should().BeFalse("a node must never re-process its own fan-out broadcast");
    }

    [Fact]
    public async Task HandleFanOutDeliveryAsync_ForeignMessageWithARegisteredHandler_IsDelivered()
    {
        await using var bus = await ZeroMqClusterMessageBusTestHelpers.CreateBusAsync();

        ClusterFanOutMessage? received = null;
        Func<ClusterFanOutMessage, CancellationToken, Task> handler = (m, _) => { received = m; return Task.CompletedTask; };

        var channelKey = Guid.NewGuid();
        await bus.SubscribeAsync(channelKey, handler);

        var foreignMessage = new ClusterFanOutMessage(Guid.NewGuid(), 1, CastType.Broadcast, new Dictionary<string, object?>());
        var payload = new ZeroMqFanOutPayload(channelKey, foreignMessage);

        await bus.HandleFanOutDeliveryAsync(payload.ToNJson(), CancellationToken.None);

        received.Should().NotBeNull();
        received!.OriginId.Should().Be(foreignMessage.OriginId);
    }

    [Fact]
    public async Task HandleFanOutDeliveryAsync_NoRegisteredHandlerForTheChannel_DoesNotThrow()
    {
        await using var bus = await ZeroMqClusterMessageBusTestHelpers.CreateBusAsync();

        var foreignMessage = new ClusterFanOutMessage(Guid.NewGuid(), 1, CastType.Broadcast, new Dictionary<string, object?>());
        var payload = new ZeroMqFanOutPayload(Guid.NewGuid(), foreignMessage);

        var act = async () => await bus.HandleFanOutDeliveryAsync(payload.ToNJson(), CancellationToken.None);

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task HandleFanOutDeliveryAsync_MalformedPayload_IsSkippedWithoutThrowing()
    {
        await using var bus = await ZeroMqClusterMessageBusTestHelpers.CreateBusAsync();

        var act = async () => await bus.HandleFanOutDeliveryAsync("{ not valid json ", CancellationToken.None);

        await act.Should().NotThrowAsync("a single malformed delivery must not crash the connection");
    }

    [Fact]
    public async Task SubscribeAsync_DisposedHandle_StopsFurtherDelivery()
    {
        await using var bus = await ZeroMqClusterMessageBusTestHelpers.CreateBusAsync();

        var invoked = false;
        Func<ClusterFanOutMessage, CancellationToken, Task> handler = (_, _) => { invoked = true; return Task.CompletedTask; };

        var channelKey = Guid.NewGuid();
        var subscription = await bus.SubscribeAsync(channelKey, handler);
        await subscription.DisposeAsync();

        var message = new ClusterFanOutMessage(Guid.NewGuid(), 1, CastType.Broadcast, new Dictionary<string, object?>());
        var payload = new ZeroMqFanOutPayload(channelKey, message);

        await bus.HandleFanOutDeliveryAsync(payload.ToNJson(), CancellationToken.None);

        invoked.Should().BeFalse("a disposed subscription must not receive further deliveries");
    }
}
