using System.Net;
using System.Text;
using FluentAssertions;
using NSubstitute;
using ThunderPropagator.Application.Channels.Cluster.Discovery;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.BuildingBlocks.Application.Enums;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.UdpClient;

namespace ThunderPropagator.UnitTests.UdpClient;

public class UdpClusterMessageBusFanOutTests
{
    [Fact]
    public async Task PublishAsync_StampsOriginId_AndSendsToEveryDiscoveredPeer()
    {
        var socket = UdpClusterMessageBusTestHelpers.CreateSocketSubstitute();
        var discovery = UdpClusterMessageBusTestHelpers.CreateDiscoverySubstitute(
            [new ClusterNodeEntry(new Uri("https://peer1:5001/"), IsLeader: false)]);

        await using var bus = await UdpClusterMessageBusTestHelpers.CreateBusAsync(socket: socket, discovery: discovery);

        var channelKey = Guid.NewGuid();
        var message = new ClusterFanOutMessage(Guid.Empty, 1, CastType.Broadcast, new Dictionary<string, object?>());

        await bus.PublishAsync(channelKey, message);

        await socket.Received(1).SendDatagramAsync(
            Arg.Is<byte[]>(bytes => DatagramMatches(bytes, channelKey, bus.SelfId)),
            Arg.Any<IPEndPoint>(),
            Arg.Any<CancellationToken>());
    }

    private static bool DatagramMatches(byte[] bytes, Guid expectedChannelKey, Guid expectedSelfId)
    {
        var frame = Encoding.UTF8.GetString(bytes).FromNJson<UdpClusterFrame>();
        if (frame is null || frame.Kind != UdpClusterFrameKind.FanOut)
            return false;

        var payload = frame.PayloadJson.FromNJson<UdpFanOutPayload>();
        return payload is not null && payload.ChannelKey == expectedChannelKey && payload.Message.OriginId == expectedSelfId;
    }

    [Fact]
    public async Task PublishAsync_OnePeerUnresolvable_DoesNotThrow()
    {
        var discovery = UdpClusterMessageBusTestHelpers.CreateDiscoverySubstitute(
            [new ClusterNodeEntry(new Uri("https://unresolvable-peer:5001/"), IsLeader: false)]);

        await using var bus = await UdpClusterMessageBusTestHelpers.CreateBusAsync(
            discovery: discovery,
            peerEndpointResolver: (_, _) => throw new InvalidOperationException("simulated DNS failure"));

        var message = new ClusterFanOutMessage(Guid.Empty, 1, CastType.Broadcast, new Dictionary<string, object?>());

        var act = async () => await bus.PublishAsync(Guid.NewGuid(), message);

        await act.Should().NotThrowAsync("one unresolvable peer must not fail the whole fan-out publish");
    }

    [Fact]
    public async Task HandleFanOutDeliveryAsync_SelfEcho_IsRejected()
    {
        await using var bus = await UdpClusterMessageBusTestHelpers.CreateBusAsync();

        var invoked = false;
        Func<ClusterFanOutMessage, CancellationToken, Task> handler = (_, _) => { invoked = true; return Task.CompletedTask; };

        var channelKey = Guid.NewGuid();
        await bus.SubscribeAsync(channelKey, handler);

        var selfMessage = new ClusterFanOutMessage(bus.SelfId, 1, CastType.Broadcast, new Dictionary<string, object?>());
        var payload = new UdpFanOutPayload(channelKey, selfMessage);

        await bus.HandleFanOutDeliveryAsync(payload.ToNJson(), CancellationToken.None);

        invoked.Should().BeFalse("a node must never re-process its own fan-out broadcast");
    }

    [Fact]
    public async Task HandleFanOutDeliveryAsync_ForeignMessageWithARegisteredHandler_IsDelivered()
    {
        await using var bus = await UdpClusterMessageBusTestHelpers.CreateBusAsync();

        ClusterFanOutMessage? received = null;
        Func<ClusterFanOutMessage, CancellationToken, Task> handler = (m, _) => { received = m; return Task.CompletedTask; };

        var channelKey = Guid.NewGuid();
        await bus.SubscribeAsync(channelKey, handler);

        var foreignMessage = new ClusterFanOutMessage(Guid.NewGuid(), 1, CastType.Broadcast, new Dictionary<string, object?>());
        var payload = new UdpFanOutPayload(channelKey, foreignMessage);

        await bus.HandleFanOutDeliveryAsync(payload.ToNJson(), CancellationToken.None);

        received.Should().NotBeNull();
        received!.OriginId.Should().Be(foreignMessage.OriginId);
    }

    [Fact]
    public async Task HandleFanOutDeliveryAsync_NoRegisteredHandlerForTheChannel_DoesNotThrow()
    {
        await using var bus = await UdpClusterMessageBusTestHelpers.CreateBusAsync();

        var foreignMessage = new ClusterFanOutMessage(Guid.NewGuid(), 1, CastType.Broadcast, new Dictionary<string, object?>());
        var payload = new UdpFanOutPayload(Guid.NewGuid(), foreignMessage);

        var act = async () => await bus.HandleFanOutDeliveryAsync(payload.ToNJson(), CancellationToken.None);

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task HandleFanOutDeliveryAsync_MalformedPayload_IsSkippedWithoutThrowing()
    {
        await using var bus = await UdpClusterMessageBusTestHelpers.CreateBusAsync();

        var act = async () => await bus.HandleFanOutDeliveryAsync("{ not valid json ", CancellationToken.None);

        await act.Should().NotThrowAsync("a single malformed delivery must not crash the receive loop");
    }

    [Fact]
    public async Task SubscribeAsync_DisposedHandle_StopsFurtherDelivery()
    {
        await using var bus = await UdpClusterMessageBusTestHelpers.CreateBusAsync();

        var invoked = false;
        Func<ClusterFanOutMessage, CancellationToken, Task> handler = (_, _) => { invoked = true; return Task.CompletedTask; };

        var channelKey = Guid.NewGuid();
        var subscription = await bus.SubscribeAsync(channelKey, handler);
        await subscription.DisposeAsync();

        var message = new ClusterFanOutMessage(Guid.NewGuid(), 1, CastType.Broadcast, new Dictionary<string, object?>());
        var payload = new UdpFanOutPayload(channelKey, message);

        await bus.HandleFanOutDeliveryAsync(payload.ToNJson(), CancellationToken.None);

        invoked.Should().BeFalse("a disposed subscription must not receive further deliveries");
    }

    [Fact]
    public async Task PublishAsync_DatagramExceedsMaxDatagramSize_SkipsThatPeerWithoutThrowing()
    {
        var discovery = UdpClusterMessageBusTestHelpers.CreateDiscoverySubstitute(
            [new ClusterNodeEntry(new Uri("https://peer1:5001/"), IsLeader: false)]);

        var options = Microsoft.Extensions.Options.Options.Create(new UdpClusterMessageBusOptions { MaxDatagramSize = 10 });
        var clusterConfiguration = new ThunderPropagator.Application.Channels.Cluster.ClusterConfiguration
        {
            NodeEndpoint = UdpClusterMessageBusTestHelpers.DefaultNodeEndpoint,
        };
        var channelResolver = Substitute.For<ThunderPropagator.ClusterMessageBuses.SharedKernel.IClusterChannelResolver>();
        var socket = UdpClusterMessageBusTestHelpers.CreateSocketSubstitute();

        await using var bus = new UdpClusterMessageBus(
            options, clusterConfiguration, channelResolver, discovery,
            Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory.Instance,
            UdpClusterMessageBusTestHelpers.SocketFactoryReturning(socket),
            UdpClusterMessageBusTestHelpers.ResolverReturning(UdpClusterMessageBusTestHelpers.DefaultResolvedEndpoint));

        var message = new ClusterFanOutMessage(Guid.Empty, 1, CastType.Broadcast, new Dictionary<string, object?> { ["padding"] = new string('x', 500) });

        var act = async () => await bus.PublishAsync(Guid.NewGuid(), message);

        // The oversized-datagram guard throws from inside SendFrameAsync, but PublishAsync's own
        // per-peer try/catch swallows it (same as an unreachable peer) — so the publish call itself
        // must not throw, even though nothing was actually sent.
        await act.Should().NotThrowAsync();
        await socket.DidNotReceive().SendDatagramAsync(Arg.Any<byte[]>(), Arg.Any<IPEndPoint>(), Arg.Any<CancellationToken>());
    }
}
