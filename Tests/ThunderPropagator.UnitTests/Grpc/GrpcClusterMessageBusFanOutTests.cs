using FluentAssertions;
using ThunderPropagator.Application.Channels.Cluster.Discovery;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.BuildingBlocks.Application.Enums;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.Grpc.Protos;

namespace ThunderPropagator.UnitTests.Grpc;

public class GrpcClusterMessageBusFanOutTests
{
    [Fact]
    public async Task PublishAsync_StampsOriginId_AndSendsToEveryDiscoveredPeer()
    {
        var (connection, fanOutWriter, _, _, _) = GrpcClusterMessageBusTestHelpers.CreatePeerConnection();
        var discovery = GrpcClusterMessageBusTestHelpers.CreateDiscoverySubstitute(
            [new ClusterNodeEntry(new Uri("https://peer1:5001/"), IsLeader: false)]);

        await using var bus = await GrpcClusterMessageBusTestHelpers.CreateBusAsync(
            discovery: discovery,
            peerConnectionFactory: (_, _) => Task.FromResult(connection));

        var channelKey = Guid.NewGuid();
        var message = new ClusterFanOutMessage(Guid.Empty, 1, CastType.Broadcast, new Dictionary<string, object?>());

        await bus.PublishAsync(channelKey, message);

        fanOutWriter.Written.Should().ContainSingle();
        var sent = fanOutWriter.Written[0];
        sent.ChannelKey.Should().Be(channelKey.ToString());
        sent.PayloadJson.FromNJson<ClusterFanOutMessage>()!.OriginId.Should().Be(bus.SelfId);
    }

    [Fact]
    public async Task PublishAsync_OnePeerUnreachable_DoesNotThrow_AndTheOtherPeerStillReceivesIt()
    {
        var (goodConnection, goodWriter, _, _, _) = GrpcClusterMessageBusTestHelpers.CreatePeerConnection();
        var discovery = GrpcClusterMessageBusTestHelpers.CreateDiscoverySubstitute(
        [
            new ClusterNodeEntry(new Uri("https://unreachable-peer:5001/"), IsLeader: false),
            new ClusterNodeEntry(new Uri("https://good-peer:5001/"), IsLeader: false),
        ]);

        await using var bus = await GrpcClusterMessageBusTestHelpers.CreateBusAsync(
            discovery: discovery,
            peerConnectionFactory: (peer, _) => peer.Host == "unreachable-peer"
                ? throw new InvalidOperationException("simulated connect failure")
                : Task.FromResult(goodConnection));

        var message = new ClusterFanOutMessage(Guid.Empty, 1, CastType.Broadcast, new Dictionary<string, object?>());

        var act = async () => await bus.PublishAsync(Guid.NewGuid(), message);

        await act.Should().NotThrowAsync("one unreachable peer must not fail the whole fan-out publish");
        goodWriter.Written.Should().ContainSingle("the other peer must still receive the message");
    }

    [Fact]
    public async Task HandleFanOutDeliveryAsync_SelfEcho_IsRejected()
    {
        await using var bus = await GrpcClusterMessageBusTestHelpers.CreateBusAsync();

        var invoked = false;
        Func<ClusterFanOutMessage, CancellationToken, Task> handler = (_, _) => { invoked = true; return Task.CompletedTask; };

        var channelKey = Guid.NewGuid();
        await bus.SubscribeAsync(channelKey, handler);

        var selfMessage = new ClusterFanOutMessage(bus.SelfId, 1, CastType.Broadcast, new Dictionary<string, object?>());
        var batch = new PushedMessageBatch { ChannelKey = channelKey.ToString(), PayloadJson = selfMessage.ToNJson() };

        await bus.HandleFanOutDeliveryAsync(batch, CancellationToken.None);

        invoked.Should().BeFalse("a node must never re-process its own fan-out broadcast");
    }

    [Fact]
    public async Task HandleFanOutDeliveryAsync_ForeignMessageWithARegisteredHandler_IsDelivered()
    {
        await using var bus = await GrpcClusterMessageBusTestHelpers.CreateBusAsync();

        ClusterFanOutMessage? received = null;
        Func<ClusterFanOutMessage, CancellationToken, Task> handler = (m, _) => { received = m; return Task.CompletedTask; };

        var channelKey = Guid.NewGuid();
        await bus.SubscribeAsync(channelKey, handler);

        var foreignMessage = new ClusterFanOutMessage(Guid.NewGuid(), 1, CastType.Broadcast, new Dictionary<string, object?>());
        var batch = new PushedMessageBatch { ChannelKey = channelKey.ToString(), PayloadJson = foreignMessage.ToNJson() };

        await bus.HandleFanOutDeliveryAsync(batch, CancellationToken.None);

        received.Should().NotBeNull();
        received!.OriginId.Should().Be(foreignMessage.OriginId);
    }

    [Fact]
    public async Task HandleFanOutDeliveryAsync_NoRegisteredHandlerForTheChannel_DoesNotThrow()
    {
        await using var bus = await GrpcClusterMessageBusTestHelpers.CreateBusAsync();

        var foreignMessage = new ClusterFanOutMessage(Guid.NewGuid(), 1, CastType.Broadcast, new Dictionary<string, object?>());
        var batch = new PushedMessageBatch { ChannelKey = Guid.NewGuid().ToString(), PayloadJson = foreignMessage.ToNJson() };

        var act = async () => await bus.HandleFanOutDeliveryAsync(batch, CancellationToken.None);

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task HandleFanOutDeliveryAsync_MalformedPayload_IsSkippedWithoutThrowing()
    {
        await using var bus = await GrpcClusterMessageBusTestHelpers.CreateBusAsync();

        var batch = new PushedMessageBatch { ChannelKey = Guid.NewGuid().ToString(), PayloadJson = "{ not valid json " };

        var act = async () => await bus.HandleFanOutDeliveryAsync(batch, CancellationToken.None);

        await act.Should().NotThrowAsync("a single malformed delivery must not crash the connection");
    }

    [Fact]
    public async Task HandleFanOutDeliveryAsync_UnparseableChannelKey_IsSkippedWithoutThrowing()
    {
        await using var bus = await GrpcClusterMessageBusTestHelpers.CreateBusAsync();

        var message = new ClusterFanOutMessage(Guid.NewGuid(), 1, CastType.Broadcast, new Dictionary<string, object?>());
        var batch = new PushedMessageBatch { ChannelKey = "not-a-guid", PayloadJson = message.ToNJson() };

        var act = async () => await bus.HandleFanOutDeliveryAsync(batch, CancellationToken.None);

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task SubscribeAsync_DisposedHandle_StopsFurtherDelivery()
    {
        await using var bus = await GrpcClusterMessageBusTestHelpers.CreateBusAsync();

        var invoked = false;
        Func<ClusterFanOutMessage, CancellationToken, Task> handler = (_, _) => { invoked = true; return Task.CompletedTask; };

        var channelKey = Guid.NewGuid();
        var subscription = await bus.SubscribeAsync(channelKey, handler);
        await subscription.DisposeAsync();

        var message = new ClusterFanOutMessage(Guid.NewGuid(), 1, CastType.Broadcast, new Dictionary<string, object?>());
        var batch = new PushedMessageBatch { ChannelKey = channelKey.ToString(), PayloadJson = message.ToNJson() };

        await bus.HandleFanOutDeliveryAsync(batch, CancellationToken.None);

        invoked.Should().BeFalse("a disposed subscription must not receive further deliveries");
    }
}
