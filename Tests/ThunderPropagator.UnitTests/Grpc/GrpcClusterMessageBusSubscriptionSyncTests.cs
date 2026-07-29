using FluentAssertions;
using ThunderPropagator.Application.Channels.Cluster.Discovery;
using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.Grpc;
using ThunderPropagator.ClusterMessageBuses.Grpc.Protos;

namespace ThunderPropagator.UnitTests.Grpc;

public class GrpcClusterMessageBusSubscriptionSyncTests
{
    private static ClusterSubscriptionEvent CreateEvent(Guid originId) =>
        new(originId, "https://origin-node:5000/", ClusterSubscriptionEventKind.Added, [], DateTimeOffset.UtcNow);

    [Fact]
    public async Task PublishAsync_StampsOriginId_AndSendsToEveryDiscoveredPeer()
    {
        var (connection, _, _, syncWriter, _) = GrpcClusterMessageBusTestHelpers.CreatePeerConnection();
        var discovery = GrpcClusterMessageBusTestHelpers.CreateDiscoverySubstitute(
            [new ClusterNodeEntry(new Uri("https://peer1:5001/"), IsLeader: false)]);

        await using var bus = await GrpcClusterMessageBusTestHelpers.CreateBusAsync(
            discovery: discovery,
            peerConnectionFactory: (_, _) => Task.FromResult(connection));

        var channelKey = Guid.NewGuid();
        await bus.PublishAsync(channelKey, CreateEvent(Guid.Empty));

        syncWriter.Written.Should().Contain(e => e.ChannelKey == channelKey.ToString());
        var sent = syncWriter.Written.First(e => e.ChannelKey == channelKey.ToString());
        sent.PayloadJson.FromNJson<ClusterSubscriptionEvent>()!.OriginId.Should().Be(bus.SelfId);
    }

    [Fact]
    public async Task Reconnect_ResendsTheMostRecentlyPublishedEventPerChannel()
    {
        var (connection, _, _, syncWriter, _) = GrpcClusterMessageBusTestHelpers.CreatePeerConnection();
        var discovery = GrpcClusterMessageBusTestHelpers.CreateDiscoverySubstitute(
            [new ClusterNodeEntry(new Uri("https://peer1:5001/"), IsLeader: false)]);

        await using var bus = await GrpcClusterMessageBusTestHelpers.CreateBusAsync(
            discovery: discovery,
            peerConnectionFactory: (_, _) => Task.FromResult(connection));

        var channelKey = Guid.NewGuid();
        await bus.PublishAsync(channelKey, CreateEvent(Guid.Empty));

        // The original publish and the peer's maintenance-loop-triggered resend-on-connect replay
        // both land on this same connection.
        await Task.Delay(150);

        syncWriter.Written.Should().HaveCountGreaterThanOrEqualTo(2,
            "PublishAsync sends the event once, and the maintenance loop resends this node's current subscription state as soon as it connects");
        syncWriter.Written.Should().OnlyContain(e => e.ChannelKey == channelKey.ToString());
    }

    [Fact]
    public async Task PeerConnectionMaintenanceLoop_FirstConnectAttemptFails_RetriesWithBackOffUntilSuccessful()
    {
        var attempts = 0;
        var (goodConnection, _, _, syncWriter, _) = GrpcClusterMessageBusTestHelpers.CreatePeerConnection();

        Func<Uri, CancellationToken, Task<GrpcPeerConnection>> factory = (_, _) =>
        {
            attempts++;
            if (attempts == 1)
                throw new InvalidOperationException("simulated first connect failure");

            return Task.FromResult(goodConnection);
        };

        var discovery = GrpcClusterMessageBusTestHelpers.CreateDiscoverySubstitute(
            [new ClusterNodeEntry(new Uri("https://peer1:5001/"), IsLeader: false)]);

        await using var bus = await GrpcClusterMessageBusTestHelpers.CreateBusAsync(
            discovery: discovery,
            peerConnectionFactory: factory,
            initialReconnectDelay: TimeSpan.FromMilliseconds(10),
            maxReconnectDelay: TimeSpan.FromMilliseconds(50));

        // The first PublishAsync's own GetOrCreateOutboundConnectionAsync call and the maintenance
        // loop's own first connect attempt race for the same cache entry — whichever wins triggers
        // the failing first attempt; the maintenance loop then backs off and retries until success.
        await bus.PublishAsync(Guid.NewGuid(), CreateEvent(Guid.Empty));

        await Task.Delay(300);

        attempts.Should().BeGreaterThanOrEqualTo(2, "the maintenance loop must reconnect with back-off after the first attempt fails");
        syncWriter.Written.Should().NotBeEmpty("once reconnected, the maintenance loop resends this node's current subscription state");
    }

    [Fact]
    public async Task HandleSubscriptionEventDeliveryAsync_SelfEcho_IsRejected()
    {
        await using var bus = await GrpcClusterMessageBusTestHelpers.CreateBusAsync();

        var invoked = false;
        Func<ClusterSubscriptionEvent, CancellationToken, Task> handler = (_, _) => { invoked = true; return Task.CompletedTask; };

        var channelKey = Guid.NewGuid();
        await bus.SubscribeAsync(channelKey, handler);

        var selfEvent = CreateEvent(bus.SelfId);
        var wireEvent = new SubscriptionEvent { ChannelKey = channelKey.ToString(), PayloadJson = selfEvent.ToNJson() };

        await bus.HandleSubscriptionEventDeliveryAsync(wireEvent, CancellationToken.None);

        invoked.Should().BeFalse("a node must never re-process its own subscription event");
    }

    [Fact]
    public async Task HandleSubscriptionEventDeliveryAsync_ForeignEvent_IsDelivered()
    {
        await using var bus = await GrpcClusterMessageBusTestHelpers.CreateBusAsync();

        ClusterSubscriptionEvent? received = null;
        Func<ClusterSubscriptionEvent, CancellationToken, Task> handler = (e, _) => { received = e; return Task.CompletedTask; };

        var channelKey = Guid.NewGuid();
        await bus.SubscribeAsync(channelKey, handler);

        var foreignEvent = CreateEvent(Guid.NewGuid());
        var wireEvent = new SubscriptionEvent { ChannelKey = channelKey.ToString(), PayloadJson = foreignEvent.ToNJson() };

        await bus.HandleSubscriptionEventDeliveryAsync(wireEvent, CancellationToken.None);

        received.Should().NotBeNull();
        received!.OriginId.Should().Be(foreignEvent.OriginId);
    }

    [Fact]
    public async Task HandleSubscriptionEventDeliveryAsync_MalformedPayload_IsSkippedWithoutThrowing()
    {
        await using var bus = await GrpcClusterMessageBusTestHelpers.CreateBusAsync();

        var wireEvent = new SubscriptionEvent { ChannelKey = Guid.NewGuid().ToString(), PayloadJson = "{ not valid json " };

        var act = async () => await bus.HandleSubscriptionEventDeliveryAsync(wireEvent, CancellationToken.None);

        await act.Should().NotThrowAsync("a single malformed delivery must not crash the connection");
    }

    [Fact]
    public async Task SubscribeAsync_DisposedHandle_StopsFurtherDelivery()
    {
        await using var bus = await GrpcClusterMessageBusTestHelpers.CreateBusAsync();

        var invoked = false;
        Func<ClusterSubscriptionEvent, CancellationToken, Task> handler = (_, _) => { invoked = true; return Task.CompletedTask; };

        var channelKey = Guid.NewGuid();
        var subscription = await bus.SubscribeAsync(channelKey, handler);
        await subscription.DisposeAsync();

        var wireEvent = new SubscriptionEvent { ChannelKey = channelKey.ToString(), PayloadJson = CreateEvent(Guid.NewGuid()).ToNJson() };

        await bus.HandleSubscriptionEventDeliveryAsync(wireEvent, CancellationToken.None);

        invoked.Should().BeFalse("a disposed subscription must not receive further deliveries");
    }
}
