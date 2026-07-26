using System.Net.Http;
using FluentAssertions;
using NSubstitute;
using ThunderPropagator.Application.Channels.Cluster.Discovery;
using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.WebApi;

namespace ThunderPropagator.UnitTests.WebApi;

public class WebApiClusterMessageBusSubscriptionSyncTests
{
    private static ClusterSubscriptionEvent CreateEvent(Guid originId) => new(
        originId,
        "https://node1:5000/",
        ClusterSubscriptionEventKind.Added,
        [new ClusterSubscriptionDescriptor("sub-1", "req-1", "conn-1")],
        DateTimeOffset.UtcNow);

    [Fact]
    public async Task PublishAsync_StampsOriginId_AndPostsToEveryDiscoveredPeer()
    {
        var httpClient = WebApiClusterMessageBusTestHelpers.CreateHttpClientSubstitute();
        var discovery = WebApiClusterMessageBusTestHelpers.CreateDiscoverySubstitute(
            [new ClusterNodeEntry(new Uri("https://peer1:5001/"), IsLeader: false)]);

        await using var bus = await WebApiClusterMessageBusTestHelpers.CreateBusAsync(discovery: discovery, httpClient: httpClient);

        var channelKey = Guid.NewGuid();
        await bus.PublishAsync(channelKey, CreateEvent(Guid.Empty));

        var expectedUrl = WebApiClusterRouting.PeerSubscriptionEventUrl(new Uri("https://peer1:5001/"), "/thunderpropagator/cluster/webapi", channelKey);

        await httpClient.Received(1).SendAsync(
            Arg.Is<HttpRequestMessage>(r => r.Method == HttpMethod.Post && r.RequestUri!.ToString() == expectedUrl),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task HandleSubscriptionEventDeliveryAsync_SelfEcho_IsRejected()
    {
        await using var bus = await WebApiClusterMessageBusTestHelpers.CreateBusAsync();

        var invoked = false;
        Func<ClusterSubscriptionEvent, CancellationToken, Task> handler = (_, _) => { invoked = true; return Task.CompletedTask; };

        var channelKey = Guid.NewGuid();
        await bus.SubscribeAsync(channelKey, handler);

        await bus.HandleSubscriptionEventDeliveryAsync(CreateEvent(bus.SelfId).ToNJson(), channelKey, CancellationToken.None);

        invoked.Should().BeFalse("a node must never re-process its own subscription-event broadcast");
    }

    [Fact]
    public async Task HandleSubscriptionEventDeliveryAsync_ForeignEventWithARegisteredHandler_IsDelivered()
    {
        await using var bus = await WebApiClusterMessageBusTestHelpers.CreateBusAsync();

        ClusterSubscriptionEvent? received = null;
        Func<ClusterSubscriptionEvent, CancellationToken, Task> handler = (e, _) => { received = e; return Task.CompletedTask; };

        var channelKey = Guid.NewGuid();
        await bus.SubscribeAsync(channelKey, handler);

        var foreignEvent = CreateEvent(Guid.NewGuid());
        await bus.HandleSubscriptionEventDeliveryAsync(foreignEvent.ToNJson(), channelKey, CancellationToken.None);

        received.Should().NotBeNull();
        received!.OriginId.Should().Be(foreignEvent.OriginId);
    }

    [Fact]
    public async Task HandleSubscriptionEventDeliveryAsync_MalformedPayload_IsSkippedWithoutThrowing()
    {
        await using var bus = await WebApiClusterMessageBusTestHelpers.CreateBusAsync();

        var act = async () => await bus.HandleSubscriptionEventDeliveryAsync("not json at all", Guid.NewGuid(), CancellationToken.None);

        await act.Should().NotThrowAsync("a single malformed delivery must not crash the request handler");
    }

    [Fact]
    public async Task SubscribeAsync_DisposedHandle_StopsFurtherDelivery()
    {
        await using var bus = await WebApiClusterMessageBusTestHelpers.CreateBusAsync();

        var invoked = false;
        Func<ClusterSubscriptionEvent, CancellationToken, Task> handler = (_, _) => { invoked = true; return Task.CompletedTask; };

        var channelKey = Guid.NewGuid();
        var subscription = await bus.SubscribeAsync(channelKey, handler);
        await subscription.DisposeAsync();

        await bus.HandleSubscriptionEventDeliveryAsync(CreateEvent(Guid.NewGuid()).ToNJson(), channelKey, CancellationToken.None);

        invoked.Should().BeFalse("a disposed subscription must not receive further deliveries");
    }
}
