using System.Net;
using System.Net.Http;
using FluentAssertions;
using NSubstitute;
using ThunderPropagator.Application.Channels.Cluster.Discovery;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.BuildingBlocks.Application.Enums;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.WebApi;

namespace ThunderPropagator.UnitTests.WebApi;

public class WebApiClusterMessageBusFanOutTests
{
    [Fact]
    public async Task PublishAsync_StampsOriginId_AndPostsToEveryDiscoveredPeer()
    {
        var httpClient = WebApiClusterMessageBusTestHelpers.CreateHttpClientSubstitute();
        var discovery = WebApiClusterMessageBusTestHelpers.CreateDiscoverySubstitute(
            [new ClusterNodeEntry(new Uri("https://peer1:5001/"), IsLeader: false)]);

        await using var bus = await WebApiClusterMessageBusTestHelpers.CreateBusAsync(discovery: discovery, httpClient: httpClient);

        var channelKey = Guid.NewGuid();
        var message = new ClusterFanOutMessage(Guid.Empty, 1, CastType.Broadcast, new Dictionary<string, object?>());

        await bus.PublishAsync(channelKey, message);

        var expectedUrl = WebApiClusterRouting.PeerFanOutUrl(new Uri("https://peer1:5001/"), "/thunderpropagator/cluster/webapi", channelKey);

        await httpClient.Received(1).SendAsync(
            Arg.Is<HttpRequestMessage>(r => r.Method == HttpMethod.Post && r.RequestUri!.ToString() == expectedUrl),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task PublishAsync_SentBody_ContainsTheStampedMessage()
    {
        HttpRequestMessage? capturedRequest = null;
        var httpClient = Substitute.For<IWebApiClusterHttpClient>();
        httpClient.SendAsync(Arg.Any<HttpRequestMessage>(), Arg.Any<CancellationToken>())
            .Returns(async callInfo =>
            {
                capturedRequest = (HttpRequestMessage)callInfo[0];
                return new HttpResponseMessage(HttpStatusCode.NoContent) { Content = new StringContent(await capturedRequest.Content!.ReadAsStringAsync()) };
            });

        var discovery = WebApiClusterMessageBusTestHelpers.CreateDiscoverySubstitute(
            [new ClusterNodeEntry(new Uri("https://peer1:5001/"), IsLeader: false)]);

        await using var bus = await WebApiClusterMessageBusTestHelpers.CreateBusAsync(discovery: discovery, httpClient: httpClient);

        var message = new ClusterFanOutMessage(Guid.Empty, 1, CastType.Broadcast, new Dictionary<string, object?>());
        await bus.PublishAsync(Guid.NewGuid(), message);

        capturedRequest.Should().NotBeNull();
        var body = await capturedRequest!.Content!.ReadAsStringAsync();
        var sent = body.FromNJson<ClusterFanOutMessage>();

        sent!.OriginId.Should().Be(bus.SelfId);
    }

    [Fact]
    public async Task PublishAsync_OnePeerUnreachable_DoesNotThrow()
    {
        var httpClient = Substitute.For<IWebApiClusterHttpClient>();
        httpClient.SendAsync(Arg.Any<HttpRequestMessage>(), Arg.Any<CancellationToken>())
            .Returns(_ => throw new HttpRequestException("simulated connect failure"));

        var discovery = WebApiClusterMessageBusTestHelpers.CreateDiscoverySubstitute(
            [new ClusterNodeEntry(new Uri("https://unreachable-peer:5001/"), IsLeader: false)]);

        await using var bus = await WebApiClusterMessageBusTestHelpers.CreateBusAsync(discovery: discovery, httpClient: httpClient);

        var message = new ClusterFanOutMessage(Guid.Empty, 1, CastType.Broadcast, new Dictionary<string, object?>());

        var act = async () => await bus.PublishAsync(Guid.NewGuid(), message);

        await act.Should().NotThrowAsync("one unreachable peer must not fail the whole fan-out publish");
    }

    [Fact]
    public async Task HandleFanOutDeliveryAsync_SelfEcho_IsRejected()
    {
        await using var bus = await WebApiClusterMessageBusTestHelpers.CreateBusAsync();

        var invoked = false;
        Func<ClusterFanOutMessage, CancellationToken, Task> handler = (_, _) => { invoked = true; return Task.CompletedTask; };

        var channelKey = Guid.NewGuid();
        await bus.SubscribeAsync(channelKey, handler);

        var selfMessage = new ClusterFanOutMessage(bus.SelfId, 1, CastType.Broadcast, new Dictionary<string, object?>());
        await bus.HandleFanOutDeliveryAsync(selfMessage.ToNJson(), channelKey, CancellationToken.None);

        invoked.Should().BeFalse("a node must never re-process its own fan-out broadcast");
    }

    [Fact]
    public async Task HandleFanOutDeliveryAsync_ForeignMessageWithARegisteredHandler_IsDelivered()
    {
        await using var bus = await WebApiClusterMessageBusTestHelpers.CreateBusAsync();

        ClusterFanOutMessage? received = null;
        Func<ClusterFanOutMessage, CancellationToken, Task> handler = (m, _) => { received = m; return Task.CompletedTask; };

        var channelKey = Guid.NewGuid();
        await bus.SubscribeAsync(channelKey, handler);

        var foreignMessage = new ClusterFanOutMessage(Guid.NewGuid(), 1, CastType.Broadcast, new Dictionary<string, object?>());
        await bus.HandleFanOutDeliveryAsync(foreignMessage.ToNJson(), channelKey, CancellationToken.None);

        received.Should().NotBeNull();
        received!.OriginId.Should().Be(foreignMessage.OriginId);
    }

    [Fact]
    public async Task HandleFanOutDeliveryAsync_MalformedPayload_IsSkippedWithoutThrowing()
    {
        await using var bus = await WebApiClusterMessageBusTestHelpers.CreateBusAsync();

        var act = async () => await bus.HandleFanOutDeliveryAsync("{ not valid json ", Guid.NewGuid(), CancellationToken.None);

        await act.Should().NotThrowAsync("a single malformed delivery must not crash the request handler");
    }

    [Fact]
    public async Task SubscribeAsync_DisposedHandle_StopsFurtherDelivery()
    {
        await using var bus = await WebApiClusterMessageBusTestHelpers.CreateBusAsync();

        var invoked = false;
        Func<ClusterFanOutMessage, CancellationToken, Task> handler = (_, _) => { invoked = true; return Task.CompletedTask; };

        var channelKey = Guid.NewGuid();
        var subscription = await bus.SubscribeAsync(channelKey, handler);
        await subscription.DisposeAsync();

        var message = new ClusterFanOutMessage(Guid.NewGuid(), 1, CastType.Broadcast, new Dictionary<string, object?>());
        await bus.HandleFanOutDeliveryAsync(message.ToNJson(), channelKey, CancellationToken.None);

        invoked.Should().BeFalse("a disposed subscription must not receive further deliveries");
    }
}
