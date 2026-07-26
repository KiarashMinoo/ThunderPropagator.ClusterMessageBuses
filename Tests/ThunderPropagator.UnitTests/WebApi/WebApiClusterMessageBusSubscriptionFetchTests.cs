using System.Net;
using FluentAssertions;
using NSubstitute;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;
using ThunderPropagator.ClusterMessageBuses.WebApi;
using ThunderPropagator.Infrastructure.Channels;

namespace ThunderPropagator.UnitTests.WebApi;

public class WebApiClusterMessageBusSubscriptionFetchTests
{
    [Fact]
    public async Task BuildSubscriptionsSelfResponseBody_ReturnsTheResolvedChannelsLocalDescriptors()
    {
        var channelKey = Guid.NewGuid();
        var channel = Substitute.For<IChannel>();
        var descriptors = new[] { new ClusterSubscriptionDescriptor("sub-1", "req-1", "conn-1") };
        channel.GetLocalClusterSubscriptionDescriptors().Returns(descriptors);

        var resolver = Substitute.For<IClusterChannelResolver>();
        resolver.GetChannel(channelKey).Returns(channel);

        await using var bus = await WebApiClusterMessageBusTestHelpers.CreateBusAsync(channelResolver: resolver);

        var body = bus.BuildSubscriptionsSelfResponseBody(channelKey);

        var returned = body.FromNJson<ClusterSubscriptionDescriptor[]>();
        returned.Should().ContainSingle(d => d.SubscriptionId == "sub-1" && d.ConnectionId == "conn-1");
    }

    [Fact]
    public async Task FetchPeerSubscriptionsAsync_PeerRespondsSuccessfully_ReturnsTheDescriptors()
    {
        var descriptors = new[] { new ClusterSubscriptionDescriptor("sub-9", "req-9", "conn-9") };
        var httpClient = WebApiClusterMessageBusTestHelpers.CreateHttpClientSubstitute(HttpStatusCode.OK, descriptors.ToNJson());

        await using var bus = await WebApiClusterMessageBusTestHelpers.CreateBusAsync(httpClient: httpClient);

        var result = await bus.FetchPeerSubscriptionsAsync(new Uri("https://peer:5001/"), Guid.NewGuid());

        result.Should().ContainSingle(d => d.SubscriptionId == "sub-9");
    }

    [Fact]
    public async Task FetchPeerSubscriptionsAsync_PeerRejectsRequest_ReturnsEmptyInsteadOfThrowing()
    {
        var httpClient = WebApiClusterMessageBusTestHelpers.CreateHttpClientSubstitute(HttpStatusCode.NotFound, "{}");
        await using var bus = await WebApiClusterMessageBusTestHelpers.CreateBusAsync(httpClient: httpClient);

        var result = await bus.FetchPeerSubscriptionsAsync(new Uri("https://peer:5001/"), Guid.NewGuid());

        result.Should().BeEmpty();
    }

    [Fact]
    public async Task FetchPeerSubscriptionsAsync_PeerUnreachable_ReturnsEmptyInsteadOfThrowing()
    {
        var httpClient = Substitute.For<IWebApiClusterHttpClient>();
        httpClient.SendAsync(Arg.Any<System.Net.Http.HttpRequestMessage>(), Arg.Any<CancellationToken>())
            .Returns(_ => throw new System.Net.Http.HttpRequestException("simulated connect failure"));

        await using var bus = await WebApiClusterMessageBusTestHelpers.CreateBusAsync(httpClient: httpClient);

        var result = await bus.FetchPeerSubscriptionsAsync(new Uri("https://unreachable-peer:5001/"), Guid.NewGuid());

        result.Should().BeEmpty("FetchPeerSubscriptionsAsync is called against every discovered peer, so an unreachable one must degrade gracefully rather than fail the whole bootstrap");
    }
}
