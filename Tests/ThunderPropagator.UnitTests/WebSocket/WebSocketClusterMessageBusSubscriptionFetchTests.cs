using System.Text;
using FluentAssertions;
using NSubstitute;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;
using ThunderPropagator.ClusterMessageBuses.WebSocket;
using ThunderPropagator.Infrastructure.Channels;

namespace ThunderPropagator.UnitTests.WebSocket;

public class WebSocketClusterMessageBusSubscriptionFetchTests
{
    [Fact]
    public async Task BuildResponseAsync_FetchSubscriptions_ReturnsTheResolvedChannelsLocalDescriptors()
    {
        var channelKey = Guid.NewGuid();
        var channel = Substitute.For<IChannel>();
        var descriptors = new[] { new ClusterSubscriptionDescriptor("sub-1", "req-1", "conn-1") };
        channel.GetLocalClusterSubscriptionDescriptors().Returns(descriptors);

        var resolver = Substitute.For<IClusterChannelResolver>();
        resolver.GetChannel(channelKey).Returns(channel);

        await using var bus = await WebSocketClusterMessageBusTestHelpers.CreateBusAsync(channelResolver: resolver);
        var request = new WebSocketClusterRequestEnvelope(Guid.NewGuid(), WebSocketClusterRequestKind.FetchSubscriptions, null, channelKey, null);

        var response = await bus.BuildResponseAsync(request, CancellationToken.None);

        response.Success.Should().BeTrue();
        var returned = response.PayloadJson!.FromNJson<ClusterSubscriptionDescriptor[]>();
        returned.Should().ContainSingle(d => d.SubscriptionId == "sub-1" && d.ConnectionId == "conn-1");
    }

    [Fact]
    public async Task BuildResponseAsync_UnknownChannelKey_ReturnsAFailureResponseInsteadOfThrowing()
    {
        var channelKey = Guid.NewGuid();
        var resolver = Substitute.For<IClusterChannelResolver>();
        resolver.GetChannel(channelKey).Returns(_ => throw new InvalidChannelKeyException(channelKey, new KeyNotFoundException()));

        await using var bus = await WebSocketClusterMessageBusTestHelpers.CreateBusAsync(channelResolver: resolver);
        var request = new WebSocketClusterRequestEnvelope(Guid.NewGuid(), WebSocketClusterRequestKind.FetchSubscriptions, null, channelKey, null);

        var response = await bus.BuildResponseAsync(request, CancellationToken.None);

        response.Success.Should().BeFalse();
        response.PayloadJson.Should().BeNull();
    }

    [Fact]
    public async Task FetchPeerSubscriptionsAsync_PeerNeverReplies_ReturnsEmptyInsteadOfThrowing()
    {
        await using var bus = await WebSocketClusterMessageBusTestHelpers.CreateBusAsync(requestTimeout: TimeSpan.FromMilliseconds(50));

        var result = await bus.FetchPeerSubscriptionsAsync(new Uri("https://unreachable-peer:5001/"), Guid.NewGuid());

        result.Should().BeEmpty("FetchPeerSubscriptionsAsync is called against every discovered peer, so an unreachable one must degrade gracefully rather than fail the whole bootstrap");
    }

    [Fact]
    public async Task FetchPeerSubscriptionsAsync_MatchingReplyArrives_ReturnsTheDescriptors()
    {
        var peerSocket = WebSocketClusterMessageBusTestHelpers.CreateSocketSubstitute();
        await using var bus = await WebSocketClusterMessageBusTestHelpers.CreateBusAsync(
            requestTimeout: TimeSpan.FromSeconds(5),
            outboundSocketFactory: WebSocketClusterMessageBusTestHelpers.OutboundSocketFactoryReturning(peerSocket));

        var fetchTask = bus.FetchPeerSubscriptionsAsync(new Uri("https://peer:5001/"), Guid.NewGuid());

        var call = peerSocket.ReceivedCalls()
            .Last(c => c.GetMethodInfo().Name == nameof(System.Net.WebSockets.WebSocket.SendAsync));
        var segment = (ArraySegment<byte>)call.GetArguments()[0]!;
        var frame = Encoding.UTF8.GetString(segment).FromNJson<WebSocketClusterFrame>()!;
        var sentRequest = frame.PayloadJson.FromNJson<WebSocketClusterRequestEnvelope>()!;

        var descriptors = new[] { new ClusterSubscriptionDescriptor("sub-9", "req-9", "conn-9") };
        bus.TryCompletePendingRequest(new WebSocketClusterResponseEnvelope(sentRequest.CorrelationId, true, null, descriptors.ToNJson()));

        var result = await fetchTask;

        result.Should().ContainSingle(d => d.SubscriptionId == "sub-9");
    }
}
