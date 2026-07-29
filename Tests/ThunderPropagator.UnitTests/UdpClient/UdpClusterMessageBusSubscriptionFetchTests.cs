using System.Net;
using System.Text;
using FluentAssertions;
using NSubstitute;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;
using ThunderPropagator.ClusterMessageBuses.UdpClient;

namespace ThunderPropagator.UnitTests.UdpClient;

public class UdpClusterMessageBusSubscriptionFetchTests
{
    [Fact]
    public async Task FetchPeerSubscriptionsAsync_Success_ReturnsTheDescriptors()
    {
        var descriptors = new[] { new ClusterSubscriptionDescriptor("sub-1", "req-1", "conn-1") };
        var channelKey = Guid.NewGuid();

        UdpClusterMessageBus? busHolder = null;
        var socket = UdpClusterMessageBusTestHelpers.CreateSocketSubstitute();
        socket.SendDatagramAsync(Arg.Any<byte[]>(), Arg.Any<IPEndPoint>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var frame = Encoding.UTF8.GetString((byte[])callInfo[0]).FromNJson<UdpClusterFrame>()!;
                var request = frame.PayloadJson.FromNJson<UdpClusterRequestEnvelope>()!;
                var response = new UdpClusterResponseEnvelope(request.CorrelationId, true, null, descriptors.ToNJson());
                _ = busHolder!.HandleResponseDeliveryAsync(response.ToNJson(), CancellationToken.None);
                return Task.CompletedTask;
            });

        await using var bus = await UdpClusterMessageBusTestHelpers.CreateBusAsync(socket: socket);
        busHolder = bus;

        var result = await bus.FetchPeerSubscriptionsAsync(new Uri("https://peer1:5001/"), channelKey, CancellationToken.None);

        result.Should().ContainSingle(d => d.SubscriptionId == "sub-1");

        await socket.Received(1).SendDatagramAsync(
            Arg.Is<byte[]>(bytes => IsFetchSubscriptionsRequest(bytes, channelKey)),
            Arg.Any<IPEndPoint>(),
            Arg.Any<CancellationToken>());
    }

    // A plain helper (rather than the previous inline statement-bodied lambda) is required
    // because Arg.Is<T> takes an Expression<Predicate<T>>, and expression trees cannot contain
    // a statement body, the null-conditional operator, or an 'is' pattern-matching operator.
    private static bool IsFetchSubscriptionsRequest(byte[] bytes, Guid channelKey)
    {
        var frame = Encoding.UTF8.GetString(bytes).FromNJson<UdpClusterFrame>();
        var request = frame?.PayloadJson.FromNJson<UdpClusterRequestEnvelope>();
        return request is not null && request.Kind == UdpClusterRequestKind.FetchSubscriptions && request.ChannelKey == channelKey;
    }

    [Fact]
    public async Task FetchPeerSubscriptionsAsync_PeerRejectsTheRequest_DegradesToEmptyArray()
    {
        var channelKey = Guid.NewGuid();

        UdpClusterMessageBus? busHolder = null;
        var socket = UdpClusterMessageBusTestHelpers.CreateSocketSubstitute();
        socket.SendDatagramAsync(Arg.Any<byte[]>(), Arg.Any<IPEndPoint>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var frame = Encoding.UTF8.GetString((byte[])callInfo[0]).FromNJson<UdpClusterFrame>()!;
                var request = frame.PayloadJson.FromNJson<UdpClusterRequestEnvelope>()!;
                var response = new UdpClusterResponseEnvelope(request.CorrelationId, false, "no such channel", null);
                _ = busHolder!.HandleResponseDeliveryAsync(response.ToNJson(), CancellationToken.None);
                return Task.CompletedTask;
            });

        await using var bus = await UdpClusterMessageBusTestHelpers.CreateBusAsync(socket: socket);
        busHolder = bus;

        var result = await bus.FetchPeerSubscriptionsAsync(new Uri("https://peer1:5001/"), channelKey, CancellationToken.None);

        result.Should().BeEmpty();
    }

    [Fact]
    public async Task FetchPeerSubscriptionsAsync_Timeout_DegradesToEmptyArray()
    {
        var socket = UdpClusterMessageBusTestHelpers.CreateSocketSubstitute();

        await using var bus = await UdpClusterMessageBusTestHelpers.CreateBusAsync(
            socket: socket,
            resendInterval: TimeSpan.FromMilliseconds(20),
            requestTimeout: TimeSpan.FromMilliseconds(60));

        var result = await bus.FetchPeerSubscriptionsAsync(new Uri("https://peer1:5001/"), Guid.NewGuid(), CancellationToken.None);

        result.Should().BeEmpty("an unreachable/unresponsive peer must degrade gracefully rather than throw");
    }

    [Fact]
    public async Task BuildFetchSubscriptionsResponse_ReturnsTheResolvedChannelsLocalDescriptors()
    {
        var channelKey = Guid.NewGuid();
        var channel = Substitute.For<IChannel>();

        var channelResolver = Substitute.For<IClusterChannelResolver>();
        channelResolver.GetChannel(channelKey).Returns(channel);

        await using var bus = await UdpClusterMessageBusTestHelpers.CreateBusAsync(channelResolver: channelResolver);

        var request = new UdpClusterRequestEnvelope(Guid.NewGuid(), UdpClusterRequestKind.FetchSubscriptions, null, channelKey, null);
        var response = await bus.BuildResponseAsync(request, CancellationToken.None);

        response.Success.Should().BeTrue();
        response.PayloadJson.Should().NotBeNull();
    }
}
