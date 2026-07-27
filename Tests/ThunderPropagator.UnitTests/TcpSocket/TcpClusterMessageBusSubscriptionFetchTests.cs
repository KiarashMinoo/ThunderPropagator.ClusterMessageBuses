using FluentAssertions;
using NSubstitute;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;
using ThunderPropagator.ClusterMessageBuses.TcpSocket;

namespace ThunderPropagator.UnitTests.TcpSocket;

public class TcpClusterMessageBusSubscriptionFetchTests
{
    [Fact]
    public async Task FetchPeerSubscriptionsAsync_Success_ReturnsTheDescriptors()
    {
        var descriptors = new[] { new ClusterSubscriptionDescriptor("sub-1", "req-1", "conn-1") };
        var channelKey = Guid.NewGuid();

        TcpClusterMessageBus? busHolder = null;
        var connection = TcpClusterMessageBusTestHelpers.CreateConnectionSubstitute();
        connection.SendFrameAsync(Arg.Any<TcpClusterFrame>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var frame = (TcpClusterFrame)callInfo[0];
                var request = frame.PayloadJson.FromNJson<TcpClusterRequestEnvelope>()!;
                var response = new TcpClusterResponseEnvelope(request.CorrelationId, true, null, descriptors.ToNJson());
                _ = busHolder!.HandleResponseDeliveryAsync(response.ToNJson(), CancellationToken.None);
                return Task.CompletedTask;
            });

        await using var bus = await TcpClusterMessageBusTestHelpers.CreateBusAsync(
            outboundConnectionFactory: (_, _) => Task.FromResult(connection));
        busHolder = bus;

        var result = await bus.FetchPeerSubscriptionsAsync(new Uri("https://peer1:5001/"), channelKey, CancellationToken.None);

        result.Should().ContainSingle(d => d.SubscriptionId == "sub-1");

        await connection.Received(1).SendFrameAsync(
            Arg.Is<TcpClusterFrame>(f => f.Kind == TcpClusterFrameKind.Request &&
                f.PayloadJson.FromNJson<TcpClusterRequestEnvelope>()!.Kind == TcpClusterRequestKind.FetchSubscriptions &&
                f.PayloadJson.FromNJson<TcpClusterRequestEnvelope>()!.ChannelKey == channelKey),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task FetchPeerSubscriptionsAsync_PeerRejectsTheRequest_DegradesToEmptyArray()
    {
        var channelKey = Guid.NewGuid();

        TcpClusterMessageBus? busHolder = null;
        var connection = TcpClusterMessageBusTestHelpers.CreateConnectionSubstitute();
        connection.SendFrameAsync(Arg.Any<TcpClusterFrame>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var frame = (TcpClusterFrame)callInfo[0];
                var request = frame.PayloadJson.FromNJson<TcpClusterRequestEnvelope>()!;
                var response = new TcpClusterResponseEnvelope(request.CorrelationId, false, "no such channel", null);
                _ = busHolder!.HandleResponseDeliveryAsync(response.ToNJson(), CancellationToken.None);
                return Task.CompletedTask;
            });

        await using var bus = await TcpClusterMessageBusTestHelpers.CreateBusAsync(
            outboundConnectionFactory: (_, _) => Task.FromResult(connection));
        busHolder = bus;

        var result = await bus.FetchPeerSubscriptionsAsync(new Uri("https://peer1:5001/"), channelKey, CancellationToken.None);

        result.Should().BeEmpty();
    }

    [Fact]
    public async Task FetchPeerSubscriptionsAsync_Timeout_DegradesToEmptyArray()
    {
        var connection = TcpClusterMessageBusTestHelpers.CreateConnectionSubstitute();

        await using var bus = await TcpClusterMessageBusTestHelpers.CreateBusAsync(
            requestTimeout: TimeSpan.FromMilliseconds(50),
            outboundConnectionFactory: (_, _) => Task.FromResult(connection));

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

        await using var bus = await TcpClusterMessageBusTestHelpers.CreateBusAsync(channelResolver: channelResolver);

        var request = new TcpClusterRequestEnvelope(Guid.NewGuid(), TcpClusterRequestKind.FetchSubscriptions, null, channelKey, null);
        var response = await bus.BuildResponseAsync(request, CancellationToken.None);

        response.Success.Should().BeTrue();
        response.PayloadJson.Should().NotBeNull();
    }
}
