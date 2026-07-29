using FluentAssertions;
using Grpc.Core;
using NSubstitute;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.Grpc;
using ThunderPropagator.ClusterMessageBuses.Grpc.Protos;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;
using ThunderPropagator.Infrastructure.Channels;

namespace ThunderPropagator.UnitTests.Grpc;

public class GrpcClusterMessageBusSubscriptionFetchTests
{
    [Fact]
    public async Task FetchPeerSubscriptionsAsync_Success_ReturnsTheDescriptors()
    {
        var descriptors = new[] { new ClusterSubscriptionDescriptor("sub-1", "req-1", "conn-1") };

        var subscriptionFetchClient = Substitute.For<ClusterSubscriptionFetch.ClusterSubscriptionFetchClient>();
        subscriptionFetchClient.FetchSubscriptionsAsync(Arg.Any<FetchSubscriptionsRequest>(), null, Arg.Any<DateTime?>(), Arg.Any<CancellationToken>())
            .Returns(_ => GrpcClusterMessageBusTestHelpers.CreateUnaryCall(new FetchSubscriptionsResponse { Success = true, DescriptorsJson = descriptors.ToNJson() }));

        var clients = new GrpcUnaryClients(Substitute.For<ClusterSnapshot.ClusterSnapshotClient>(), subscriptionFetchClient, null);

        await using var bus = await GrpcClusterMessageBusTestHelpers.CreateBusAsync(unaryClientsFactory: (_, _) => Task.FromResult(clients));

        var result = await bus.FetchPeerSubscriptionsAsync(new Uri("https://peer1:5001/"), Guid.NewGuid());

        result.Should().ContainSingle(d => d.SubscriptionId == "sub-1");
    }

    [Fact]
    public async Task FetchPeerSubscriptionsAsync_PeerRejectsTheRequest_DegradesToEmptyArray()
    {
        var subscriptionFetchClient = Substitute.For<ClusterSubscriptionFetch.ClusterSubscriptionFetchClient>();
        subscriptionFetchClient.FetchSubscriptionsAsync(Arg.Any<FetchSubscriptionsRequest>(), null, Arg.Any<DateTime?>(), Arg.Any<CancellationToken>())
            .Returns(_ => GrpcClusterMessageBusTestHelpers.CreateUnaryCall(new FetchSubscriptionsResponse { Success = false, ErrorMessage = "no such channel" }));

        var clients = new GrpcUnaryClients(Substitute.For<ClusterSnapshot.ClusterSnapshotClient>(), subscriptionFetchClient, null);

        await using var bus = await GrpcClusterMessageBusTestHelpers.CreateBusAsync(unaryClientsFactory: (_, _) => Task.FromResult(clients));

        var result = await bus.FetchPeerSubscriptionsAsync(new Uri("https://peer1:5001/"), Guid.NewGuid());

        result.Should().BeEmpty();
    }

    [Fact]
    public async Task FetchPeerSubscriptionsAsync_PeerUnavailable_DegradesToEmptyArrayInsteadOfThrowing()
    {
        var subscriptionFetchClient = Substitute.For<ClusterSubscriptionFetch.ClusterSubscriptionFetchClient>();
        subscriptionFetchClient.FetchSubscriptionsAsync(Arg.Any<FetchSubscriptionsRequest>(), null, Arg.Any<DateTime?>(), Arg.Any<CancellationToken>())
            .Returns(_ => GrpcClusterMessageBusTestHelpers.CreateFailingUnaryCall<FetchSubscriptionsResponse>(
                new RpcException(new Status(StatusCode.Unavailable, "peer unreachable"))));

        var clients = new GrpcUnaryClients(Substitute.For<ClusterSnapshot.ClusterSnapshotClient>(), subscriptionFetchClient, null);

        await using var bus = await GrpcClusterMessageBusTestHelpers.CreateBusAsync(unaryClientsFactory: (_, _) => Task.FromResult(clients));

        var result = await bus.FetchPeerSubscriptionsAsync(new Uri("https://unreachable-peer:5001/"), Guid.NewGuid());

        result.Should().BeEmpty("FetchPeerSubscriptionsAsync is called against every discovered peer, so an unreachable one must degrade gracefully rather than fail the whole bootstrap");
    }

    [Fact]
    public async Task BuildFetchSubscriptionsResponseAsync_ReturnsTheResolvedChannelsLocalDescriptors()
    {
        var channelKey = Guid.NewGuid();
        var channel = Substitute.For<IChannel>();
        var descriptors = new[] { new ClusterSubscriptionDescriptor("sub-1", "req-1", "conn-1") };
        channel.GetLocalClusterSubscriptionDescriptors().Returns(descriptors);

        var resolver = Substitute.For<IClusterChannelResolver>();
        resolver.GetChannel(channelKey).Returns(channel);

        await using var bus = await GrpcClusterMessageBusTestHelpers.CreateBusAsync(channelResolver: resolver);

        var response = await bus.BuildFetchSubscriptionsResponseAsync(new FetchSubscriptionsRequest { ChannelKey = channelKey.ToString() }, CancellationToken.None);

        response.Success.Should().BeTrue();
        var returned = response.DescriptorsJson.FromNJson<ClusterSubscriptionDescriptor[]>();
        returned.Should().ContainSingle(d => d.SubscriptionId == "sub-1" && d.ConnectionId == "conn-1");
    }

    [Fact]
    public async Task BuildFetchSubscriptionsResponseAsync_UnknownChannelKey_ReturnsAFailureResponseInsteadOfThrowing()
    {
        var channelKey = Guid.NewGuid();
        var resolver = Substitute.For<IClusterChannelResolver>();
        resolver.GetChannel(channelKey).Returns(_ => throw new InvalidChannelKeyException(channelKey, new KeyNotFoundException()));

        await using var bus = await GrpcClusterMessageBusTestHelpers.CreateBusAsync(channelResolver: resolver);

        var response = await bus.BuildFetchSubscriptionsResponseAsync(new FetchSubscriptionsRequest { ChannelKey = channelKey.ToString() }, CancellationToken.None);

        response.Success.Should().BeFalse();
    }
}
