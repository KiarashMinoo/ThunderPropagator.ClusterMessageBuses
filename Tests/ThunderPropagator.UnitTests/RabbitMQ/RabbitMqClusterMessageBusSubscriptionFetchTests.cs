using FluentAssertions;
using NSubstitute;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.RabbitMQ;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;
using ThunderPropagator.Infrastructure.Channels;

namespace ThunderPropagator.UnitTests.RabbitMQ;

public class RabbitMqClusterMessageBusSubscriptionFetchTests
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

        await using var bus = await RabbitMqClusterMessageBusTestHelpers.CreateBusAsync(channelResolver: resolver);
        var request = new RabbitMqClusterRequestEnvelope(Guid.NewGuid(), RabbitMqClusterRequestKind.FetchSubscriptions, null, channelKey, null, new Uri("https://requester:5001/"));

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

        await using var bus = await RabbitMqClusterMessageBusTestHelpers.CreateBusAsync(channelResolver: resolver);
        var request = new RabbitMqClusterRequestEnvelope(Guid.NewGuid(), RabbitMqClusterRequestKind.FetchSubscriptions, null, channelKey, null, new Uri("https://requester:5001/"));

        var response = await bus.BuildResponseAsync(request, CancellationToken.None);

        response.Success.Should().BeFalse();
        response.PayloadJson.Should().BeNull();
    }

    [Fact]
    public async Task FetchPeerSubscriptionsAsync_PeerNeverReplies_ReturnsEmptyInsteadOfThrowing()
    {
        await using var bus = await RabbitMqClusterMessageBusTestHelpers.CreateBusAsync(requestTimeout: TimeSpan.FromMilliseconds(50));

        var result = await bus.FetchPeerSubscriptionsAsync(new Uri("https://unreachable-peer:5001/"), Guid.NewGuid());

        result.Should().BeEmpty("FetchPeerSubscriptionsAsync is called against every discovered peer, so an unreachable one must degrade gracefully rather than fail the whole bootstrap");
    }

    [Fact]
    public async Task FetchPeerSubscriptionsAsync_MatchingReplyArrives_ReturnsTheDescriptors()
    {
        var connection = RabbitMqClusterMessageBusTestHelpers.CreateTrackedConnection(out var channels);
        await using var bus = await RabbitMqClusterMessageBusTestHelpers.CreateBusAsync(connection: connection, requestTimeout: TimeSpan.FromSeconds(5));

        var fetchTask = bus.FetchPeerSubscriptionsAsync(new Uri("https://peer:5001/"), Guid.NewGuid());

        var call = channels[0].ReceivedCalls()
            .Last(c => c.GetMethodInfo().Name == nameof(RabbitMQ.Client.IChannel.BasicPublishAsync));
        var publishedBody = (ReadOnlyMemory<byte>)call.GetArguments()[4]!;
        var sentRequest = System.Text.Encoding.UTF8.GetString(publishedBody.Span).FromNJson<RabbitMqClusterRequestEnvelope>()!;

        var descriptors = new[] { new ClusterSubscriptionDescriptor("sub-9", "req-9", "conn-9") };
        bus.TryCompletePendingRequest(new RabbitMqClusterResponseEnvelope(sentRequest.CorrelationId, true, null, descriptors.ToNJson()));

        var result = await fetchTask;

        result.Should().ContainSingle(d => d.SubscriptionId == "sub-9");
    }
}
