using FluentAssertions;
using Google.Cloud.PubSub.V1;
using NSubstitute;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.GcpPubSub;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.UnitTests.GcpPubSub;

public class GcpPubSubClusterMessageBusSubscriptionFetchTests
{
    [Fact]
    public async Task FetchPeerSubscriptionsAsync_Success_ReturnsTheDescriptors()
    {
        var descriptors = new[] { new ClusterSubscriptionDescriptor("sub-1", "req-1", "conn-1") };
        var channelKey = Guid.NewGuid();

        GcpPubSubClusterMessageBus? busHolder = null;
        var publisher = GcpPubSubClusterMessageBusTestHelpers.CreatePublisherSubstitute();
        publisher.PublishAsync(Arg.Any<TopicName>(), Arg.Any<IEnumerable<PubsubMessage>>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var topicName = (TopicName)callInfo[0];
                var messages = (IEnumerable<PubsubMessage>)callInfo[1];
                if (topicName.TopicId.Contains("-requests-"))
                {
                    var request = messages.Single().Data.ToStringUtf8().FromNJson<GcpPubSubClusterRequestEnvelope>()!;
                    var response = new GcpPubSubClusterResponseEnvelope(request.CorrelationId, true, null, descriptors.ToNJson());
                    _ = busHolder!.HandleReplyDeliveryAsync(response.ToNJson(), CancellationToken.None);
                }

                return Task.FromResult(new PublishResponse());
            });

        await using var bus = await GcpPubSubClusterMessageBusTestHelpers.CreateBusAsync(publisher: publisher);
        busHolder = bus;

        var result = await bus.FetchPeerSubscriptionsAsync(new Uri("https://peer1:5001/"), channelKey, CancellationToken.None);

        result.Should().ContainSingle(d => d.SubscriptionId == "sub-1");

        await publisher.Received(1).PublishAsync(
            Arg.Any<TopicName>(),
            Arg.Is<IEnumerable<PubsubMessage>>(messages =>
                messages.Single().Data.ToStringUtf8().FromNJson<GcpPubSubClusterRequestEnvelope>()!.Kind == GcpPubSubClusterRequestKind.FetchSubscriptions &&
                messages.Single().Data.ToStringUtf8().FromNJson<GcpPubSubClusterRequestEnvelope>()!.ChannelKey == channelKey),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task FetchPeerSubscriptionsAsync_PeerRejectsTheRequest_DegradesToEmptyArray()
    {
        var channelKey = Guid.NewGuid();

        GcpPubSubClusterMessageBus? busHolder = null;
        var publisher = GcpPubSubClusterMessageBusTestHelpers.CreatePublisherSubstitute();
        publisher.PublishAsync(Arg.Any<TopicName>(), Arg.Any<IEnumerable<PubsubMessage>>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var topicName = (TopicName)callInfo[0];
                var messages = (IEnumerable<PubsubMessage>)callInfo[1];
                if (topicName.TopicId.Contains("-requests-"))
                {
                    var request = messages.Single().Data.ToStringUtf8().FromNJson<GcpPubSubClusterRequestEnvelope>()!;
                    var response = new GcpPubSubClusterResponseEnvelope(request.CorrelationId, false, "no such channel", null);
                    _ = busHolder!.HandleReplyDeliveryAsync(response.ToNJson(), CancellationToken.None);
                }

                return Task.FromResult(new PublishResponse());
            });

        await using var bus = await GcpPubSubClusterMessageBusTestHelpers.CreateBusAsync(publisher: publisher);
        busHolder = bus;

        var result = await bus.FetchPeerSubscriptionsAsync(new Uri("https://peer1:5001/"), channelKey, CancellationToken.None);

        result.Should().BeEmpty();
    }

    [Fact]
    public async Task FetchPeerSubscriptionsAsync_Timeout_DegradesToEmptyArray()
    {
        var publisher = GcpPubSubClusterMessageBusTestHelpers.CreatePublisherSubstitute();

        await using var bus = await GcpPubSubClusterMessageBusTestHelpers.CreateBusAsync(publisher: publisher, requestTimeout: TimeSpan.FromMilliseconds(50));

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

        await using var bus = await GcpPubSubClusterMessageBusTestHelpers.CreateBusAsync(channelResolver: channelResolver);

        var request = new GcpPubSubClusterRequestEnvelope(Guid.NewGuid(), GcpPubSubClusterRequestKind.FetchSubscriptions, null, channelKey, null, new Uri("https://requester:5002/"));
        var response = await bus.BuildResponseAsync(request, CancellationToken.None);

        response.Success.Should().BeTrue();
        response.PayloadJson.Should().NotBeNull();
    }
}
