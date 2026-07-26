using Confluent.Kafka;
using FluentAssertions;
using NSubstitute;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.Kafka;
using ThunderPropagator.Infrastructure.Channels;

namespace ThunderPropagator.UnitTests.Kafka;

public class KafkaClusterMessageBusSubscriptionFetchTests
{
    [Fact]
    public async Task BuildResponseAsync_FetchSubscriptions_ReturnsTheResolvedChannelsLocalDescriptors()
    {
        var channelKey = Guid.NewGuid();
        var channel = Substitute.For<IChannel>();
        var descriptors = new[] { new ClusterSubscriptionDescriptor("sub-1", "req-1", "conn-1") };
        channel.GetLocalClusterSubscriptionDescriptors().Returns(descriptors);

        var resolver = Substitute.For<IKafkaChannelResolver>();
        resolver.GetChannel(channelKey).Returns(channel);

        await using var bus = KafkaClusterMessageBusTestHelpers.CreateBus(channelResolver: resolver);
        var request = new KafkaClusterRequestEnvelope(Guid.NewGuid(), KafkaClusterRequestKind.FetchSubscriptions, null, channelKey, null, new Uri("https://requester:5001/"));

        var response = await bus.BuildResponseAsync(request, CancellationToken.None);

        response.Success.Should().BeTrue();
        var returned = response.PayloadJson!.FromNJson<ClusterSubscriptionDescriptor[]>();
        returned.Should().ContainSingle(d => d.SubscriptionId == "sub-1" && d.ConnectionId == "conn-1");
    }

    [Fact]
    public async Task BuildResponseAsync_UnknownChannelKey_ReturnsAFailureResponseInsteadOfThrowing()
    {
        var channelKey = Guid.NewGuid();
        var resolver = Substitute.For<IKafkaChannelResolver>();
        resolver.GetChannel(channelKey).Returns(_ => throw new InvalidChannelKeyException(channelKey, new KeyNotFoundException()));

        await using var bus = KafkaClusterMessageBusTestHelpers.CreateBus(channelResolver: resolver);
        var request = new KafkaClusterRequestEnvelope(Guid.NewGuid(), KafkaClusterRequestKind.FetchSubscriptions, null, channelKey, null, new Uri("https://requester:5001/"));

        var response = await bus.BuildResponseAsync(request, CancellationToken.None);

        response.Success.Should().BeFalse();
        response.PayloadJson.Should().BeNull();
    }

    [Fact]
    public async Task FetchPeerSubscriptionsAsync_PeerNeverReplies_ReturnsEmptyInsteadOfThrowing()
    {
        var producer = Substitute.For<IProducer<string, string>>();
        producer.ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, string>>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<DeliveryResult<string, string>>(null!));

        await using var bus = KafkaClusterMessageBusTestHelpers.CreateBus(producer: producer, requestTimeout: TimeSpan.FromMilliseconds(50));

        var result = await bus.FetchPeerSubscriptionsAsync(new Uri("https://unreachable-peer:5001/"), Guid.NewGuid());

        result.Should().BeEmpty("FetchPeerSubscriptionsAsync is called against every discovered peer, so an unreachable one must degrade gracefully rather than fail the whole bootstrap");
    }

    [Fact]
    public async Task FetchPeerSubscriptionsAsync_MatchingReplyArrives_ReturnsTheDescriptors()
    {
        KafkaClusterRequestEnvelope? sentRequest = null;
        var producer = Substitute.For<IProducer<string, string>>();
        producer.ProduceAsync(
                Arg.Any<string>(),
                Arg.Do<Message<string, string>>(m => sentRequest = m.Value.FromNJson<KafkaClusterRequestEnvelope>()),
                Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<DeliveryResult<string, string>>(null!));

        await using var bus = KafkaClusterMessageBusTestHelpers.CreateBus(producer: producer, requestTimeout: TimeSpan.FromSeconds(5));

        var fetchTask = bus.FetchPeerSubscriptionsAsync(new Uri("https://peer:5001/"), Guid.NewGuid());

        sentRequest.Should().NotBeNull();
        var descriptors = new[] { new ClusterSubscriptionDescriptor("sub-9", "req-9", "conn-9") };
        bus.TryCompletePendingRequest(new KafkaClusterResponseEnvelope(sentRequest!.CorrelationId, true, null, descriptors.ToNJson()));

        var result = await fetchTask;

        result.Should().ContainSingle(d => d.SubscriptionId == "sub-9");
    }
}
