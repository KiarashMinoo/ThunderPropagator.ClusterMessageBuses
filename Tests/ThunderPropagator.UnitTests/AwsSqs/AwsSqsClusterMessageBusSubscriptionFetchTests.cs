using Amazon.SQS.Model;
using FluentAssertions;
using NSubstitute;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.AwsSqs;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.UnitTests.AwsSqs;

public class AwsSqsClusterMessageBusSubscriptionFetchTests
{
    [Fact]
    public async Task FetchPeerSubscriptionsAsync_Success_ReturnsTheDescriptors()
    {
        var descriptors = new[] { new ClusterSubscriptionDescriptor("sub-1", "req-1", "conn-1") };
        var channelKey = Guid.NewGuid();

        AwsSqsClusterMessageBus? busHolder = null;
        var sqs = AwsSqsClusterMessageBusTestHelpers.CreateSqsSubstitute();
        sqs.SendMessageAsync(Arg.Any<SendMessageRequest>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var request = ((SendMessageRequest)callInfo[0]).MessageBody.FromNJson<AwsSqsClusterRequestEnvelope>()!;
                var response = new AwsSqsClusterResponseEnvelope(request.CorrelationId, true, null, descriptors.ToNJson());
                _ = busHolder!.HandleReplyDeliveryAsync(response.ToNJson(), CancellationToken.None);
                return Task.FromResult(new SendMessageResponse());
            });

        await using var bus = await AwsSqsClusterMessageBusTestHelpers.CreateBusAsync(sqs: sqs);
        busHolder = bus;

        var result = await bus.FetchPeerSubscriptionsAsync(new Uri("https://peer1:5001/"), channelKey, CancellationToken.None);

        result.Should().ContainSingle(d => d.SubscriptionId == "sub-1");

        await sqs.Received(1).SendMessageAsync(
            Arg.Is<SendMessageRequest>(r =>
                r.MessageBody.FromNJson<AwsSqsClusterRequestEnvelope>()!.Kind == AwsSqsClusterRequestKind.FetchSubscriptions &&
                r.MessageBody.FromNJson<AwsSqsClusterRequestEnvelope>()!.ChannelKey == channelKey),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task FetchPeerSubscriptionsAsync_PeerRejectsTheRequest_DegradesToEmptyArray()
    {
        var channelKey = Guid.NewGuid();

        AwsSqsClusterMessageBus? busHolder = null;
        var sqs = AwsSqsClusterMessageBusTestHelpers.CreateSqsSubstitute();
        sqs.SendMessageAsync(Arg.Any<SendMessageRequest>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var request = ((SendMessageRequest)callInfo[0]).MessageBody.FromNJson<AwsSqsClusterRequestEnvelope>()!;
                var response = new AwsSqsClusterResponseEnvelope(request.CorrelationId, false, "no such channel", null);
                _ = busHolder!.HandleReplyDeliveryAsync(response.ToNJson(), CancellationToken.None);
                return Task.FromResult(new SendMessageResponse());
            });

        await using var bus = await AwsSqsClusterMessageBusTestHelpers.CreateBusAsync(sqs: sqs);
        busHolder = bus;

        var result = await bus.FetchPeerSubscriptionsAsync(new Uri("https://peer1:5001/"), channelKey, CancellationToken.None);

        result.Should().BeEmpty();
    }

    [Fact]
    public async Task FetchPeerSubscriptionsAsync_Timeout_DegradesToEmptyArray()
    {
        var sqs = AwsSqsClusterMessageBusTestHelpers.CreateSqsSubstitute();

        await using var bus = await AwsSqsClusterMessageBusTestHelpers.CreateBusAsync(sqs: sqs, requestTimeout: TimeSpan.FromMilliseconds(50));

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

        await using var bus = await AwsSqsClusterMessageBusTestHelpers.CreateBusAsync(channelResolver: channelResolver);

        var request = new AwsSqsClusterRequestEnvelope(Guid.NewGuid(), AwsSqsClusterRequestKind.FetchSubscriptions, null, channelKey, null, new Uri("https://requester:5002/"));
        var response = await bus.BuildResponseAsync(request, CancellationToken.None);

        response.Success.Should().BeTrue();
        response.PayloadJson.Should().NotBeNull();
    }
}
