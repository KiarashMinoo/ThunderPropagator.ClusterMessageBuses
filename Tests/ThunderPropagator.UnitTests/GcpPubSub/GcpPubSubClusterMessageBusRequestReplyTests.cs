using FluentAssertions;
using Google.Cloud.PubSub.V1;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.GcpPubSub;

namespace ThunderPropagator.UnitTests.GcpPubSub;

public class GcpPubSubClusterMessageBusRequestReplyTests
{
    /// <summary>
    /// Wires a publisher substitute whose <c>PublishAsync</c>, whenever it observes a message headed
    /// to a request topic, synchronously turns around and delivers a matching response back into
    /// <paramref name="bus"/> via <see cref="GcpPubSubClusterMessageBus.HandleReplyDeliveryAsync"/> —
    /// simulating the answering peer's reply arriving on this node's own reply subscription, without
    /// a real poll loop or real GCP resources.
    /// </summary>
    private static PublisherServiceApiClient CreatePublisherWithAutoReply(
        Func<GcpPubSubClusterRequestEnvelope, GcpPubSubClusterResponseEnvelope> buildResponse,
        Func<GcpPubSubClusterMessageBus> bus)
    {
        var publisher = GcpPubSubClusterMessageBusTestHelpers.CreatePublisherSubstitute();
        publisher.PublishAsync(Arg.Any<TopicName>(), Arg.Any<IEnumerable<PubsubMessage>>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var topicName = (TopicName)callInfo[0];
                var messages = (IEnumerable<PubsubMessage>)callInfo[1];
                if (topicName.TopicId.Contains("-requests-"))
                {
                    var parsedRequest = messages.Single().Data.ToStringUtf8().FromNJson<GcpPubSubClusterRequestEnvelope>();
                    if (parsedRequest is not null)
                    {
                        var response = buildResponse(parsedRequest);
                        _ = bus().HandleReplyDeliveryAsync(response.ToNJson(), CancellationToken.None);
                    }
                }

                return Task.FromResult(new PublishResponse());
            });

        return publisher;
    }

    [Fact]
    public async Task SendRequestAsync_Success_ReturnsTheResponseEnvelope()
    {
        GcpPubSubClusterMessageBus? busHolder = null;
        var publisher = CreatePublisherWithAutoReply(
            request => new GcpPubSubClusterResponseEnvelope(request.CorrelationId, true, null, "\"payload\""),
            () => busHolder!);

        await using var bus = await GcpPubSubClusterMessageBusTestHelpers.CreateBusAsync(publisher: publisher);
        busHolder = bus;

        var response = await bus.SendRequestAsync(
            new Uri("https://peer1:5001/"), GcpPubSubClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null, CancellationToken.None);

        response.Success.Should().BeTrue();
        response.PayloadJson.Should().Be("\"payload\"");
    }

    [Fact]
    public async Task SendRequestAsync_RejectedResponse_ThrowsInvalidOperationException()
    {
        GcpPubSubClusterMessageBus? busHolder = null;
        var publisher = CreatePublisherWithAutoReply(
            request => new GcpPubSubClusterResponseEnvelope(request.CorrelationId, false, "channel not found", null),
            () => busHolder!);

        await using var bus = await GcpPubSubClusterMessageBusTestHelpers.CreateBusAsync(publisher: publisher);
        busHolder = bus;

        var act = async () => await bus.SendRequestAsync(
            new Uri("https://peer1:5001/"), GcpPubSubClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null, CancellationToken.None);

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("*channel not found*");
    }

    [Fact]
    public async Task SendRequestAsync_NoResponseEverArrives_ThrowsTimeoutException()
    {
        var publisher = GcpPubSubClusterMessageBusTestHelpers.CreatePublisherSubstitute();

        await using var bus = await GcpPubSubClusterMessageBusTestHelpers.CreateBusAsync(
            publisher: publisher, requestTimeout: TimeSpan.FromMilliseconds(50));

        var act = async () => await bus.SendRequestAsync(
            new Uri("https://peer1:5001/"), GcpPubSubClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null, CancellationToken.None);

        await act.Should().ThrowAsync<TimeoutException>();
    }

    [Fact]
    public async Task HandleRequestDeliveryAsync_MalformedPayload_DoesNotThrow()
    {
        await using var bus = await GcpPubSubClusterMessageBusTestHelpers.CreateBusAsync();

        var act = async () => await bus.HandleRequestDeliveryAsync("{ not valid json ", CancellationToken.None);

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task HandleIncomingRequestAsync_SendsTheResponseToTheRequestersReplyTopic()
    {
        var publisher = GcpPubSubClusterMessageBusTestHelpers.CreatePublisherSubstitute();
        await using var bus = await GcpPubSubClusterMessageBusTestHelpers.CreateBusAsync(publisher: publisher);

        var replyToEndpoint = new Uri("https://requester-node:5002/");
        var request = new GcpPubSubClusterRequestEnvelope(Guid.NewGuid(), (GcpPubSubClusterRequestKind)999, null, Guid.NewGuid(), null, replyToEndpoint);

        await bus.HandleIncomingRequestAsync(request, CancellationToken.None);

        var expectedReplyTopicId = "tp-cluster-replies-requester-node-5002";

        await publisher.Received(1).PublishAsync(
            Arg.Is<TopicName>(t => t.TopicId == expectedReplyTopicId),
            Arg.Is<IEnumerable<PubsubMessage>>(messages =>
                messages.Single().Data.ToStringUtf8().FromNJson<GcpPubSubClusterResponseEnvelope>()!.CorrelationId == request.CorrelationId &&
                messages.Single().Data.ToStringUtf8().FromNJson<GcpPubSubClusterResponseEnvelope>()!.Success == false),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task HandleIncomingRequestAsync_ReplySendFails_DoesNotThrow()
    {
        var publisher = GcpPubSubClusterMessageBusTestHelpers.CreatePublisherSubstitute();
        publisher.PublishAsync(Arg.Any<TopicName>(), Arg.Any<IEnumerable<PubsubMessage>>(), Arg.Any<CancellationToken>())
            .Throws(new InvalidOperationException("simulated send failure"));

        await using var bus = await GcpPubSubClusterMessageBusTestHelpers.CreateBusAsync(publisher: publisher);

        var request = new GcpPubSubClusterRequestEnvelope(Guid.NewGuid(), GcpPubSubClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null, new Uri("https://requester:5002/"));

        var act = async () => await bus.HandleIncomingRequestAsync(request, CancellationToken.None);

        await act.Should().NotThrowAsync("a failure sending the reply back must not crash the poll loop");
    }

    [Fact]
    public async Task TryCompletePendingRequest_NoPendingRequestForCorrelationId_ReturnsFalse()
    {
        await using var bus = await GcpPubSubClusterMessageBusTestHelpers.CreateBusAsync();

        var response = new GcpPubSubClusterResponseEnvelope(Guid.NewGuid(), true, null, null);

        bus.TryCompletePendingRequest(response).Should().BeFalse();
    }

    [Fact]
    public async Task BuildResponseAsync_HandlerThrows_ReturnsUnsuccessfulResponseInsteadOfPropagating()
    {
        var channelResolver = NSubstitute.Substitute.For<ThunderPropagator.ClusterMessageBuses.SharedKernel.IClusterChannelResolver>();
        channelResolver.GetChannel(Arg.Any<Guid>()).Returns(_ => throw new InvalidOperationException("no such channel"));

        await using var bus = await GcpPubSubClusterMessageBusTestHelpers.CreateBusAsync(channelResolver: channelResolver);

        var request = new GcpPubSubClusterRequestEnvelope(Guid.NewGuid(), GcpPubSubClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null, new Uri("https://requester:5002/"));

        var response = await bus.BuildResponseAsync(request, CancellationToken.None);

        response.Success.Should().BeFalse();
        response.ErrorMessage.Should().Contain("no such channel");
    }
}
