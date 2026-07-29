using Amazon.SQS;
using Amazon.SQS.Model;
using FluentAssertions;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.AwsSqs;

namespace ThunderPropagator.UnitTests.AwsSqs;

public class AwsSqsClusterMessageBusRequestReplyTests
{
    /// <summary>
    /// Wires an SQS substitute whose <c>SendMessageAsync</c>, whenever it observes a message headed
    /// to a request queue, synchronously turns around and delivers a matching response back into
    /// <paramref name="bus"/> via <see cref="AwsSqsClusterMessageBus.HandleReplyDeliveryAsync"/> —
    /// simulating the answering peer's reply arriving on this node's own reply queue, without a
    /// real poll loop or real AWS resources.
    /// </summary>
    private static IAmazonSQS CreateSqsWithAutoReply(
        Func<AwsSqsClusterRequestEnvelope, AwsSqsClusterResponseEnvelope> buildResponse,
        Func<AwsSqsClusterMessageBus> bus)
    {
        var sqs = AwsSqsClusterMessageBusTestHelpers.CreateSqsSubstitute();
        sqs.SendMessageAsync(Arg.Any<SendMessageRequest>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var request = (SendMessageRequest)callInfo[0];
                var parsedRequest = request.MessageBody.FromNJson<AwsSqsClusterRequestEnvelope>();
                if (parsedRequest is not null)
                {
                    var response = buildResponse(parsedRequest);
                    _ = bus().HandleReplyDeliveryAsync(response.ToNJson(), CancellationToken.None);
                }
                return Task.FromResult(new SendMessageResponse());
            });

        return sqs;
    }

    [Fact]
    public async Task SendRequestAsync_Success_ReturnsTheResponseEnvelope()
    {
        AwsSqsClusterMessageBus? busHolder = null;
        var sqs = CreateSqsWithAutoReply(
            request => new AwsSqsClusterResponseEnvelope(request.CorrelationId, true, null, "\"payload\""),
            () => busHolder!);

        await using var bus = await AwsSqsClusterMessageBusTestHelpers.CreateBusAsync(sqs: sqs);
        busHolder = bus;

        var response = await bus.SendRequestAsync(
            new Uri("https://peer1:5001/"), AwsSqsClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null, CancellationToken.None);

        response.Success.Should().BeTrue();
        response.PayloadJson.Should().Be("\"payload\"");
    }

    [Fact]
    public async Task SendRequestAsync_RejectedResponse_ThrowsInvalidOperationException()
    {
        AwsSqsClusterMessageBus? busHolder = null;
        var sqs = CreateSqsWithAutoReply(
            request => new AwsSqsClusterResponseEnvelope(request.CorrelationId, false, "channel not found", null),
            () => busHolder!);

        await using var bus = await AwsSqsClusterMessageBusTestHelpers.CreateBusAsync(sqs: sqs);
        busHolder = bus;

        var act = async () => await bus.SendRequestAsync(
            new Uri("https://peer1:5001/"), AwsSqsClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null, CancellationToken.None);

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("*channel not found*");
    }

    [Fact]
    public async Task SendRequestAsync_NoResponseEverArrives_ThrowsTimeoutException()
    {
        var sqs = AwsSqsClusterMessageBusTestHelpers.CreateSqsSubstitute();

        await using var bus = await AwsSqsClusterMessageBusTestHelpers.CreateBusAsync(
            sqs: sqs, requestTimeout: TimeSpan.FromMilliseconds(50));

        var act = async () => await bus.SendRequestAsync(
            new Uri("https://peer1:5001/"), AwsSqsClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null, CancellationToken.None);

        await act.Should().ThrowAsync<TimeoutException>();
    }

    [Fact]
    public async Task HandleRequestDeliveryAsync_MalformedPayload_DoesNotThrow()
    {
        await using var bus = await AwsSqsClusterMessageBusTestHelpers.CreateBusAsync();

        var act = async () => await bus.HandleRequestDeliveryAsync("{ not valid json ", CancellationToken.None);

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task HandleIncomingRequestAsync_SendsTheResponseToTheRequestersReplyQueue()
    {
        var sqs = AwsSqsClusterMessageBusTestHelpers.CreateSqsSubstitute();
        await using var bus = await AwsSqsClusterMessageBusTestHelpers.CreateBusAsync(sqs: sqs);

        var replyToEndpoint = new Uri("https://requester-node:5002/");
        var request = new AwsSqsClusterRequestEnvelope(Guid.NewGuid(), (AwsSqsClusterRequestKind)999, null, Guid.NewGuid(), null, replyToEndpoint);

        await bus.HandleIncomingRequestAsync(request, CancellationToken.None);

        var expectedReplyQueueUrl = AwsSqsClusterMessageBusTestHelpers.QueueUrlFor("tp-cluster-replies-requester-node-5002");

        await sqs.Received(1).SendMessageAsync(
            Arg.Is<SendMessageRequest>(r => r.QueueUrl == expectedReplyQueueUrl &&
                r.MessageBody.FromNJson<AwsSqsClusterResponseEnvelope>()!.CorrelationId == request.CorrelationId &&
                r.MessageBody.FromNJson<AwsSqsClusterResponseEnvelope>()!.Success == false),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task HandleIncomingRequestAsync_ReplySendFails_DoesNotThrow()
    {
        var sqs = AwsSqsClusterMessageBusTestHelpers.CreateSqsSubstitute();
        sqs.SendMessageAsync(Arg.Any<SendMessageRequest>(), Arg.Any<CancellationToken>())
            .Throws(new InvalidOperationException("simulated send failure"));

        await using var bus = await AwsSqsClusterMessageBusTestHelpers.CreateBusAsync(sqs: sqs);

        var request = new AwsSqsClusterRequestEnvelope(Guid.NewGuid(), AwsSqsClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null, new Uri("https://requester:5002/"));

        var act = async () => await bus.HandleIncomingRequestAsync(request, CancellationToken.None);

        await act.Should().NotThrowAsync("a failure sending the reply back must not crash the poll loop");
    }

    [Fact]
    public async Task TryCompletePendingRequest_NoPendingRequestForCorrelationId_ReturnsFalse()
    {
        await using var bus = await AwsSqsClusterMessageBusTestHelpers.CreateBusAsync();

        var response = new AwsSqsClusterResponseEnvelope(Guid.NewGuid(), true, null, null);

        bus.TryCompletePendingRequest(response).Should().BeFalse();
    }

    [Fact]
    public async Task BuildResponseAsync_HandlerThrows_ReturnsUnsuccessfulResponseInsteadOfPropagating()
    {
        var channelResolver = NSubstitute.Substitute.For<ThunderPropagator.ClusterMessageBuses.SharedKernel.IClusterChannelResolver>();
        channelResolver.GetChannel(Arg.Any<Guid>()).Returns(_ => throw new InvalidOperationException("no such channel"));

        await using var bus = await AwsSqsClusterMessageBusTestHelpers.CreateBusAsync(channelResolver: channelResolver);

        var request = new AwsSqsClusterRequestEnvelope(Guid.NewGuid(), AwsSqsClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null, new Uri("https://requester:5002/"));

        var response = await bus.BuildResponseAsync(request, CancellationToken.None);

        response.Success.Should().BeFalse();
        response.ErrorMessage.Should().Contain("no such channel");
    }
}
