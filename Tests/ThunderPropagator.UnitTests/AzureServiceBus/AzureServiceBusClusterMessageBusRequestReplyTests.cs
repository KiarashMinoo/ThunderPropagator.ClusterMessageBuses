using Azure.Messaging.ServiceBus;
using FluentAssertions;
using NSubstitute;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.AzureServiceBus;

namespace ThunderPropagator.UnitTests.AzureServiceBus;

public class AzureServiceBusClusterMessageBusRequestReplyTests
{
    /// <summary>
    /// Wires a client substitute whose sender for any request-queue entity, whenever it observes a
    /// message, synchronously turns around and delivers a matching response back into
    /// <paramref name="bus"/> via <see cref="AzureServiceBusClusterMessageBus.HandleReplyDeliveryAsync"/> —
    /// simulating the answering peer's reply arriving on this node's own reply queue, without a real
    /// poll loop or a live Service Bus namespace.
    /// </summary>
    private static ServiceBusClient CreateClientWithAutoReply(
        Func<AzureServiceBusClusterRequestEnvelope, AzureServiceBusClusterResponseEnvelope> buildResponse,
        Func<AzureServiceBusClusterMessageBus> bus)
    {
        ServiceBusSender SenderFactory(string entityName)
        {
            var sender = AzureServiceBusClusterMessageBusTestHelpers.CreateSenderSubstitute();

            if (entityName.Contains("-requests-"))
            {
                sender.SendMessageAsync(Arg.Any<ServiceBusMessage>(), Arg.Any<CancellationToken>())
                    .Returns(callInfo =>
                    {
                        var message = (ServiceBusMessage)callInfo[0];
                        var parsedRequest = message.Body.ToString().FromNJson<AzureServiceBusClusterRequestEnvelope>();
                        if (parsedRequest is not null)
                        {
                            var response = buildResponse(parsedRequest);
                            _ = bus().HandleReplyDeliveryAsync(response.ToNJson(), CancellationToken.None);
                        }
                        return Task.CompletedTask;
                    });
            }

            return sender;
        }

        return AzureServiceBusClusterMessageBusTestHelpers.CreateClientSubstitute(senderFactory: SenderFactory);
    }

    [Fact]
    public async Task SendRequestAsync_Success_ReturnsTheResponseEnvelope()
    {
        AzureServiceBusClusterMessageBus? busHolder = null;
        var client = CreateClientWithAutoReply(
            request => new AzureServiceBusClusterResponseEnvelope(request.CorrelationId, true, null, "\"payload\""),
            () => busHolder!);

        await using var bus = await AzureServiceBusClusterMessageBusTestHelpers.CreateBusAsync(client: client);
        busHolder = bus;

        var response = await bus.SendRequestAsync(
            new Uri("https://peer1:5001/"), AzureServiceBusClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null, CancellationToken.None);

        response.Success.Should().BeTrue();
        response.PayloadJson.Should().Be("\"payload\"");
    }

    [Fact]
    public async Task SendRequestAsync_RejectedResponse_ThrowsInvalidOperationException()
    {
        AzureServiceBusClusterMessageBus? busHolder = null;
        var client = CreateClientWithAutoReply(
            request => new AzureServiceBusClusterResponseEnvelope(request.CorrelationId, false, "channel not found", null),
            () => busHolder!);

        await using var bus = await AzureServiceBusClusterMessageBusTestHelpers.CreateBusAsync(client: client);
        busHolder = bus;

        var act = async () => await bus.SendRequestAsync(
            new Uri("https://peer1:5001/"), AzureServiceBusClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null, CancellationToken.None);

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("*channel not found*");
    }

    [Fact]
    public async Task SendRequestAsync_NoResponseEverArrives_ThrowsTimeoutException()
    {
        await using var bus = await AzureServiceBusClusterMessageBusTestHelpers.CreateBusAsync(requestTimeout: TimeSpan.FromMilliseconds(50));

        var act = async () => await bus.SendRequestAsync(
            new Uri("https://peer1:5001/"), AzureServiceBusClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null, CancellationToken.None);

        await act.Should().ThrowAsync<TimeoutException>();
    }

    [Fact]
    public async Task HandleRequestDeliveryAsync_MalformedPayload_DoesNotThrow()
    {
        await using var bus = await AzureServiceBusClusterMessageBusTestHelpers.CreateBusAsync();

        var act = async () => await bus.HandleRequestDeliveryAsync("{ not valid json ", CancellationToken.None);

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task HandleIncomingRequestAsync_SendsTheResponseToTheRequestersReplyQueue()
    {
        var sender = AzureServiceBusClusterMessageBusTestHelpers.CreateSenderSubstitute();
        var client = AzureServiceBusClusterMessageBusTestHelpers.CreateClientSubstitute(senderFactory: _ => sender);
        await using var bus = await AzureServiceBusClusterMessageBusTestHelpers.CreateBusAsync(client: client);

        var replyToEndpoint = new Uri("https://requester-node:5002/");
        var request = new AzureServiceBusClusterRequestEnvelope(Guid.NewGuid(), (AzureServiceBusClusterRequestKind)999, null, Guid.NewGuid(), null, replyToEndpoint);

        await bus.HandleIncomingRequestAsync(request, CancellationToken.None);

        var expectedReplyQueueName = "tp-cluster-replies-requester-node-5002";

        client.Received(1).CreateSender(expectedReplyQueueName);
        await sender.Received(1).SendMessageAsync(
            Arg.Is<ServiceBusMessage>(m =>
                m.Body.ToString().FromNJson<AzureServiceBusClusterResponseEnvelope>()!.CorrelationId == request.CorrelationId &&
                m.Body.ToString().FromNJson<AzureServiceBusClusterResponseEnvelope>()!.Success == false),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task HandleIncomingRequestAsync_ReplySendFails_DoesNotThrow()
    {
        var sender = AzureServiceBusClusterMessageBusTestHelpers.CreateSenderSubstitute();
        sender.SendMessageAsync(Arg.Any<ServiceBusMessage>(), Arg.Any<CancellationToken>())
            .Returns(_ => throw new InvalidOperationException("simulated send failure"));
        var client = AzureServiceBusClusterMessageBusTestHelpers.CreateClientSubstitute(senderFactory: _ => sender);

        await using var bus = await AzureServiceBusClusterMessageBusTestHelpers.CreateBusAsync(client: client);

        var request = new AzureServiceBusClusterRequestEnvelope(Guid.NewGuid(), AzureServiceBusClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null, new Uri("https://requester:5002/"));

        var act = async () => await bus.HandleIncomingRequestAsync(request, CancellationToken.None);

        await act.Should().NotThrowAsync("a failure sending the reply back must not crash the poll loop");
    }

    [Fact]
    public async Task TryCompletePendingRequest_NoPendingRequestForCorrelationId_ReturnsFalse()
    {
        await using var bus = await AzureServiceBusClusterMessageBusTestHelpers.CreateBusAsync();

        var response = new AzureServiceBusClusterResponseEnvelope(Guid.NewGuid(), true, null, null);

        bus.TryCompletePendingRequest(response).Should().BeFalse();
    }

    [Fact]
    public async Task BuildResponseAsync_HandlerThrows_ReturnsUnsuccessfulResponseInsteadOfPropagating()
    {
        var channelResolver = NSubstitute.Substitute.For<ThunderPropagator.ClusterMessageBuses.SharedKernel.IClusterChannelResolver>();
        channelResolver.GetChannel(Arg.Any<Guid>()).Returns(_ => throw new InvalidOperationException("no such channel"));

        await using var bus = await AzureServiceBusClusterMessageBusTestHelpers.CreateBusAsync(channelResolver: channelResolver);

        var request = new AzureServiceBusClusterRequestEnvelope(Guid.NewGuid(), AzureServiceBusClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null, new Uri("https://requester:5002/"));

        var response = await bus.BuildResponseAsync(request, CancellationToken.None);

        response.Success.Should().BeFalse();
        response.ErrorMessage.Should().Contain("no such channel");
    }
}
