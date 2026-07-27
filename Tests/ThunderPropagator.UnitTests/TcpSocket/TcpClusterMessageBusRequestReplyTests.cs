using FluentAssertions;
using NSubstitute;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.TcpSocket;

namespace ThunderPropagator.UnitTests.TcpSocket;

public class TcpClusterMessageBusRequestReplyTests
{
    /// <summary>
    /// Builds a connection substitute whose <c>SendFrameAsync</c>, whenever it observes a
    /// <see cref="TcpClusterFrameKind.Request"/> frame, synchronously turns around and delivers
    /// <paramref name="buildResponse"/>'s response back into <paramref name="bus"/> via
    /// <see cref="TcpClusterMessageBus.HandleResponseDeliveryAsync"/> — simulating the answering
    /// peer writing its response back over the same physical connection the request arrived on,
    /// without needing a real socket pair. <paramref name="bus"/> is a settable holder because the
    /// bus doesn't exist yet at the point the connection (and its outbound-connection factory) must
    /// be built.
    /// </summary>
    private static ITcpClusterConnection CreateAutoRepliyingConnection(
        Func<TcpClusterRequestEnvelope, TcpClusterResponseEnvelope> buildResponse,
        Func<TcpClusterMessageBus> bus)
    {
        var connection = TcpClusterMessageBusTestHelpers.CreateConnectionSubstitute();
        connection.SendFrameAsync(Arg.Any<TcpClusterFrame>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var frame = (TcpClusterFrame)callInfo[0];
                if (frame.Kind == TcpClusterFrameKind.Request)
                {
                    var request = frame.PayloadJson.FromNJson<TcpClusterRequestEnvelope>()!;
                    var response = buildResponse(request);
                    _ = bus().HandleResponseDeliveryAsync(response.ToNJson(), CancellationToken.None);
                }
                return Task.CompletedTask;
            });
        return connection;
    }

    [Fact]
    public async Task SendRequestAsync_Success_ReturnsTheResponseEnvelope()
    {
        TcpClusterMessageBus? bus = null;
        var connection = CreateAutoRepliyingConnection(
            request => new TcpClusterResponseEnvelope(request.CorrelationId, true, null, "\"payload\""),
            () => bus!);

        bus = await TcpClusterMessageBusTestHelpers.CreateBusAsync(
            outboundConnectionFactory: (_, _) => Task.FromResult(connection));

        var response = await bus.SendRequestAsync(
            new Uri("https://peer1:5001/"), TcpClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null, CancellationToken.None);

        response.Success.Should().BeTrue();
        response.PayloadJson.Should().Be("\"payload\"");

        await bus.DisposeAsync();
    }

    [Fact]
    public async Task SendRequestAsync_RejectedResponse_ThrowsInvalidOperationException()
    {
        TcpClusterMessageBus? bus = null;
        var connection = CreateAutoRepliyingConnection(
            request => new TcpClusterResponseEnvelope(request.CorrelationId, false, "channel not found", null),
            () => bus!);

        bus = await TcpClusterMessageBusTestHelpers.CreateBusAsync(
            outboundConnectionFactory: (_, _) => Task.FromResult(connection));

        var act = async () => await bus.SendRequestAsync(
            new Uri("https://peer1:5001/"), TcpClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null, CancellationToken.None);

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("*channel not found*");

        await bus.DisposeAsync();
    }

    [Fact]
    public async Task SendRequestAsync_NoResponseEverArrives_ThrowsTimeoutException()
    {
        // A connection substitute whose SendFrameAsync never triggers a reply — the pending request
        // is left to time out against the very short RequestTimeout configured below.
        var connection = TcpClusterMessageBusTestHelpers.CreateConnectionSubstitute();

        await using var bus = await TcpClusterMessageBusTestHelpers.CreateBusAsync(
            requestTimeout: TimeSpan.FromMilliseconds(50),
            outboundConnectionFactory: (_, _) => Task.FromResult(connection));

        var act = async () => await bus.SendRequestAsync(
            new Uri("https://peer1:5001/"), TcpClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null, CancellationToken.None);

        await act.Should().ThrowAsync<TimeoutException>();
    }

    [Fact]
    public async Task HandleRequestFrameAsync_MalformedPayload_DoesNotThrow_AndSendsNoResponse()
    {
        await using var bus = await TcpClusterMessageBusTestHelpers.CreateBusAsync();

        var responsesSent = new List<TcpClusterFrame>();
        Task CaptureResponse(TcpClusterFrame frame, CancellationToken ct) { responsesSent.Add(frame); return Task.CompletedTask; }

        var act = async () => await bus.HandleRequestFrameAsync("{ not valid json ", CaptureResponse, CancellationToken.None);

        await act.Should().NotThrowAsync();
        responsesSent.Should().BeEmpty();
    }

    [Fact]
    public async Task HandleRequestFrameAsync_UnknownKind_SendsAnUnsuccessfulResponse()
    {
        await using var bus = await TcpClusterMessageBusTestHelpers.CreateBusAsync();

        var responsesSent = new List<TcpClusterFrame>();
        Task CaptureResponse(TcpClusterFrame frame, CancellationToken ct) { responsesSent.Add(frame); return Task.CompletedTask; }

        var correlationId = Guid.NewGuid();
        var request = new TcpClusterRequestEnvelope(correlationId, (TcpClusterRequestKind)999, null, Guid.NewGuid(), null);

        await bus.HandleRequestFrameAsync(request.ToNJson(), CaptureResponse, CancellationToken.None);

        responsesSent.Should().HaveCount(1);
        responsesSent[0].Kind.Should().Be(TcpClusterFrameKind.Response);

        var response = responsesSent[0].PayloadJson.FromNJson<TcpClusterResponseEnvelope>()!;
        response.CorrelationId.Should().Be(correlationId);
        response.Success.Should().BeFalse();
    }

    [Fact]
    public async Task HandleRequestFrameAsync_ResponseSendFails_DoesNotThrow()
    {
        await using var bus = await TcpClusterMessageBusTestHelpers.CreateBusAsync();

        Task ThrowingSend(TcpClusterFrame frame, CancellationToken ct) => throw new IOException("simulated write failure");

        var request = new TcpClusterRequestEnvelope(Guid.NewGuid(), TcpClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null);

        var act = async () => await bus.HandleRequestFrameAsync(request.ToNJson(), ThrowingSend, CancellationToken.None);

        await act.Should().NotThrowAsync("a failure writing the response back must not crash the receive loop");
    }

    [Fact]
    public async Task TryCompletePendingRequest_NoPendingRequestForCorrelationId_ReturnsFalse()
    {
        await using var bus = await TcpClusterMessageBusTestHelpers.CreateBusAsync();

        var response = new TcpClusterResponseEnvelope(Guid.NewGuid(), true, null, null);

        bus.TryCompletePendingRequest(response).Should().BeFalse();
    }

    [Fact]
    public async Task BuildResponseAsync_HandlerThrows_ReturnsUnsuccessfulResponseInsteadOfPropagating()
    {
        var channelResolver = NSubstitute.Substitute.For<ThunderPropagator.ClusterMessageBuses.SharedKernel.IClusterChannelResolver>();
        channelResolver.GetChannel(Arg.Any<Guid>()).Returns(_ => throw new InvalidOperationException("no such channel"));

        await using var bus = await TcpClusterMessageBusTestHelpers.CreateBusAsync(channelResolver: channelResolver);

        var request = new TcpClusterRequestEnvelope(Guid.NewGuid(), TcpClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null);

        var response = await bus.BuildResponseAsync(request, CancellationToken.None);

        response.Success.Should().BeFalse();
        response.ErrorMessage.Should().Contain("no such channel");
    }
}
