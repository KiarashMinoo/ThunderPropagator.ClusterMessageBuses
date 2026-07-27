using System.Net;
using System.Text;
using System.Threading;
using FluentAssertions;
using NSubstitute;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.UdpClient;

namespace ThunderPropagator.UnitTests.UdpClient;

public class UdpClusterMessageBusRequestReplyTests
{
    /// <summary>
    /// Builds a socket substitute whose <c>SendDatagramAsync</c>, on every call carrying a
    /// <see cref="UdpClusterFrameKind.Request"/> frame, invokes <paramref name="maybeRespond"/> with
    /// the decoded request and the 1-based attempt number for that correlation id; if it returns a
    /// non-null response, the socket synchronously turns around and delivers it back into
    /// <paramref name="bus"/> via <see cref="UdpClusterMessageBus.HandleResponseDeliveryAsync"/> —
    /// simulating a peer's reply datagram arriving, without a real socket. Returning <see langword="null"/>
    /// simulates that attempt's datagram being silently dropped, letting tests exercise the resend loop.
    /// </summary>
    private static IUdpClusterSocket CreateSocketWithScriptedReplies(
        Func<UdpClusterRequestEnvelope, int, UdpClusterResponseEnvelope?> maybeRespond,
        Func<UdpClusterMessageBus> bus)
    {
        var attemptsByCorrelationId = new Dictionary<Guid, int>();
        var socket = UdpClusterMessageBusTestHelpers.CreateSocketSubstitute();

        socket.SendDatagramAsync(Arg.Any<byte[]>(), Arg.Any<IPEndPoint>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var bytes = (byte[])callInfo[0];
                var frame = Encoding.UTF8.GetString(bytes).FromNJson<UdpClusterFrame>();
                if (frame is { Kind: UdpClusterFrameKind.Request })
                {
                    var request = frame.PayloadJson.FromNJson<UdpClusterRequestEnvelope>()!;
                    var attempt = attemptsByCorrelationId[request.CorrelationId] =
                        attemptsByCorrelationId.GetValueOrDefault(request.CorrelationId) + 1;

                    var response = maybeRespond(request, attempt);
                    if (response is not null)
                        _ = bus().HandleResponseDeliveryAsync(response.ToNJson(), CancellationToken.None);
                }
                return Task.CompletedTask;
            });

        return socket;
    }

    [Fact]
    public async Task SendRequestAsync_Success_ReturnsTheResponseEnvelope()
    {
        UdpClusterMessageBus? busHolder = null;
        var socket = CreateSocketWithScriptedReplies(
            (request, _) => new UdpClusterResponseEnvelope(request.CorrelationId, true, null, "\"payload\""),
            () => busHolder!);

        await using var bus = await UdpClusterMessageBusTestHelpers.CreateBusAsync(socket: socket);
        busHolder = bus;

        var response = await bus.SendRequestAsync(
            new Uri("https://peer1:5001/"), UdpClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null, CancellationToken.None);

        response.Success.Should().BeTrue();
        response.PayloadJson.Should().Be("\"payload\"");
    }

    [Fact]
    public async Task SendRequestAsync_RejectedResponse_ThrowsInvalidOperationException()
    {
        UdpClusterMessageBus? busHolder = null;
        var socket = CreateSocketWithScriptedReplies(
            (request, _) => new UdpClusterResponseEnvelope(request.CorrelationId, false, "channel not found", null),
            () => busHolder!);

        await using var bus = await UdpClusterMessageBusTestHelpers.CreateBusAsync(socket: socket);
        busHolder = bus;

        var act = async () => await bus.SendRequestAsync(
            new Uri("https://peer1:5001/"), UdpClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null, CancellationToken.None);

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("*channel not found*");
    }

    [Fact]
    public async Task SendRequestAsync_FirstDatagramIsSilentlyDropped_ResendsAndEventuallySucceeds()
    {
        // Simulates plain UDP's lack of delivery guarantee: the first send never gets a reply, only
        // the second (resent) one does. This is the behavior no other transport in this repo needs,
        // since every other transport's underlying protocol guarantees delivery of what it did send.
        UdpClusterMessageBus? busHolder = null;
        var socket = CreateSocketWithScriptedReplies(
            (request, attempt) => attempt < 2 ? null : new UdpClusterResponseEnvelope(request.CorrelationId, true, null, "\"payload\""),
            () => busHolder!);

        await using var bus = await UdpClusterMessageBusTestHelpers.CreateBusAsync(
            socket: socket,
            resendInterval: TimeSpan.FromMilliseconds(30),
            requestTimeout: TimeSpan.FromSeconds(5));
        busHolder = bus;

        var response = await bus.SendRequestAsync(
            new Uri("https://peer1:5001/"), UdpClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null, CancellationToken.None);

        response.Success.Should().BeTrue();

        await socket.Received(2).SendDatagramAsync(Arg.Any<byte[]>(), Arg.Any<IPEndPoint>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task SendRequestAsync_NoResponseEverArrives_ThrowsTimeoutException_AfterResending()
    {
        var sendCount = 0;
        var socket = UdpClusterMessageBusTestHelpers.CreateSocketSubstitute();
        socket.SendDatagramAsync(Arg.Any<byte[]>(), Arg.Any<IPEndPoint>(), Arg.Any<CancellationToken>())
            .Returns(_ => { Interlocked.Increment(ref sendCount); return Task.CompletedTask; });

        await using var bus = await UdpClusterMessageBusTestHelpers.CreateBusAsync(
            socket: socket,
            resendInterval: TimeSpan.FromMilliseconds(20),
            requestTimeout: TimeSpan.FromMilliseconds(80));

        var act = async () => await bus.SendRequestAsync(
            new Uri("https://peer1:5001/"), UdpClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null, CancellationToken.None);

        await act.Should().ThrowAsync<TimeoutException>();

        // With an 80ms overall timeout and a 20ms resend interval, the request datagram should have
        // gone out more than once before giving up.
        sendCount.Should().BeGreaterThan(1);
    }

    [Fact]
    public async Task HandleRequestFrameAsync_MalformedPayload_DoesNotThrow_AndSendsNoResponse()
    {
        await using var bus = await UdpClusterMessageBusTestHelpers.CreateBusAsync();

        var responsesSent = new List<(UdpClusterFrame Frame, IPEndPoint Endpoint)>();
        Task CaptureResponse(UdpClusterFrame frame, IPEndPoint endpoint, CancellationToken ct) { responsesSent.Add((frame, endpoint)); return Task.CompletedTask; }

        var remoteEndpoint = new IPEndPoint(IPAddress.Loopback, 6300);
        var act = async () => await bus.HandleRequestFrameAsync("{ not valid json ", remoteEndpoint, CaptureResponse, CancellationToken.None);

        await act.Should().NotThrowAsync();
        responsesSent.Should().BeEmpty();
    }

    [Fact]
    public async Task HandleRequestFrameAsync_UnknownKind_SendsAnUnsuccessfulResponse_ToTheOriginatingEndpoint()
    {
        await using var bus = await UdpClusterMessageBusTestHelpers.CreateBusAsync();

        var responsesSent = new List<(UdpClusterFrame Frame, IPEndPoint Endpoint)>();
        Task CaptureResponse(UdpClusterFrame frame, IPEndPoint endpoint, CancellationToken ct) { responsesSent.Add((frame, endpoint)); return Task.CompletedTask; }

        var correlationId = Guid.NewGuid();
        var request = new UdpClusterRequestEnvelope(correlationId, (UdpClusterRequestKind)999, null, Guid.NewGuid(), null);
        var remoteEndpoint = new IPEndPoint(IPAddress.Loopback, 6300);

        await bus.HandleRequestFrameAsync(request.ToNJson(), remoteEndpoint, CaptureResponse, CancellationToken.None);

        responsesSent.Should().HaveCount(1);
        responsesSent[0].Endpoint.Should().Be(remoteEndpoint);
        responsesSent[0].Frame.Kind.Should().Be(UdpClusterFrameKind.Response);

        var response = responsesSent[0].Frame.PayloadJson.FromNJson<UdpClusterResponseEnvelope>()!;
        response.CorrelationId.Should().Be(correlationId);
        response.Success.Should().BeFalse();
    }

    [Fact]
    public async Task HandleRequestFrameAsync_ResponseSendFails_DoesNotThrow()
    {
        await using var bus = await UdpClusterMessageBusTestHelpers.CreateBusAsync();

        Task ThrowingSend(UdpClusterFrame frame, IPEndPoint endpoint, CancellationToken ct) => throw new InvalidOperationException("simulated send failure");

        var request = new UdpClusterRequestEnvelope(Guid.NewGuid(), UdpClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null);
        var remoteEndpoint = new IPEndPoint(IPAddress.Loopback, 6300);

        var act = async () => await bus.HandleRequestFrameAsync(request.ToNJson(), remoteEndpoint, ThrowingSend, CancellationToken.None);

        await act.Should().NotThrowAsync("a failure sending the response back must not crash the receive loop");
    }

    [Fact]
    public async Task TryCompletePendingRequest_NoPendingRequestForCorrelationId_ReturnsFalse()
    {
        await using var bus = await UdpClusterMessageBusTestHelpers.CreateBusAsync();

        var response = new UdpClusterResponseEnvelope(Guid.NewGuid(), true, null, null);

        bus.TryCompletePendingRequest(response).Should().BeFalse();
    }

    [Fact]
    public async Task BuildResponseAsync_HandlerThrows_ReturnsUnsuccessfulResponseInsteadOfPropagating()
    {
        var channelResolver = Substitute.For<ThunderPropagator.ClusterMessageBuses.SharedKernel.IClusterChannelResolver>();
        channelResolver.GetChannel(Arg.Any<Guid>()).Returns(_ => throw new InvalidOperationException("no such channel"));

        await using var bus = await UdpClusterMessageBusTestHelpers.CreateBusAsync(channelResolver: channelResolver);

        var request = new UdpClusterRequestEnvelope(Guid.NewGuid(), UdpClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null);

        var response = await bus.BuildResponseAsync(request, CancellationToken.None);

        response.Success.Should().BeFalse();
        response.ErrorMessage.Should().Contain("no such channel");
    }
}
