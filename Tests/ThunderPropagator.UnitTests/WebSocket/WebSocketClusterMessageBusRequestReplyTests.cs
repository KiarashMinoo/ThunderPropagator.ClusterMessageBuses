using System.Net.WebSockets;
using System.Text;
using FluentAssertions;
using NSubstitute;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.WebSocket;

namespace ThunderPropagator.UnitTests.WebSocket;

public class WebSocketClusterMessageBusRequestReplyTests
{
    private static WebSocketClusterRequestEnvelope CapturePublishedRequest(System.Net.WebSockets.WebSocket socket)
    {
        var call = socket.ReceivedCalls()
            .Last(c => c.GetMethodInfo().Name == nameof(System.Net.WebSockets.WebSocket.SendAsync));
        var segment = (ArraySegment<byte>)call.GetArguments()[0]!;
        var frame = Encoding.UTF8.GetString(segment).FromNJson<WebSocketClusterFrame>()!;

        return frame.PayloadJson.FromNJson<WebSocketClusterRequestEnvelope>()!;
    }

    [Fact]
    public async Task SendRequestAsync_NoReplyArrives_ThrowsTimeoutException()
    {
        await using var bus = await WebSocketClusterMessageBusTestHelpers.CreateBusAsync(requestTimeout: TimeSpan.FromMilliseconds(50));

        var act = async () => await bus.SendRequestAsync(
            new Uri("https://peer:5001/"), WebSocketClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null, CancellationToken.None);

        await act.Should().ThrowAsync<TimeoutException>();
    }

    [Fact]
    public async Task SendRequestAsync_CallerCancels_ThrowsOperationCanceledExceptionNotTimeoutException()
    {
        await using var bus = await WebSocketClusterMessageBusTestHelpers.CreateBusAsync(requestTimeout: TimeSpan.FromSeconds(30));

        using var cts = new CancellationTokenSource();
        var task = bus.SendRequestAsync(
            new Uri("https://peer:5001/"), WebSocketClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null, cts.Token);

        cts.Cancel();

        var act = async () => await task;

        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task SendRequestAsync_MatchingReplyArrivesViaTryCompletePendingRequest_ReturnsIt()
    {
        var peerSocket = WebSocketClusterMessageBusTestHelpers.CreateSocketSubstitute();
        await using var bus = await WebSocketClusterMessageBusTestHelpers.CreateBusAsync(
            requestTimeout: TimeSpan.FromSeconds(5),
            outboundSocketFactory: WebSocketClusterMessageBusTestHelpers.OutboundSocketFactoryReturning(peerSocket));

        var task = bus.SendRequestAsync(
            new Uri("https://peer:5001/"), WebSocketClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null, CancellationToken.None);

        var sentRequest = CapturePublishedRequest(peerSocket);
        var expectedPayload = "[{\"SubscriptionId\":\"sub-9\"}]";

        var completed = bus.TryCompletePendingRequest(new WebSocketClusterResponseEnvelope(sentRequest.CorrelationId, true, null, expectedPayload));

        completed.Should().BeTrue();

        var response = await task;

        response.PayloadJson.Should().Be(expectedPayload);
    }

    [Fact]
    public async Task SendRequestAsync_PeerRejectsRequest_ThrowsInvalidOperationExceptionWithThePeersMessage()
    {
        var peerSocket = WebSocketClusterMessageBusTestHelpers.CreateSocketSubstitute();
        await using var bus = await WebSocketClusterMessageBusTestHelpers.CreateBusAsync(
            requestTimeout: TimeSpan.FromSeconds(5),
            outboundSocketFactory: WebSocketClusterMessageBusTestHelpers.OutboundSocketFactoryReturning(peerSocket));

        var task = bus.SendRequestAsync(
            new Uri("https://peer:5001/"), WebSocketClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null, CancellationToken.None);

        var sentRequest = CapturePublishedRequest(peerSocket);

        var completed = bus.TryCompletePendingRequest(new WebSocketClusterResponseEnvelope(sentRequest.CorrelationId, false, "channel not found", null));
        completed.Should().BeTrue();

        var act = async () => await task;

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("*channel not found*");
    }

    [Fact]
    public async Task TryCompletePendingRequest_NoMatchingPendingRequest_ReturnsFalse()
    {
        await using var bus = await WebSocketClusterMessageBusTestHelpers.CreateBusAsync();

        var response = new WebSocketClusterResponseEnvelope(Guid.NewGuid(), true, null, null);

        bus.TryCompletePendingRequest(response).Should().BeFalse();
    }

    [Fact]
    public async Task HandleRequestFrameAsync_MalformedPayload_IsSkippedWithoutThrowing()
    {
        await using var bus = await WebSocketClusterMessageBusTestHelpers.CreateBusAsync();

        var sendResponseCalled = false;
        Func<WebSocketClusterFrame, CancellationToken, Task> sendResponse = (_, _) => { sendResponseCalled = true; return Task.CompletedTask; };

        var act = async () => await bus.HandleRequestFrameAsync("not json", sendResponse, CancellationToken.None);

        await act.Should().NotThrowAsync();
        sendResponseCalled.Should().BeFalse();
    }

    [Fact]
    public async Task HandleRequestFrameAsync_ValidRequest_SendsAResponseBackOverTheSameConnection()
    {
        await using var bus = await WebSocketClusterMessageBusTestHelpers.CreateBusAsync();

        WebSocketClusterFrame? sentFrame = null;
        Func<WebSocketClusterFrame, CancellationToken, Task> sendResponse = (frame, _) => { sentFrame = frame; return Task.CompletedTask; };

        var request = new WebSocketClusterRequestEnvelope(Guid.NewGuid(), WebSocketClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null);

        await bus.HandleRequestFrameAsync(request.ToNJson(), sendResponse, CancellationToken.None);

        sentFrame.Should().NotBeNull();
        sentFrame!.Kind.Should().Be(WebSocketClusterFrameKind.Response);

        var response = sentFrame.PayloadJson.FromNJson<WebSocketClusterResponseEnvelope>();
        response!.CorrelationId.Should().Be(request.CorrelationId);
    }

    [Fact]
    public async Task HandleResponseDeliveryAsync_MalformedPayload_IsSkippedWithoutThrowing()
    {
        await using var bus = await WebSocketClusterMessageBusTestHelpers.CreateBusAsync();

        var act = async () => await bus.HandleResponseDeliveryAsync("not json", CancellationToken.None);

        await act.Should().NotThrowAsync();
    }
}
