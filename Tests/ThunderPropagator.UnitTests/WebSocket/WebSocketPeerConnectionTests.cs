using System.Net.WebSockets;
using System.Text;
using FluentAssertions;
using NSubstitute;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.WebSocket;

namespace ThunderPropagator.UnitTests.WebSocket;

public class WebSocketPeerConnectionTests
{
    private static System.Net.WebSockets.WebSocket CreateOpenSocketSubstitute()
    {
        var socket = Substitute.For<System.Net.WebSockets.WebSocket>();
        socket.State.Returns(WebSocketState.Open);
        return socket;
    }

    private static void WriteFrameToSegment(WebSocketClusterFrame frame, ArraySegment<byte> segment, out int length)
    {
        var bytes = Encoding.UTF8.GetBytes(frame.ToNJson());
        Array.Copy(bytes, 0, segment.Array!, segment.Offset, bytes.Length);
        length = bytes.Length;
    }

    [Fact]
    public async Task SendFrameAsync_SendsASingleTextMessageContainingTheSerializedFrame()
    {
        var socket = WebSocketClusterMessageBusTestHelpers.CreateSocketSubstitute();
        var connection = new WebSocketPeerConnection(socket, 1024);
        var frame = new WebSocketClusterFrame(WebSocketClusterFrameKind.FanOut, "{\"hello\":1}");

        await connection.SendFrameAsync(frame, CancellationToken.None);

        await socket.Received(1).SendAsync(
            Arg.Is<ArraySegment<byte>>(segment => Encoding.UTF8.GetString(segment).FromNJson<WebSocketClusterFrame>()!.PayloadJson == frame.PayloadJson),
            WebSocketMessageType.Text,
            true,
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ReceiveFramesAsync_SingleFragmentMessage_YieldsTheDeserializedFrame()
    {
        var frame = new WebSocketClusterFrame(WebSocketClusterFrameKind.FanOut, "{\"a\":1}");
        var callCount = 0;

        var socket = CreateOpenSocketSubstitute();
        socket.ReceiveAsync(Arg.Any<ArraySegment<byte>>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                callCount++;
                if (callCount == 1)
                {
                    WriteFrameToSegment(frame, (ArraySegment<byte>)callInfo[0], out var length);
                    return Task.FromResult(new WebSocketReceiveResult(length, WebSocketMessageType.Text, true));
                }

                throw new WebSocketException("connection closed");
            });

        var connection = new WebSocketPeerConnection(socket, 4096);

        var received = new List<WebSocketClusterFrame>();
        await foreach (var received_frame in connection.ReceiveFramesAsync(CancellationToken.None))
        {
            received.Add(received_frame);
        }

        received.Should().ContainSingle();
        received[0].Kind.Should().Be(WebSocketClusterFrameKind.FanOut);
        received[0].PayloadJson.Should().Be(frame.PayloadJson);
    }

    [Fact]
    public async Task ReceiveFramesAsync_MultiFragmentMessage_ReassemblesBeforeYielding()
    {
        var frame = new WebSocketClusterFrame(WebSocketClusterFrameKind.SubscriptionEvent, "{\"payload\":\"a somewhat longer value to force fragmentation in the test\"}");
        var bytes = Encoding.UTF8.GetBytes(frame.ToNJson());
        var splitAt = bytes.Length / 2;
        var callCount = 0;

        var socket = CreateOpenSocketSubstitute();
        socket.ReceiveAsync(Arg.Any<ArraySegment<byte>>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                callCount++;
                var segment = (ArraySegment<byte>)callInfo[0];

                switch (callCount)
                {
                    case 1:
                        Array.Copy(bytes, 0, segment.Array!, segment.Offset, splitAt);
                        return Task.FromResult(new WebSocketReceiveResult(splitAt, WebSocketMessageType.Text, false));
                    case 2:
                        Array.Copy(bytes, splitAt, segment.Array!, segment.Offset, bytes.Length - splitAt);
                        return Task.FromResult(new WebSocketReceiveResult(bytes.Length - splitAt, WebSocketMessageType.Text, true));
                    default:
                        throw new WebSocketException("connection closed");
                }
            });

        var connection = new WebSocketPeerConnection(socket, 4096);

        var received = new List<WebSocketClusterFrame>();
        await foreach (var received_frame in connection.ReceiveFramesAsync(CancellationToken.None))
        {
            received.Add(received_frame);
        }

        received.Should().ContainSingle();
        received[0].PayloadJson.Should().Be(frame.PayloadJson);
    }

    [Fact]
    public async Task ReceiveFramesAsync_MalformedMessage_IsSkippedWithoutThrowing()
    {
        var goodFrame = new WebSocketClusterFrame(WebSocketClusterFrameKind.FanOut, "{\"ok\":true}");
        var callCount = 0;

        var socket = CreateOpenSocketSubstitute();
        socket.ReceiveAsync(Arg.Any<ArraySegment<byte>>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                callCount++;
                var segment = (ArraySegment<byte>)callInfo[0];

                switch (callCount)
                {
                    case 1:
                        var malformed = Encoding.UTF8.GetBytes("{ not valid json ");
                        Array.Copy(malformed, 0, segment.Array!, segment.Offset, malformed.Length);
                        return Task.FromResult(new WebSocketReceiveResult(malformed.Length, WebSocketMessageType.Text, true));
                    case 2:
                        WriteFrameToSegment(goodFrame, segment, out var length);
                        return Task.FromResult(new WebSocketReceiveResult(length, WebSocketMessageType.Text, true));
                    default:
                        throw new WebSocketException("connection closed");
                }
            });

        var connection = new WebSocketPeerConnection(socket, 4096);

        var received = new List<WebSocketClusterFrame>();
        var act = async () =>
        {
            await foreach (var received_frame in connection.ReceiveFramesAsync(CancellationToken.None))
            {
                received.Add(received_frame);
            }
        };

        await act.Should().NotThrowAsync("a single malformed message must not crash the connection's receive loop");
        received.Should().ContainSingle();
        received[0].PayloadJson.Should().Be(goodFrame.PayloadJson);
    }

    [Fact]
    public async Task ReceiveFramesAsync_CloseMessage_EndsTheLoop()
    {
        var socket = CreateOpenSocketSubstitute();
        socket.ReceiveAsync(Arg.Any<ArraySegment<byte>>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new WebSocketReceiveResult(0, WebSocketMessageType.Close, true)));

        var connection = new WebSocketPeerConnection(socket, 1024);

        var received = new List<WebSocketClusterFrame>();
        await foreach (var received_frame in connection.ReceiveFramesAsync(CancellationToken.None))
        {
            received.Add(received_frame);
        }

        received.Should().BeEmpty();
    }

    [Fact]
    public async Task DisposeAsync_OpenSocket_ClosesItGracefully()
    {
        var socket = WebSocketClusterMessageBusTestHelpers.CreateSocketSubstitute();
        socket.CloseAsync(Arg.Any<WebSocketCloseStatus>(), Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);

        var connection = new WebSocketPeerConnection(socket, 1024);

        await connection.DisposeAsync();

        await socket.Received(1).CloseAsync(WebSocketCloseStatus.NormalClosure, Arg.Any<string>(), Arg.Any<CancellationToken>());
    }
}
