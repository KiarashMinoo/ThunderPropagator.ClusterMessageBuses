using System.Net;
using System.Text;
using FluentAssertions;
using NSubstitute;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Channels.Metadata;
using ThunderPropagator.Application.Channels.Snapshots;
using ThunderPropagator.BuildingBlocks.Application.Enums;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;
using ThunderPropagator.ClusterMessageBuses.UdpClient;

namespace ThunderPropagator.UnitTests.UdpClient;

public class UdpClusterMessageBusSnapshotsTests
{
    /// <summary>
    /// <see cref="SnapshotEntry"/> has an <c>internal</c> constructor and no public/settable
    /// properties, so tests can only construct one via the
    /// <c>InternalsVisibleTo("ThunderPropagator.UnitTests")</c> grant on the core
    /// <c>ThunderPropagator.Application</c> assembly — never via an object initializer. Defaults to
    /// <see cref="SnapshotEntryState.Active"/>, which is all every test here needs.
    /// </summary>
    private static SnapshotEntry CreateActiveEntry(int hashKey) =>
        new(hashKey, new Dictionary<string, object?>(), CastType.Broadcast, new Dictionary<string, object?>());

    private static IChannel CreateChannelSubstitute(string channelName, SnapshotEntry[] searchResults)
    {
        var metadata = Substitute.For<IChannelMetadata>();
        metadata.ChannelName.Returns(channelName);

        var channel = Substitute.For<IChannel>();
        channel.Metadata.Returns(metadata);
        channel.SearchSnapshotsAsync(Arg.Any<Func<SnapshotEntry, bool>>(), Arg.Any<int>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(searchResults));

        return channel;
    }

    [Fact]
    public async Task BuildResponseAsync_RestoreSnapshot_ReturnsTheChannelsActiveEntries()
    {
        var entries = new[] { CreateActiveEntry(1) };
        var channel = CreateChannelSubstitute("orders", entries);

        var channelResolver = Substitute.For<IClusterChannelResolver>();
        channelResolver.GetChannel("orders").Returns(channel);

        await using var bus = await UdpClusterMessageBusTestHelpers.CreateBusAsync(channelResolver: channelResolver);

        var request = new UdpClusterRequestEnvelope(Guid.NewGuid(), UdpClusterRequestKind.RestoreSnapshot, "orders", null, null);
        var response = await bus.BuildResponseAsync(request, CancellationToken.None);

        response.Success.Should().BeTrue();
        var returnedEntries = response.PayloadJson!.FromNJson<SnapshotEntry[]>();
        returnedEntries.Should().ContainSingle(e => e.HashKey == 1);
    }

    [Fact]
    public async Task BuildResponseAsync_RestoreSnapshot_UnknownChannel_ReturnsUnsuccessfulResponse()
    {
        var channelResolver = Substitute.For<IClusterChannelResolver>();
        channelResolver.GetChannel("missing").Returns(_ => throw new InvalidOperationException("Channel 'missing' is not registered."));

        await using var bus = await UdpClusterMessageBusTestHelpers.CreateBusAsync(channelResolver: channelResolver);

        var request = new UdpClusterRequestEnvelope(Guid.NewGuid(), UdpClusterRequestKind.RestoreSnapshot, "missing", null, null);
        var response = await bus.BuildResponseAsync(request, CancellationToken.None);

        response.Success.Should().BeFalse();
        response.ErrorMessage.Should().Contain("missing");
    }

    [Fact]
    public async Task BuildResponseAsync_SyncDelta_ReturnsUpdatedEntriesAsAPayload()
    {
        var entries = new[] { CreateActiveEntry(2) };
        var channel = CreateChannelSubstitute("orders", entries);

        var channelResolver = Substitute.For<IClusterChannelResolver>();
        channelResolver.GetChannel("orders").Returns(channel);

        await using var bus = await UdpClusterMessageBusTestHelpers.CreateBusAsync(channelResolver: channelResolver);

        var since = DateTimeOffset.UtcNow.AddMinutes(-5);
        var request = new UdpClusterRequestEnvelope(Guid.NewGuid(), UdpClusterRequestKind.SyncDelta, "orders", null, since.UtcTicks);
        var response = await bus.BuildResponseAsync(request, CancellationToken.None);

        response.Success.Should().BeTrue();
        var delta = response.PayloadJson!.FromNJson<UdpSnapshotDeltaPayload>();
        delta!.UpdatedEntries.Should().ContainSingle(e => e.HashKey == 2);
    }

    [Fact]
    public async Task RestoreFromLeaderAsync_SendsARestoreSnapshotRequest_ForTheChannelsName()
    {
        var channel = CreateChannelSubstitute("orders", []);

        UdpClusterMessageBus? busHolder = null;
        var socket = UdpClusterMessageBusTestHelpers.CreateSocketSubstitute();
        socket.SendDatagramAsync(Arg.Any<byte[]>(), Arg.Any<IPEndPoint>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var frame = Encoding.UTF8.GetString((byte[])callInfo[0]).FromNJson<UdpClusterFrame>()!;
                var request = frame.PayloadJson.FromNJson<UdpClusterRequestEnvelope>()!;
                var response = new UdpClusterResponseEnvelope(request.CorrelationId, true, null, Array.Empty<SnapshotEntry>().ToNJson());
                _ = busHolder!.HandleResponseDeliveryAsync(response.ToNJson(), CancellationToken.None);
                return Task.CompletedTask;
            });

        await using var bus = await UdpClusterMessageBusTestHelpers.CreateBusAsync(socket: socket);
        busHolder = bus;

        var act = async () => await bus.RestoreFromLeaderAsync(new Uri("https://leader:5001/"), channel, CancellationToken.None);

        await act.Should().NotThrowAsync();

        await socket.Received(1).SendDatagramAsync(
            Arg.Is<byte[]>(bytes => RequestFrameIs(bytes, UdpClusterRequestKind.RestoreSnapshot, "orders")),
            Arg.Any<IPEndPoint>(),
            Arg.Any<CancellationToken>());
    }

    private static bool RequestFrameIs(byte[] bytes, UdpClusterRequestKind expectedKind, string expectedChannelName)
    {
        var frame = Encoding.UTF8.GetString(bytes).FromNJson<UdpClusterFrame>();
        if (frame is not { Kind: UdpClusterFrameKind.Request })
            return false;

        var request = frame.PayloadJson.FromNJson<UdpClusterRequestEnvelope>();
        return request is not null && request.Kind == expectedKind && request.ChannelName == expectedChannelName;
    }

    [Fact]
    public async Task SyncDeltaFromLeaderAsync_SendsASyncDeltaRequest_WithTheSinceTimestamp()
    {
        var channel = CreateChannelSubstitute("orders", []);
        var since = DateTimeOffset.UtcNow.AddMinutes(-10);

        UdpClusterMessageBus? busHolder = null;
        var socket = UdpClusterMessageBusTestHelpers.CreateSocketSubstitute();
        socket.SendDatagramAsync(Arg.Any<byte[]>(), Arg.Any<IPEndPoint>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var frame = Encoding.UTF8.GetString((byte[])callInfo[0]).FromNJson<UdpClusterFrame>()!;
                var request = frame.PayloadJson.FromNJson<UdpClusterRequestEnvelope>()!;
                var deltaPayload = new UdpSnapshotDeltaPayload { UpdatedEntries = [], DeletedHashKeys = [] };
                var response = new UdpClusterResponseEnvelope(request.CorrelationId, true, null, deltaPayload.ToNJson());
                _ = busHolder!.HandleResponseDeliveryAsync(response.ToNJson(), CancellationToken.None);
                return Task.CompletedTask;
            });

        await using var bus = await UdpClusterMessageBusTestHelpers.CreateBusAsync(socket: socket);
        busHolder = bus;

        await bus.SyncDeltaFromLeaderAsync(new Uri("https://leader:5001/"), channel, since, CancellationToken.None);

        await socket.Received(1).SendDatagramAsync(
            Arg.Is<byte[]>(bytes =>
            {
                var frame = Encoding.UTF8.GetString(bytes).FromNJson<UdpClusterFrame>();
                var request = frame?.PayloadJson.FromNJson<UdpClusterRequestEnvelope>();
                return request is not null && request.Kind == UdpClusterRequestKind.SyncDelta && request.SinceTicks == since.UtcTicks;
            }),
            Arg.Any<IPEndPoint>(),
            Arg.Any<CancellationToken>());
    }
}
