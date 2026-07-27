using FluentAssertions;
using NSubstitute;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Channels.Metadata;
using ThunderPropagator.Application.Channels.Snapshots;
using ThunderPropagator.BuildingBlocks.Application.Enums;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;
using ThunderPropagator.ClusterMessageBuses.TcpSocket;

namespace ThunderPropagator.UnitTests.TcpSocket;

public class TcpClusterMessageBusSnapshotsTests
{
    /// <summary>
    /// <see cref="SnapshotEntry"/> has an <c>internal</c> constructor and no public/settable
    /// properties (its state is populated exclusively by <see cref="IChannel"/>'s own snapshot
    /// storage), so tests can only construct one via the <c>InternalsVisibleTo("ThunderPropagator.UnitTests")</c>
    /// grant on the core <c>ThunderPropagator.Application</c> assembly — never via an object
    /// initializer (<c>HashKey</c> has no setter at all; <c>State</c>'s setter is <c>private</c>,
    /// which <c>InternalsVisibleTo</c> does not reach). The entry defaults to
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

        await using var bus = await TcpClusterMessageBusTestHelpers.CreateBusAsync(channelResolver: channelResolver);

        var request = new TcpClusterRequestEnvelope(Guid.NewGuid(), TcpClusterRequestKind.RestoreSnapshot, "orders", null, null);
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

        await using var bus = await TcpClusterMessageBusTestHelpers.CreateBusAsync(channelResolver: channelResolver);

        var request = new TcpClusterRequestEnvelope(Guid.NewGuid(), TcpClusterRequestKind.RestoreSnapshot, "missing", null, null);
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

        await using var bus = await TcpClusterMessageBusTestHelpers.CreateBusAsync(channelResolver: channelResolver);

        var since = DateTimeOffset.UtcNow.AddMinutes(-5);
        var request = new TcpClusterRequestEnvelope(Guid.NewGuid(), TcpClusterRequestKind.SyncDelta, "orders", null, since.UtcTicks);
        var response = await bus.BuildResponseAsync(request, CancellationToken.None);

        response.Success.Should().BeTrue();
        var delta = response.PayloadJson!.FromNJson<TcpSnapshotDeltaPayload>();
        delta!.UpdatedEntries.Should().ContainSingle(e => e.HashKey == 2);
    }

    [Fact]
    public async Task RestoreFromLeaderAsync_SendsARestoreSnapshotRequest_ForTheChannelsName()
    {
        var channel = CreateChannelSubstitute("orders", []);

        // The response must actually be delivered back into the bus; since we don't have the bus
        // instance yet when wiring the connection, capture it via a mutable holder.
        TcpClusterMessageBus? busHolder = null;
        var connection = TcpClusterMessageBusTestHelpers.CreateConnectionSubstitute();
        connection.SendFrameAsync(Arg.Any<TcpClusterFrame>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var frame = (TcpClusterFrame)callInfo[0];
                var request = frame.PayloadJson.FromNJson<TcpClusterRequestEnvelope>()!;
                var response = new TcpClusterResponseEnvelope(request.CorrelationId, true, null, Array.Empty<SnapshotEntry>().ToNJson());
                _ = busHolder!.HandleResponseDeliveryAsync(response.ToNJson(), CancellationToken.None);
                return Task.CompletedTask;
            });

        await using var bus = await TcpClusterMessageBusTestHelpers.CreateBusAsync(
            outboundConnectionFactory: (_, _) => Task.FromResult(connection));
        busHolder = bus;

        var act = async () => await bus.RestoreFromLeaderAsync(new Uri("https://leader:5001/"), channel, CancellationToken.None);

        await act.Should().NotThrowAsync();

        await connection.Received(1).SendFrameAsync(
            Arg.Is<TcpClusterFrame>(f => f.Kind == TcpClusterFrameKind.Request &&
                f.PayloadJson.FromNJson<TcpClusterRequestEnvelope>()!.Kind == TcpClusterRequestKind.RestoreSnapshot &&
                f.PayloadJson.FromNJson<TcpClusterRequestEnvelope>()!.ChannelName == "orders"),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task SyncDeltaFromLeaderAsync_SendsASyncDeltaRequest_WithTheSinceTimestamp()
    {
        var channel = CreateChannelSubstitute("orders", []);
        var since = DateTimeOffset.UtcNow.AddMinutes(-10);

        TcpClusterMessageBus? busHolder = null;
        var connection = TcpClusterMessageBusTestHelpers.CreateConnectionSubstitute();
        connection.SendFrameAsync(Arg.Any<TcpClusterFrame>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var frame = (TcpClusterFrame)callInfo[0];
                var request = frame.PayloadJson.FromNJson<TcpClusterRequestEnvelope>()!;
                var deltaPayload = new TcpSnapshotDeltaPayload { UpdatedEntries = [], DeletedHashKeys = [] };
                var response = new TcpClusterResponseEnvelope(request.CorrelationId, true, null, deltaPayload.ToNJson());
                _ = busHolder!.HandleResponseDeliveryAsync(response.ToNJson(), CancellationToken.None);
                return Task.CompletedTask;
            });

        await using var bus = await TcpClusterMessageBusTestHelpers.CreateBusAsync(
            outboundConnectionFactory: (_, _) => Task.FromResult(connection));
        busHolder = bus;

        await bus.SyncDeltaFromLeaderAsync(new Uri("https://leader:5001/"), channel, since, CancellationToken.None);

        await connection.Received(1).SendFrameAsync(
            Arg.Is<TcpClusterFrame>(f => f.Kind == TcpClusterFrameKind.Request &&
                f.PayloadJson.FromNJson<TcpClusterRequestEnvelope>()!.Kind == TcpClusterRequestKind.SyncDelta &&
                f.PayloadJson.FromNJson<TcpClusterRequestEnvelope>()!.SinceTicks == since.UtcTicks),
            Arg.Any<CancellationToken>());
    }
}
