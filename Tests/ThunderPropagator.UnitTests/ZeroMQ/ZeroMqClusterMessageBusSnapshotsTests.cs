using FluentAssertions;
using NSubstitute;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Channels.Metadata;
using ThunderPropagator.Application.Channels.Snapshots;
using ThunderPropagator.BuildingBlocks.Application.Enums;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;
using ThunderPropagator.ClusterMessageBuses.ZeroMQ;

namespace ThunderPropagator.UnitTests.ZeroMQ;

public class ZeroMqClusterMessageBusSnapshotsTests
{
    private static SnapshotEntry MakeEntry(int hashKey) =>
        new(hashKey, new Dictionary<string, object?> { ["Id"] = hashKey }, CastType.Multicast,
            new Dictionary<string, object?> { ["Value"] = hashKey });

    private static IChannel FakeChannel(string channelName)
    {
        var channel = Substitute.For<IChannel>();
        var metadata = Substitute.For<IChannelMetadata>();
        metadata.ChannelName.Returns(channelName);
        channel.Metadata.Returns(metadata);
        return channel;
    }

    [Fact]
    public async Task RestoreFromLeaderAsync_AppliesOnlyActiveEntriesToTheLocalChannel()
    {
        var localChannel = FakeChannel("Orders");
        var entries = new[] { MakeEntry(1) };

        ZeroMqClusterMessageBus? busHolder = null;
        var connection = ZeroMqClusterMessageBusTestHelpers.CreatePeerConnectionSubstitute();
        connection.When(c => c.SendFrame(Arg.Any<ZeroMqClusterFrame>())).Do(callInfo =>
        {
            var frame = callInfo.Arg<ZeroMqClusterFrame>();
            var request = frame.PayloadJson.FromNJson<ZeroMqClusterRequestEnvelope>()!;
            var response = new ZeroMqClusterResponseEnvelope(request.CorrelationId, true, null, entries.ToNJson());
            busHolder!.TryCompletePendingRequest(response);
        });

        await using var bus = await ZeroMqClusterMessageBusTestHelpers.CreateBusAsync(
            peerConnectionFactory: (_, _) => Task.FromResult(connection));
        busHolder = bus;

        await bus.RestoreFromLeaderAsync(new Uri("https://leader:5000/"), localChannel, CancellationToken.None);

        localChannel.Received(1).OverwriteSnapshot(Arg.Is<SnapshotEntry>(e => e.HashKey == 1));
    }

    [Fact]
    public async Task SyncDeltaFromLeaderAsync_AppliesUpdatesAndDeletions()
    {
        var localChannel = FakeChannel("Orders");
        var updated = new[] { MakeEntry(5) };

        ZeroMqClusterMessageBus? busHolder = null;
        var connection = ZeroMqClusterMessageBusTestHelpers.CreatePeerConnectionSubstitute();
        connection.When(c => c.SendFrame(Arg.Any<ZeroMqClusterFrame>())).Do(callInfo =>
        {
            var frame = callInfo.Arg<ZeroMqClusterFrame>();
            var request = frame.PayloadJson.FromNJson<ZeroMqClusterRequestEnvelope>()!;
            var delta = new ZeroMqSnapshotDeltaPayload { UpdatedEntries = updated, DeletedHashKeys = [7, 8] };
            var response = new ZeroMqClusterResponseEnvelope(request.CorrelationId, true, null, delta.ToNJson());
            busHolder!.TryCompletePendingRequest(response);
        });

        await using var bus = await ZeroMqClusterMessageBusTestHelpers.CreateBusAsync(
            peerConnectionFactory: (_, _) => Task.FromResult(connection));
        busHolder = bus;

        await bus.SyncDeltaFromLeaderAsync(new Uri("https://leader:5000/"), localChannel, DateTimeOffset.UtcNow.AddMinutes(-5), CancellationToken.None);

        localChannel.Received(1).OverwriteSnapshot(Arg.Is<SnapshotEntry>(e => e.HashKey == 5));
        localChannel.Received(1).DeleteSnapshot(7);
        localChannel.Received(1).DeleteSnapshot(8);
    }

    [Fact]
    public async Task RestoreFromLeaderAsync_UnsuccessfulResponse_ThrowsInvalidOperationException()
    {
        var localChannel = FakeChannel("Orders");

        ZeroMqClusterMessageBus? busHolder = null;
        var connection = ZeroMqClusterMessageBusTestHelpers.CreatePeerConnectionSubstitute();
        connection.When(c => c.SendFrame(Arg.Any<ZeroMqClusterFrame>())).Do(callInfo =>
        {
            var frame = callInfo.Arg<ZeroMqClusterFrame>();
            var request = frame.PayloadJson.FromNJson<ZeroMqClusterRequestEnvelope>()!;
            var response = new ZeroMqClusterResponseEnvelope(request.CorrelationId, false, "channel not found", null);
            busHolder!.TryCompletePendingRequest(response);
        });

        await using var bus = await ZeroMqClusterMessageBusTestHelpers.CreateBusAsync(
            peerConnectionFactory: (_, _) => Task.FromResult(connection));
        busHolder = bus;

        var act = async () => await bus.RestoreFromLeaderAsync(new Uri("https://leader:5000/"), localChannel, CancellationToken.None);

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("*channel not found*");
    }

    [Fact]
    public async Task RestoreFromLeaderAsync_PeerNeverReplies_ThrowsTimeoutException()
    {
        var localChannel = FakeChannel("Orders");
        var connection = ZeroMqClusterMessageBusTestHelpers.CreatePeerConnectionSubstitute();

        await using var bus = await ZeroMqClusterMessageBusTestHelpers.CreateBusAsync(
            peerConnectionFactory: (_, _) => Task.FromResult(connection),
            requestTimeout: TimeSpan.FromMilliseconds(50));

        var act = async () => await bus.RestoreFromLeaderAsync(new Uri("https://leader:5000/"), localChannel, CancellationToken.None);

        await act.Should().ThrowAsync<TimeoutException>();
    }

    [Fact]
    public async Task BuildResponseAsync_RestoreSnapshot_ReturnsActiveEntriesFromTheResolvedChannel()
    {
        var channel = FakeChannel("Orders");
        var entries = new[] { MakeEntry(1), MakeEntry(2) };
        channel.SearchSnapshotsAsync(Arg.Any<Func<SnapshotEntry, bool>>(), 0, 0, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(entries));

        var resolver = Substitute.For<IClusterChannelResolver>();
        resolver.GetChannel("Orders").Returns(channel);

        await using var bus = await ZeroMqClusterMessageBusTestHelpers.CreateBusAsync(channelResolver: resolver);
        var request = new ZeroMqClusterRequestEnvelope(Guid.NewGuid(), ZeroMqClusterRequestKind.RestoreSnapshot, "Orders", null, null);

        var response = await bus.BuildResponseAsync(request, CancellationToken.None);

        response.Success.Should().BeTrue();
        var returnedEntries = response.PayloadJson!.FromNJson<SnapshotEntry[]>();
        returnedEntries.Should().HaveCount(2);
        returnedEntries!.Select(e => e.HashKey).Should().BeEquivalentTo([1, 2]);
    }

    [Fact]
    public async Task BuildResponseAsync_SyncDelta_ReturnsUpdatedEntriesAndDeletedHashKeys()
    {
        var channel = FakeChannel("Orders");
        var updated = new[] { MakeEntry(5) };
        channel.SearchSnapshotsAsync(Arg.Any<Func<SnapshotEntry, bool>>(), 0, 0, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(updated));
        channel.GetSnapshotTombstonesSince(Arg.Any<DateTimeOffset>()).Returns([7, 8]);

        var resolver = Substitute.For<IClusterChannelResolver>();
        resolver.GetChannel("Orders").Returns(channel);

        await using var bus = await ZeroMqClusterMessageBusTestHelpers.CreateBusAsync(channelResolver: resolver);
        var since = DateTimeOffset.UtcNow.AddMinutes(-5);
        var request = new ZeroMqClusterRequestEnvelope(Guid.NewGuid(), ZeroMqClusterRequestKind.SyncDelta, "Orders", null, since.UtcTicks);

        var response = await bus.BuildResponseAsync(request, CancellationToken.None);

        response.Success.Should().BeTrue();
        var delta = response.PayloadJson!.FromNJson<ZeroMqSnapshotDeltaPayload>();
        delta!.UpdatedEntries.Select(e => e.HashKey).Should().BeEquivalentTo([5]);
        delta.DeletedHashKeys.Should().BeEquivalentTo([7, 8]);
    }

    [Fact]
    public async Task BuildResponseAsync_UnknownChannelName_ReturnsAFailureResponseInsteadOfThrowing()
    {
        var resolver = Substitute.For<IClusterChannelResolver>();
        resolver.GetChannel("Missing").Returns(_ => throw new InvalidOperationException("channel Missing could not be found!"));

        await using var bus = await ZeroMqClusterMessageBusTestHelpers.CreateBusAsync(channelResolver: resolver);
        var request = new ZeroMqClusterRequestEnvelope(Guid.NewGuid(), ZeroMqClusterRequestKind.RestoreSnapshot, "Missing", null, null);

        var response = await bus.BuildResponseAsync(request, CancellationToken.None);

        response.Success.Should().BeFalse();
        response.ErrorMessage.Should().Contain("Missing");
        response.PayloadJson.Should().BeNull();
    }
}
