using FluentAssertions;
using Grpc.Core;
using NSubstitute;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Channels.Metadata;
using ThunderPropagator.Application.Channels.Snapshots;
using ThunderPropagator.BuildingBlocks.Application.Enums;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.Grpc;
using ThunderPropagator.ClusterMessageBuses.Grpc.Protos;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.UnitTests.Grpc;

public class GrpcClusterMessageBusSnapshotsTests
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

        var snapshotClient = Substitute.For<ClusterSnapshot.ClusterSnapshotClient>();
        snapshotClient.RestoreSnapshotAsync(Arg.Any<RestoreSnapshotRequest>(), null, Arg.Any<DateTime?>(), Arg.Any<CancellationToken>())
            .Returns(_ => GrpcClusterMessageBusTestHelpers.CreateUnaryCall(new RestoreSnapshotResponse { Success = true, EntriesJson = entries.ToNJson() }));

        var clients = new GrpcUnaryClients(snapshotClient, Substitute.For<ClusterSubscriptionFetch.ClusterSubscriptionFetchClient>(), null);

        await using var bus = await GrpcClusterMessageBusTestHelpers.CreateBusAsync(unaryClientsFactory: (_, _) => Task.FromResult(clients));

        await bus.RestoreFromLeaderAsync(new Uri("https://leader:5000/"), localChannel, CancellationToken.None);

        localChannel.Received(1).OverwriteSnapshot(Arg.Is<SnapshotEntry>(e => e.HashKey == 1));
    }

    [Fact]
    public async Task SyncDeltaFromLeaderAsync_AppliesUpdatesAndDeletions()
    {
        var localChannel = FakeChannel("Orders");
        var updated = new[] { MakeEntry(5) };

        var snapshotClient = Substitute.For<ClusterSnapshot.ClusterSnapshotClient>();
        snapshotClient.SyncDeltaAsync(Arg.Any<SyncDeltaRequest>(), null, Arg.Any<DateTime?>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                var response = new SyncDeltaResponse { Success = true, UpdatedEntriesJson = updated.ToNJson() };
                response.DeletedHashKeys.Add(7);
                response.DeletedHashKeys.Add(8);
                return GrpcClusterMessageBusTestHelpers.CreateUnaryCall(response);
            });

        var clients = new GrpcUnaryClients(snapshotClient, Substitute.For<ClusterSubscriptionFetch.ClusterSubscriptionFetchClient>(), null);

        await using var bus = await GrpcClusterMessageBusTestHelpers.CreateBusAsync(unaryClientsFactory: (_, _) => Task.FromResult(clients));

        await bus.SyncDeltaFromLeaderAsync(new Uri("https://leader:5000/"), localChannel, DateTimeOffset.UtcNow.AddMinutes(-5), CancellationToken.None);

        localChannel.Received(1).OverwriteSnapshot(Arg.Is<SnapshotEntry>(e => e.HashKey == 5));
        localChannel.Received(1).DeleteSnapshot(7);
        localChannel.Received(1).DeleteSnapshot(8);
    }

    [Fact]
    public async Task RestoreFromLeaderAsync_UnsuccessfulResponse_ThrowsInvalidOperationException()
    {
        var localChannel = FakeChannel("Orders");

        var snapshotClient = Substitute.For<ClusterSnapshot.ClusterSnapshotClient>();
        snapshotClient.RestoreSnapshotAsync(Arg.Any<RestoreSnapshotRequest>(), null, Arg.Any<DateTime?>(), Arg.Any<CancellationToken>())
            .Returns(_ => GrpcClusterMessageBusTestHelpers.CreateUnaryCall(new RestoreSnapshotResponse { Success = false, ErrorMessage = "channel not found" }));

        var clients = new GrpcUnaryClients(snapshotClient, Substitute.For<ClusterSubscriptionFetch.ClusterSubscriptionFetchClient>(), null);

        await using var bus = await GrpcClusterMessageBusTestHelpers.CreateBusAsync(unaryClientsFactory: (_, _) => Task.FromResult(clients));

        var act = async () => await bus.RestoreFromLeaderAsync(new Uri("https://leader:5000/"), localChannel, CancellationToken.None);

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("*channel not found*");
    }

    [Fact]
    public async Task RestoreFromLeaderAsync_DeadlineExceeded_ThrowsTimeoutException()
    {
        var localChannel = FakeChannel("Orders");

        var snapshotClient = Substitute.For<ClusterSnapshot.ClusterSnapshotClient>();
        snapshotClient.RestoreSnapshotAsync(Arg.Any<RestoreSnapshotRequest>(), null, Arg.Any<DateTime?>(), Arg.Any<CancellationToken>())
            .Returns(_ => GrpcClusterMessageBusTestHelpers.CreateFailingUnaryCall<RestoreSnapshotResponse>(
                new RpcException(new Status(StatusCode.DeadlineExceeded, "timed out"))));

        var clients = new GrpcUnaryClients(snapshotClient, Substitute.For<ClusterSubscriptionFetch.ClusterSubscriptionFetchClient>(), null);

        await using var bus = await GrpcClusterMessageBusTestHelpers.CreateBusAsync(unaryClientsFactory: (_, _) => Task.FromResult(clients));

        var act = async () => await bus.RestoreFromLeaderAsync(new Uri("https://leader:5000/"), localChannel, CancellationToken.None);

        await act.Should().ThrowAsync<TimeoutException>();
    }

    [Fact]
    public async Task BuildRestoreSnapshotResponseAsync_ReturnsActiveEntriesFromTheResolvedChannel()
    {
        var channel = FakeChannel("Orders");
        var entries = new[] { MakeEntry(1), MakeEntry(2) };
        channel.SearchSnapshotsAsync(Arg.Any<Func<SnapshotEntry, bool>>(), 0, 0, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(entries));

        var resolver = Substitute.For<IClusterChannelResolver>();
        resolver.GetChannel("Orders").Returns(channel);

        await using var bus = await GrpcClusterMessageBusTestHelpers.CreateBusAsync(channelResolver: resolver);

        var response = await bus.BuildRestoreSnapshotResponseAsync(new RestoreSnapshotRequest { ChannelName = "Orders" }, CancellationToken.None);

        response.Success.Should().BeTrue();
        var returnedEntries = response.EntriesJson.FromNJson<SnapshotEntry[]>();
        returnedEntries.Should().HaveCount(2);
        returnedEntries!.Select(e => e.HashKey).Should().BeEquivalentTo([1, 2]);
    }

    [Fact]
    public async Task BuildRestoreSnapshotResponseAsync_UnknownChannelName_ReturnsAFailureResponseInsteadOfThrowing()
    {
        var resolver = Substitute.For<IClusterChannelResolver>();
        resolver.GetChannel("Missing").Returns(_ => throw new InvalidOperationException("channel Missing could not be found!"));

        await using var bus = await GrpcClusterMessageBusTestHelpers.CreateBusAsync(channelResolver: resolver);

        var response = await bus.BuildRestoreSnapshotResponseAsync(new RestoreSnapshotRequest { ChannelName = "Missing" }, CancellationToken.None);

        response.Success.Should().BeFalse();
        response.ErrorMessage.Should().Contain("Missing");
    }

    [Fact]
    public async Task BuildSyncDeltaResponseAsync_ReturnsUpdatedEntriesAndDeletedHashKeys()
    {
        var channel = FakeChannel("Orders");
        var updated = new[] { MakeEntry(5) };
        channel.SearchSnapshotsAsync(Arg.Any<Func<SnapshotEntry, bool>>(), 0, 0, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(updated));
        channel.GetSnapshotTombstonesSince(Arg.Any<DateTimeOffset>()).Returns([7, 8]);

        var resolver = Substitute.For<IClusterChannelResolver>();
        resolver.GetChannel("Orders").Returns(channel);

        await using var bus = await GrpcClusterMessageBusTestHelpers.CreateBusAsync(channelResolver: resolver);

        var since = DateTimeOffset.UtcNow.AddMinutes(-5);
        var response = await bus.BuildSyncDeltaResponseAsync(new SyncDeltaRequest { ChannelName = "Orders", SinceTicks = since.UtcTicks }, CancellationToken.None);

        response.Success.Should().BeTrue();
        response.UpdatedEntriesJson.FromNJson<SnapshotEntry[]>()!.Select(e => e.HashKey).Should().BeEquivalentTo([5]);
        response.DeletedHashKeys.Should().BeEquivalentTo([7, 8]);
    }
}
