using Confluent.Kafka;
using FluentAssertions;
using NSubstitute;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Channels.Metadata;
using ThunderPropagator.Application.Channels.Snapshots;
using ThunderPropagator.BuildingBlocks.Application.Enums;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.Kafka;

using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.UnitTests.Kafka;

public class KafkaClusterMessageBusSnapshotsTests
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
    public async Task BuildResponseAsync_RestoreSnapshot_ReturnsActiveEntriesFromTheResolvedChannel()
    {
        var channel = FakeChannel("Orders");
        var entries = new[] { MakeEntry(1), MakeEntry(2) };
        channel.SearchSnapshotsAsync(Arg.Any<Func<SnapshotEntry, bool>>(), 0, 0, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(entries));

        var resolver = Substitute.For<IClusterChannelResolver>();
        resolver.GetChannel("Orders").Returns(channel);

        await using var bus = KafkaClusterMessageBusTestHelpers.CreateBus(channelResolver: resolver);
        var request = new KafkaClusterRequestEnvelope(Guid.NewGuid(), KafkaClusterRequestKind.RestoreSnapshot, "Orders", null, null, new Uri("https://follower:5001/"));

        var response = await bus.BuildResponseAsync(request, CancellationToken.None);

        response.Success.Should().BeTrue();
        response.PayloadJson.Should().NotBeNull();
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

        await using var bus = KafkaClusterMessageBusTestHelpers.CreateBus(channelResolver: resolver);
        var since = DateTimeOffset.UtcNow.AddMinutes(-5);
        var request = new KafkaClusterRequestEnvelope(Guid.NewGuid(), KafkaClusterRequestKind.SyncDelta, "Orders", null, since.UtcTicks, new Uri("https://follower:5001/"));

        var response = await bus.BuildResponseAsync(request, CancellationToken.None);

        response.Success.Should().BeTrue();
        var delta = response.PayloadJson!.FromNJson<KafkaSnapshotDeltaPayload>();
        delta!.UpdatedEntries.Select(e => e.HashKey).Should().BeEquivalentTo([5]);
        delta.DeletedHashKeys.Should().BeEquivalentTo([7, 8]);
    }

    [Fact]
    public async Task BuildResponseAsync_UnknownChannelName_ReturnsAFailureResponseInsteadOfThrowing()
    {
        var resolver = Substitute.For<IClusterChannelResolver>();
        resolver.GetChannel("Missing").Returns(_ => throw new InvalidOperationException("channel Missing could not be found!"));

        await using var bus = KafkaClusterMessageBusTestHelpers.CreateBus(channelResolver: resolver);
        var request = new KafkaClusterRequestEnvelope(Guid.NewGuid(), KafkaClusterRequestKind.RestoreSnapshot, "Missing", null, null, new Uri("https://follower:5001/"));

        var response = await bus.BuildResponseAsync(request, CancellationToken.None);

        response.Success.Should().BeFalse();
        response.ErrorMessage.Should().Contain("Missing");
        response.PayloadJson.Should().BeNull();
    }

    [Fact]
    public async Task RestoreFromLeaderAsync_AppliesOnlyActiveEntriesToTheLocalChannel()
    {
        KafkaClusterRequestEnvelope? sentRequest = null;
        var producer = Substitute.For<IProducer<string, string>>();
        producer.ProduceAsync(
                Arg.Any<string>(),
                Arg.Do<Message<string, string>>(m => sentRequest = m.Value.FromNJson<KafkaClusterRequestEnvelope>()),
                Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<DeliveryResult<string, string>>(null!));

        await using var bus = KafkaClusterMessageBusTestHelpers.CreateBus(producer: producer, requestTimeout: TimeSpan.FromSeconds(5));

        var localChannel = FakeChannel("Orders");
        var restoreTask = bus.RestoreFromLeaderAsync(new Uri("https://leader:5000/"), localChannel, CancellationToken.None);

        sentRequest.Should().NotBeNull();
        var entries = new[] { MakeEntry(1) };
        bus.TryCompletePendingRequest(new KafkaClusterResponseEnvelope(sentRequest!.CorrelationId, true, null, entries.ToNJson()));

        await restoreTask;

        localChannel.Received(1).OverwriteSnapshot(Arg.Is<SnapshotEntry>(e => e.HashKey == 1));
    }
}
