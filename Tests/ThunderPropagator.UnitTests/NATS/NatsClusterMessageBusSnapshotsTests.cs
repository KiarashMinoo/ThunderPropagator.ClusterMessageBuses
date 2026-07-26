using FluentAssertions;
using NSubstitute;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Channels.Snapshots;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.NATS;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.UnitTests.NATS;

public class NatsClusterMessageBusSnapshotsTests
{
    [Fact]
    public async Task BuildResponseAsync_RestoreSnapshot_ReturnsTheResolvedChannelsActiveEntries()
    {
        var channel = Substitute.For<IChannel>();
        channel.SearchSnapshotsAsync(Arg.Any<Func<SnapshotEntry, bool>>(), 0, 0, Arg.Any<CancellationToken>())
            .Returns([]);

        var resolver = Substitute.For<IClusterChannelResolver>();
        resolver.GetChannel("my-channel").Returns(channel);

        await using var bus = await NatsClusterMessageBusTestHelpers.CreateBusAsync(channelResolver: resolver);
        var request = new NatsClusterRequestEnvelope(NatsClusterRequestKind.RestoreSnapshot, "my-channel", null, null);

        var response = await bus.BuildResponseAsync(request, CancellationToken.None);

        response.Success.Should().BeTrue();
        response.PayloadJson.Should().NotBeNull();
        await channel.Received(1).SearchSnapshotsAsync(Arg.Any<Func<SnapshotEntry, bool>>(), 0, 0, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task BuildResponseAsync_SyncDelta_ReturnsUpdatedEntriesAndDeletedHashKeys()
    {
        var channel = Substitute.For<IChannel>();
        channel.SearchSnapshotsAsync(Arg.Any<Func<SnapshotEntry, bool>>(), 0, 0, Arg.Any<CancellationToken>())
            .Returns([]);

        var resolver = Substitute.For<IClusterChannelResolver>();
        resolver.GetChannel("my-channel").Returns(channel);

        await using var bus = await NatsClusterMessageBusTestHelpers.CreateBusAsync(channelResolver: resolver);
        var since = DateTimeOffset.UtcNow.AddMinutes(-5);
        var request = new NatsClusterRequestEnvelope(NatsClusterRequestKind.SyncDelta, "my-channel", null, since.UtcTicks);

        var response = await bus.BuildResponseAsync(request, CancellationToken.None);

        response.Success.Should().BeTrue();
        var payload = response.PayloadJson!.FromNJson<NatsSnapshotDeltaPayload>();
        payload.Should().NotBeNull();
    }

    [Fact]
    public async Task BuildResponseAsync_UnknownChannelName_ReturnsAFailureResponseInsteadOfThrowing()
    {
        var resolver = Substitute.For<IClusterChannelResolver>();
        resolver.GetChannel("missing-channel").Returns(_ => throw new KeyNotFoundException("no such channel"));

        await using var bus = await NatsClusterMessageBusTestHelpers.CreateBusAsync(channelResolver: resolver);
        var request = new NatsClusterRequestEnvelope(NatsClusterRequestKind.RestoreSnapshot, "missing-channel", null, null);

        var response = await bus.BuildResponseAsync(request, CancellationToken.None);

        response.Success.Should().BeFalse();
        response.PayloadJson.Should().BeNull();
    }
}
