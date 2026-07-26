using FluentAssertions;
using NSubstitute;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Channels.Snapshots;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.RedisPubSub;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.UnitTests.RedisPubSub;

public class RedisPubSubClusterMessageBusSnapshotsTests
{
    [Fact]
    public async Task BuildResponseAsync_RestoreSnapshot_ReturnsTheResolvedChannelsActiveEntries()
    {
        var channel = Substitute.For<IChannel>();
        channel.SearchSnapshotsAsync(Arg.Any<Func<SnapshotEntry, bool>>(), 0, 0, Arg.Any<CancellationToken>())
            .Returns([]);

        var resolver = Substitute.For<IClusterChannelResolver>();
        resolver.GetChannel("my-channel").Returns(channel);

        await using var bus = await RedisPubSubClusterMessageBusTestHelpers.CreateBusAsync(channelResolver: resolver);
        var request = new RedisPubSubClusterRequestEnvelope(Guid.NewGuid(), RedisPubSubClusterRequestKind.RestoreSnapshot, "my-channel", null, null, new Uri("https://requester:5001/"));

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

        await using var bus = await RedisPubSubClusterMessageBusTestHelpers.CreateBusAsync(channelResolver: resolver);
        var since = DateTimeOffset.UtcNow.AddMinutes(-5);
        var request = new RedisPubSubClusterRequestEnvelope(Guid.NewGuid(), RedisPubSubClusterRequestKind.SyncDelta, "my-channel", null, since.UtcTicks, new Uri("https://requester:5001/"));

        var response = await bus.BuildResponseAsync(request, CancellationToken.None);

        response.Success.Should().BeTrue();
        var payload = response.PayloadJson!.FromNJson<RedisPubSubSnapshotDeltaPayload>();
        payload.Should().NotBeNull();
    }

    [Fact]
    public async Task BuildResponseAsync_UnknownChannelName_ReturnsAFailureResponseInsteadOfThrowing()
    {
        var resolver = Substitute.For<IClusterChannelResolver>();
        resolver.GetChannel("missing-channel").Returns(_ => throw new KeyNotFoundException("no such channel"));

        await using var bus = await RedisPubSubClusterMessageBusTestHelpers.CreateBusAsync(channelResolver: resolver);
        var request = new RedisPubSubClusterRequestEnvelope(Guid.NewGuid(), RedisPubSubClusterRequestKind.RestoreSnapshot, "missing-channel", null, null, new Uri("https://requester:5001/"));

        var response = await bus.BuildResponseAsync(request, CancellationToken.None);

        response.Success.Should().BeFalse();
        response.PayloadJson.Should().BeNull();
    }
}
