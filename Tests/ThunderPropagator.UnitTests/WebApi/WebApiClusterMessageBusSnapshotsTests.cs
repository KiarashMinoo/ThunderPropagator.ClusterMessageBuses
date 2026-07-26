using System.Net;
using System.Net.Http;
using FluentAssertions;
using NSubstitute;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Channels.Snapshots;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;
using ThunderPropagator.ClusterMessageBuses.WebApi;

namespace ThunderPropagator.UnitTests.WebApi;

public class WebApiClusterMessageBusSnapshotsTests
{
    [Fact]
    public async Task BuildSnapshotResponseBodyAsync_ReturnsTheResolvedChannelsActiveEntries()
    {
        var channel = Substitute.For<IChannel>();
        channel.SearchSnapshotsAsync(Arg.Any<Func<SnapshotEntry, bool>>(), 0, 0, Arg.Any<CancellationToken>()).Returns([]);

        var resolver = Substitute.For<IClusterChannelResolver>();
        resolver.GetChannel("my-channel").Returns(channel);

        await using var bus = await WebApiClusterMessageBusTestHelpers.CreateBusAsync(channelResolver: resolver);

        var body = await bus.BuildSnapshotResponseBodyAsync("my-channel", CancellationToken.None);

        body.Should().NotBeNullOrEmpty();
        await channel.Received(1).SearchSnapshotsAsync(Arg.Any<Func<SnapshotEntry, bool>>(), 0, 0, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task BuildSnapshotDeltaResponseBodyAsync_ReturnsUpdatedEntriesAndDeletedHashKeys()
    {
        var channel = Substitute.For<IChannel>();
        channel.SearchSnapshotsAsync(Arg.Any<Func<SnapshotEntry, bool>>(), 0, 0, Arg.Any<CancellationToken>()).Returns([]);

        var resolver = Substitute.For<IClusterChannelResolver>();
        resolver.GetChannel("my-channel").Returns(channel);

        await using var bus = await WebApiClusterMessageBusTestHelpers.CreateBusAsync(channelResolver: resolver);
        var since = DateTimeOffset.UtcNow.AddMinutes(-5);

        var body = await bus.BuildSnapshotDeltaResponseBodyAsync("my-channel", since.UtcTicks, CancellationToken.None);

        var payload = body.FromNJson<WebApiSnapshotDeltaPayload>();
        payload.Should().NotBeNull();
    }

    [Fact]
    public async Task RestoreFromLeaderAsync_AppliesEveryActiveEntryFromTheResponse()
    {
        var entries = new[]
        {
            new SnapshotEntry { HashKey = 1, State = SnapshotEntryState.Active },
        };

        var httpClient = WebApiClusterMessageBusTestHelpers.CreateHttpClientSubstitute(HttpStatusCode.OK, entries.ToNJson());
        await using var bus = await WebApiClusterMessageBusTestHelpers.CreateBusAsync(httpClient: httpClient);

        var channel = Substitute.For<IChannel>();
        channel.Metadata.ChannelName.Returns("my-channel");

        await bus.RestoreFromLeaderAsync(new Uri("https://leader:5001/"), channel);

        await httpClient.Received(1).SendAsync(
            Arg.Is<HttpRequestMessage>(r => r.Method == HttpMethod.Get),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task RestoreFromLeaderAsync_NonSuccessStatus_Throws()
    {
        var httpClient = WebApiClusterMessageBusTestHelpers.CreateHttpClientSubstitute(HttpStatusCode.InternalServerError, "{}");
        await using var bus = await WebApiClusterMessageBusTestHelpers.CreateBusAsync(httpClient: httpClient);

        var channel = Substitute.For<IChannel>();
        channel.Metadata.ChannelName.Returns("my-channel");

        var act = async () => await bus.RestoreFromLeaderAsync(new Uri("https://leader:5001/"), channel);

        await act.Should().ThrowAsync<HttpRequestException>();
    }
}
