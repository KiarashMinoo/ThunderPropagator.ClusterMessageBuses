using FluentAssertions;
using NSubstitute;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.Application.Channels.Snapshots;
using ThunderPropagator.BuildingBlocks.Application.Enums;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;
using ThunderPropagator.ClusterMessageBuses.WebApi;

namespace ThunderPropagator.UnitTests.WebApi;

public class WebApiClusterMessageBusRoutingTests
{
    private const string ListenPath = "/thunderpropagator/cluster/webapi";

    [Fact]
    public async Task DispatchRequestAsync_UnknownRoute_Responds404()
    {
        await using var bus = await WebApiClusterMessageBusTestHelpers.CreateBusAsync();

        var request = WebApiClusterMessageBusTestHelpers.CreateIncomingRequest("GET", $"{ListenPath}/nonsense", null, "", out var responses);

        await bus.DispatchRequestAsync(request, CancellationToken.None);

        responses.Should().ContainSingle(r => r.StatusCode == 404);
    }

    [Fact]
    public async Task DispatchRequestAsync_FanOutRoute_DeliversAndResponds204()
    {
        await using var bus = await WebApiClusterMessageBusTestHelpers.CreateBusAsync();

        var channelKey = Guid.NewGuid();
        ClusterFanOutMessage? received = null;
        await bus.SubscribeAsync(channelKey, (m, _) => { received = m; return Task.CompletedTask; });

        var message = new ClusterFanOutMessage(Guid.NewGuid(), 1, CastType.Broadcast, new Dictionary<string, object?>());
        var path = WebApiClusterRouting.FanOutPath(ListenPath, channelKey);
        var request = WebApiClusterMessageBusTestHelpers.CreateIncomingRequest("POST", path, null, message.ToNJson(), out var responses);

        await bus.DispatchRequestAsync(request, CancellationToken.None);

        responses.Should().ContainSingle(r => r.StatusCode == 204);
        received.Should().NotBeNull();
        received!.OriginId.Should().Be(message.OriginId);
    }

    [Fact]
    public async Task DispatchRequestAsync_SnapshotRoute_RespondsWithTheChannelsActiveEntries()
    {
        var channel = Substitute.For<IChannel>();
        channel.SearchSnapshotsAsync(Arg.Any<Func<SnapshotEntry, bool>>(), 0, 0, Arg.Any<CancellationToken>()).Returns([]);

        var resolver = Substitute.For<IClusterChannelResolver>();
        resolver.GetChannel("my-channel").Returns(channel);

        await using var bus = await WebApiClusterMessageBusTestHelpers.CreateBusAsync(channelResolver: resolver);

        var path = WebApiClusterRouting.SnapshotPath(ListenPath, "my-channel");
        var request = WebApiClusterMessageBusTestHelpers.CreateIncomingRequest("GET", path, null, "", out var responses);

        await bus.DispatchRequestAsync(request, CancellationToken.None);

        responses.Should().ContainSingle(r => r.StatusCode == 200);
        await channel.Received(1).SearchSnapshotsAsync(Arg.Any<Func<SnapshotEntry, bool>>(), 0, 0, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task DispatchRequestAsync_HandlerThrows_Responds500InsteadOfCrashingTheAcceptLoop()
    {
        var resolver = Substitute.For<IClusterChannelResolver>();
        resolver.GetChannel("missing-channel").Returns(_ => throw new KeyNotFoundException("no such channel"));

        await using var bus = await WebApiClusterMessageBusTestHelpers.CreateBusAsync(channelResolver: resolver);

        var path = WebApiClusterRouting.SnapshotPath(ListenPath, "missing-channel");
        var request = WebApiClusterMessageBusTestHelpers.CreateIncomingRequest("GET", path, null, "", out var responses);

        var act = async () => await bus.DispatchRequestAsync(request, CancellationToken.None);

        await act.Should().NotThrowAsync("a single faulted request must not crash the listener's accept loop");
        responses.Should().ContainSingle(r => r.StatusCode == 500);
    }
}
