using FluentAssertions;
using ThunderPropagator.ClusterMessageBuses.WebApi;

namespace ThunderPropagator.UnitTests.WebApi;

public class WebApiClusterRoutingTests
{
    private const string ListenPath = "/thunderpropagator/cluster/webapi";

    [Fact]
    public void Match_PostFanOut_ReturnsFanOutRouteWithChannelKey()
    {
        var channelKey = Guid.NewGuid();
        var path = WebApiClusterRouting.FanOutPath(ListenPath, channelKey);

        var match = WebApiClusterRouting.Match(ListenPath, "POST", path, null);

        match.Should().NotBeNull();
        match!.Kind.Should().Be(WebApiRouteKind.FanOut);
        match.ChannelKey.Should().Be(channelKey);
    }

    [Fact]
    public void Match_PostSubscriptionEvent_ReturnsSubscriptionEventRouteWithChannelKey()
    {
        var channelKey = Guid.NewGuid();
        var path = WebApiClusterRouting.SubscriptionEventPath(ListenPath, channelKey);

        var match = WebApiClusterRouting.Match(ListenPath, "POST", path, null);

        match.Should().NotBeNull();
        match!.Kind.Should().Be(WebApiRouteKind.SubscriptionEvent);
        match.ChannelKey.Should().Be(channelKey);
    }

    [Fact]
    public void Match_GetSnapshot_ReturnsSnapshotRouteWithChannelName()
    {
        var path = WebApiClusterRouting.SnapshotPath(ListenPath, "my channel");

        var match = WebApiClusterRouting.Match(ListenPath, "GET", path, null);

        match.Should().NotBeNull();
        match!.Kind.Should().Be(WebApiRouteKind.Snapshot);
        match.ChannelName.Should().Be("my channel");
    }

    [Fact]
    public void Match_GetSnapshotDelta_ReturnsSnapshotDeltaRouteWithChannelNameAndSinceTicks()
    {
        var sinceTicks = DateTimeOffset.UtcNow.UtcTicks;
        var fullPath = WebApiClusterRouting.SnapshotDeltaPath(ListenPath, "my-channel", sinceTicks);
        var (path, query) = SplitPathAndQuery(fullPath);

        var match = WebApiClusterRouting.Match(ListenPath, "GET", path, query);

        match.Should().NotBeNull();
        match!.Kind.Should().Be(WebApiRouteKind.SnapshotDelta);
        match.ChannelName.Should().Be("my-channel");
        match.SinceTicks.Should().Be(sinceTicks);
    }

    [Fact]
    public void Match_GetSubscriptionsSelf_ReturnsSubscriptionsSelfRouteWithChannelKey()
    {
        var channelKey = Guid.NewGuid();
        var path = WebApiClusterRouting.SubscriptionsSelfPath(ListenPath, channelKey);

        var match = WebApiClusterRouting.Match(ListenPath, "GET", path, null);

        match.Should().NotBeNull();
        match!.Kind.Should().Be(WebApiRouteKind.SubscriptionsSelf);
        match.ChannelKey.Should().Be(channelKey);
    }

    [Fact]
    public void Match_UnknownPath_ReturnsNull()
    {
        var match = WebApiClusterRouting.Match(ListenPath, "GET", $"{ListenPath}/nonsense", null);

        match.Should().BeNull();
    }

    [Fact]
    public void Match_WrongMethodForARoute_ReturnsNull()
    {
        var path = WebApiClusterRouting.SnapshotPath(ListenPath, "my-channel");

        var match = WebApiClusterRouting.Match(ListenPath, "POST", path, null);

        match.Should().BeNull();
    }

    [Fact]
    public void PeerFanOutUrl_AndFanOutPath_AgreeOnTheRouteSegment()
    {
        var channelKey = Guid.NewGuid();
        var peerEndpoint = new Uri("https://peer1:5001/");

        var url = WebApiClusterRouting.PeerFanOutUrl(peerEndpoint, ListenPath, channelKey);
        var expectedPath = WebApiClusterRouting.FanOutPath(ListenPath, channelKey);

        url.Should().Be($"https://peer1:5001{expectedPath}");
    }

    private static (string Path, string? Query) SplitPathAndQuery(string pathAndQuery)
    {
        var index = pathAndQuery.IndexOf('?');
        return index < 0 ? (pathAndQuery, null) : (pathAndQuery[..index], pathAndQuery[index..]);
    }
}
