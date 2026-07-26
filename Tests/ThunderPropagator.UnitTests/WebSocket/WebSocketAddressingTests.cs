using FluentAssertions;
using ThunderPropagator.ClusterMessageBuses.WebSocket;

namespace ThunderPropagator.UnitTests.WebSocket;

public class WebSocketAddressingTests
{
    [Fact]
    public void ListenPrefix_UsesNodeEndpointsSchemeAndAuthority_WithTheGivenPath()
    {
        var prefix = WebSocketAddressing.ListenPrefix(new Uri("https://node1:5001/"), "/cluster/ws");

        prefix.Should().Be("https://node1:5001/cluster/ws/");
    }

    [Fact]
    public void ListenPrefix_PathWithoutLeadingSlash_IsNormalized()
    {
        var prefix = WebSocketAddressing.ListenPrefix(new Uri("http://node1:5001/"), "cluster/ws");

        prefix.Should().Be("http://node1:5001/cluster/ws/");
    }

    [Fact]
    public void PeerConnectUri_HttpsEndpoint_UsesWssScheme()
    {
        var uri = WebSocketAddressing.PeerConnectUri(new Uri("https://node2:5001/"), "/cluster/ws");

        uri.Scheme.Should().Be("wss");
        uri.Authority.Should().Be("node2:5001");
    }

    [Fact]
    public void PeerConnectUri_HttpEndpoint_UsesWsScheme()
    {
        var uri = WebSocketAddressing.PeerConnectUri(new Uri("http://node2:5001/"), "/cluster/ws");

        uri.Scheme.Should().Be("ws");
    }

    [Fact]
    public void PeerConnectUri_AndListenPrefix_AgreeOnThePathForTheSameNode()
    {
        var nodeEndpoint = new Uri("https://node1:5001/");

        var prefix = WebSocketAddressing.ListenPrefix(nodeEndpoint, "/cluster/ws");
        var connectUri = WebSocketAddressing.PeerConnectUri(nodeEndpoint, "/cluster/ws");

        prefix.Should().Be("https://node1:5001/cluster/ws/");
        connectUri.AbsolutePath.Should().Be("/cluster/ws/");
    }
}
