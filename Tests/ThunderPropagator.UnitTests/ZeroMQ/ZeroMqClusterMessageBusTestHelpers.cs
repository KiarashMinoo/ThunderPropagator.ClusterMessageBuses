using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using ThunderPropagator.Application.Channels.Cluster;
using ThunderPropagator.Application.Channels.Cluster.Discovery;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;
using ThunderPropagator.ClusterMessageBuses.ZeroMQ;

namespace ThunderPropagator.UnitTests.ZeroMQ;

/// <summary>
/// Shared construction helpers for <see cref="ZeroMqClusterMessageBus"/> tests. Every test
/// constructs the bus through the same seams the production DI extension uses.
/// <c>NetMQ.Sockets.RouterSocket</c>/<c>DealerSocket</c> have no interface and cannot be substituted
/// directly, so <see cref="IZeroMqClusterHost"/>/<see cref="IZeroMqPeerConnection"/> are the narrow
/// test seams — no real socket or network is ever needed.
/// </summary>
internal static class ZeroMqClusterMessageBusTestHelpers
{
    internal static readonly Uri DefaultNodeEndpoint = new("https://node1:5000/");

    internal static IZeroMqClusterHost CreateHostSubstitute()
    {
        var host = Substitute.For<IZeroMqClusterHost>();
        host.StartAsync(Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        host.DisposeAsync().Returns(ValueTask.CompletedTask);
        return host;
    }

    internal static IZeroMqPeerConnection CreatePeerConnectionSubstitute() => Substitute.For<IZeroMqPeerConnection>();

    internal static IClusterNodeDiscovery CreateDiscoverySubstitute(IReadOnlyList<ClusterNodeEntry> peers)
    {
        var discovery = Substitute.For<IClusterNodeDiscovery>();
        discovery.GetPeersAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(peers));
        return discovery;
    }

    internal static async Task<ZeroMqClusterMessageBus> CreateBusAsync(
        IClusterNodeDiscovery? discovery = null,
        IClusterChannelResolver? channelResolver = null,
        Uri? nodeEndpoint = null,
        TimeSpan? requestTimeout = null,
        Func<CancellationToken, Task<IZeroMqClusterHost>>? hostFactory = null,
        Func<Uri, CancellationToken, Task<IZeroMqPeerConnection>>? peerConnectionFactory = null)
    {
        discovery ??= CreateDiscoverySubstitute([]);
        channelResolver ??= Substitute.For<IClusterChannelResolver>();

        var options = Options.Create(new ClusterZeroMqOptions
        {
            RequestTimeout = requestTimeout ?? TimeSpan.FromSeconds(30),
        });

        var clusterConfiguration = new ClusterConfiguration { NodeEndpoint = nodeEndpoint ?? DefaultNodeEndpoint };

        var bus = new ZeroMqClusterMessageBus(
            options,
            clusterConfiguration,
            channelResolver,
            discovery,
            NullLoggerFactory.Instance,
            hostFactory ?? (_ => Task.FromResult(CreateHostSubstitute())),
            peerConnectionFactory);

        await bus.EnsureInitializedAsync(CancellationToken.None);

        return bus;
    }
}
