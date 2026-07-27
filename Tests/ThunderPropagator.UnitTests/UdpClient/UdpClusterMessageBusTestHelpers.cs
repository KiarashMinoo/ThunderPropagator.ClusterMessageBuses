using System.Net;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using ThunderPropagator.Application.Channels.Cluster;
using ThunderPropagator.Application.Channels.Cluster.Discovery;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;
using ThunderPropagator.ClusterMessageBuses.UdpClient;

namespace ThunderPropagator.UnitTests.UdpClient;

/// <summary>
/// Shared construction helpers for <see cref="UdpClusterMessageBus"/> tests. Every test constructs
/// the bus through the same seams the production DI extension uses
/// (<c>IOptions&lt;UdpClusterMessageBusOptions&gt;</c>, <c>ClusterConfiguration</c>,
/// <c>IClusterChannelResolver</c>, <c>IClusterNodeDiscovery</c>, and injectable socket-factory /
/// peer-endpoint-resolver delegates) so no test ever binds a real UDP socket or performs a real DNS
/// lookup: <c>IUdpClusterSocket</c> is a plain interface, substituted directly with NSubstitute.
/// </summary>
internal static class UdpClusterMessageBusTestHelpers
{
    internal static readonly Uri DefaultNodeEndpoint = new("https://node1:5000/");
    internal static readonly IPEndPoint DefaultResolvedEndpoint = new(IPAddress.Loopback, 6300);

    /// <summary>
    /// Builds a substitute <see cref="IUdpClusterSocket"/> whose <c>SendDatagramAsync</c> always
    /// completes successfully and whose <c>ReceiveDatagramsAsync</c>, by default, yields nothing and
    /// blocks (respecting cancellation) rather than completing immediately — matching a real idle
    /// socket that simply hasn't received anything yet.
    /// </summary>
    internal static IUdpClusterSocket CreateSocketSubstitute()
    {
        var socket = Substitute.For<IUdpClusterSocket>();

        socket.SendDatagramAsync(Arg.Any<byte[]>(), Arg.Any<IPEndPoint>(), Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        socket.ReceiveDatagramsAsync(Arg.Any<CancellationToken>())
            .Returns(callInfo => EmptyUntilCancelledAsync((CancellationToken)callInfo[0]));
        socket.DisposeAsync().Returns(ValueTask.CompletedTask);

        return socket;
    }

    private static async IAsyncEnumerable<UdpReceivedDatagram> EmptyUntilCancelledAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Expected once the bus is disposed and its lifetime token is cancelled.
        }

        yield break;
    }

    internal static Func<CancellationToken, Task<IUdpClusterSocket>> SocketFactoryReturning(IUdpClusterSocket socket)
        => _ => Task.FromResult(socket);

    internal static Func<Uri, CancellationToken, Task<IPEndPoint>> ResolverReturning(IPEndPoint endpoint)
        => (_, _) => Task.FromResult(endpoint);

    internal static IClusterNodeDiscovery CreateDiscoverySubstitute(IReadOnlyList<ClusterNodeEntry> peers)
    {
        var discovery = Substitute.For<IClusterNodeDiscovery>();
        discovery.GetPeersAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(peers));
        return discovery;
    }

    internal static async Task<UdpClusterMessageBus> CreateBusAsync(
        IUdpClusterSocket? socket = null,
        IClusterNodeDiscovery? discovery = null,
        IClusterChannelResolver? channelResolver = null,
        Uri? nodeEndpoint = null,
        TimeSpan? requestTimeout = null,
        TimeSpan? resendInterval = null,
        Func<Uri, CancellationToken, Task<IPEndPoint>>? peerEndpointResolver = null)
    {
        discovery ??= CreateDiscoverySubstitute([]);
        channelResolver ??= Substitute.For<IClusterChannelResolver>();
        socket ??= CreateSocketSubstitute();
        peerEndpointResolver ??= ResolverReturning(DefaultResolvedEndpoint);

        var options = Options.Create(new UdpClusterMessageBusOptions
        {
            RequestTimeout = requestTimeout ?? TimeSpan.FromSeconds(30),
            ResendInterval = resendInterval ?? TimeSpan.FromSeconds(2),
        });

        var clusterConfiguration = new ClusterConfiguration { NodeEndpoint = nodeEndpoint ?? DefaultNodeEndpoint };

        var bus = new UdpClusterMessageBus(
            options,
            clusterConfiguration,
            channelResolver,
            discovery,
            NullLoggerFactory.Instance,
            SocketFactoryReturning(socket),
            peerEndpointResolver);

        await bus.EnsureInitializedAsync(CancellationToken.None);

        return bus;
    }
}
