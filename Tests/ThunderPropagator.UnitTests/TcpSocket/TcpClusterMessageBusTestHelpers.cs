using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using ThunderPropagator.Application.Channels.Cluster;
using ThunderPropagator.Application.Channels.Cluster.Discovery;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;
using ThunderPropagator.ClusterMessageBuses.TcpSocket;

namespace ThunderPropagator.UnitTests.TcpSocket;

/// <summary>
/// Shared construction helpers for <see cref="TcpClusterMessageBus"/> tests. Every test constructs
/// the bus through the same seams the production DI extension uses
/// (<c>IOptions&lt;TcpClusterMessageBusOptions&gt;</c>, <c>ClusterConfiguration</c>,
/// <c>IClusterChannelResolver</c>, <c>IClusterNodeDiscovery</c>, and injectable listener/outbound-
/// connection factory delegates) so no test ever binds a real socket:
/// <c>ITcpClusterConnection</c>/<c>ITcpClusterListener</c> are plain interfaces, substituted
/// directly with NSubstitute — simpler than the WebSocket transport's abstract-class substitution,
/// since <c>NetworkStream</c>/<c>TcpClient</c> have no virtual members to mock at all.
/// </summary>
internal static class TcpClusterMessageBusTestHelpers
{
    internal static readonly Uri DefaultNodeEndpoint = new("https://node1:5000/");

    /// <summary>
    /// Builds a substitute <see cref="ITcpClusterConnection"/> whose <c>SendFrameAsync</c> always
    /// completes successfully and whose <c>ReceiveFramesAsync</c>, by default, yields nothing and
    /// blocks (respecting cancellation) rather than completing immediately — an unconfigured
    /// substitute returning an empty, already-completed sequence would make
    /// <c>RunConnectionReceiveLoopAsync</c>'s background task exit instantly, which is fine for most
    /// tests but would mask a hang if a test's expectations changed; blocking-until-cancelled is the
    /// more faithful default, matching a real idle connection.
    /// </summary>
    internal static ITcpClusterConnection CreateConnectionSubstitute()
    {
        var connection = Substitute.For<ITcpClusterConnection>();

        connection.SendFrameAsync(Arg.Any<TcpClusterFrame>(), Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        connection.ReceiveFramesAsync(Arg.Any<CancellationToken>())
            .Returns(callInfo => EmptyUntilCancelledAsync((CancellationToken)callInfo[0]));
        connection.DisposeAsync().Returns(ValueTask.CompletedTask);

        return connection;
    }

    private static async IAsyncEnumerable<TcpClusterFrame> EmptyUntilCancelledAsync([EnumeratorCancellation] CancellationToken cancellationToken)
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

    internal static Func<Uri, CancellationToken, Task<ITcpClusterConnection>> OutboundConnectionFactoryReturning(ITcpClusterConnection connection)
        => (_, _) => Task.FromResult(connection);

    /// <summary>
    /// Builds a substitute <see cref="ITcpClusterListener"/> whose <c>AcceptConnectionsAsync</c>
    /// yields <paramref name="connectionsToAccept"/> (if any) and then blocks, respecting
    /// cancellation, exactly like a real listener with no further inbound connections.
    /// </summary>
    internal static ITcpClusterListener CreateListenerSubstitute(IEnumerable<ITcpClusterConnection>? connectionsToAccept = null)
    {
        var connections = (connectionsToAccept ?? []).ToArray();

        var listener = Substitute.For<ITcpClusterListener>();
        listener.AcceptConnectionsAsync(Arg.Any<CancellationToken>())
            .Returns(callInfo => AcceptAsync(connections, (CancellationToken)callInfo[0]));
        listener.DisposeAsync().Returns(ValueTask.CompletedTask);

        return listener;
    }

    private static async IAsyncEnumerable<ITcpClusterConnection> AcceptAsync(
        ITcpClusterConnection[] connections,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        foreach (var connection in connections)
        {
            yield return connection;
        }

        try
        {
            await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Expected once the bus is disposed and its lifetime token is cancelled.
        }
    }

    internal static Func<CancellationToken, Task<ITcpClusterListener>> ListenerFactoryReturning(ITcpClusterListener listener)
        => _ => Task.FromResult(listener);

    internal static IClusterNodeDiscovery CreateDiscoverySubstitute(IReadOnlyList<ClusterNodeEntry> peers)
    {
        var discovery = Substitute.For<IClusterNodeDiscovery>();
        discovery.GetPeersAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(peers));
        return discovery;
    }

    internal static async Task<TcpClusterMessageBus> CreateBusAsync(
        IEnumerable<ITcpClusterConnection>? acceptedConnections = null,
        IClusterNodeDiscovery? discovery = null,
        IClusterChannelResolver? channelResolver = null,
        Uri? nodeEndpoint = null,
        TimeSpan? requestTimeout = null,
        Func<Uri, CancellationToken, Task<ITcpClusterConnection>>? outboundConnectionFactory = null)
    {
        discovery ??= CreateDiscoverySubstitute([]);
        channelResolver ??= Substitute.For<IClusterChannelResolver>();
        outboundConnectionFactory ??= (_, _) => Task.FromResult(CreateConnectionSubstitute());

        var listener = CreateListenerSubstitute(acceptedConnections);

        var options = Options.Create(new TcpClusterMessageBusOptions
        {
            RequestTimeout = requestTimeout ?? TimeSpan.FromSeconds(30),
        });

        var clusterConfiguration = new ClusterConfiguration { NodeEndpoint = nodeEndpoint ?? DefaultNodeEndpoint };

        var bus = new TcpClusterMessageBus(
            options,
            clusterConfiguration,
            channelResolver,
            discovery,
            NullLoggerFactory.Instance,
            ListenerFactoryReturning(listener),
            outboundConnectionFactory);

        await bus.EnsureInitializedAsync(CancellationToken.None);

        return bus;
    }
}
