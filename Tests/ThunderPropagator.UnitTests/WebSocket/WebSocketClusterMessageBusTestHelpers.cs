using System.Net.WebSockets;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using ThunderPropagator.Application.Channels.Cluster;
using ThunderPropagator.Application.Channels.Cluster.Discovery;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;
using ThunderPropagator.ClusterMessageBuses.WebSocket;

namespace ThunderPropagator.UnitTests.WebSocket;

/// <summary>
/// Shared construction helpers for <see cref="WebSocketClusterMessageBus"/> tests. Every test
/// constructs the bus through the same seams the production DI extension uses
/// (<c>IOptions&lt;WebSocketClusterMessageBusOptions&gt;</c>, <c>ClusterConfiguration</c>,
/// <c>IClusterChannelResolver</c>, <c>IClusterNodeDiscovery</c>, and injectable listener/outbound-
/// socket factory delegates) so no test ever binds a real socket:
/// <see cref="System.Net.WebSockets.WebSocket"/> is an abstract class with no sealed members, so it
/// is substituted directly with NSubstitute (the same technique commonly used for
/// <c>HttpMessageHandler</c>), while <see cref="IWebSocketClusterListener"/> — the narrow seam
/// around the real <see cref="System.Net.HttpListener"/> — is a plain interface substituted the
/// same way.
/// </summary>
internal static class WebSocketClusterMessageBusTestHelpers
{
    internal static readonly Uri DefaultNodeEndpoint = new("https://node1:5000/");

    /// <summary>
    /// Builds a substitute <see cref="System.Net.WebSockets.WebSocket"/> whose <c>SendAsync</c>
    /// always completes successfully and whose <c>ReceiveAsync</c>, by default, blocks until the
    /// caller's own <see cref="CancellationToken"/> is cancelled rather than returning
    /// immediately — an unconfigured NSubstitute member would otherwise return a default
    /// (all-zero) <see cref="WebSocketReceiveResult"/> with <c>EndOfMessage = false</c>, which would
    /// spin <see cref="WebSocketPeerConnection.ReceiveFramesAsync"/>'s inner loop forever without
    /// ever awaiting real I/O. Every connection this bus opens or accepts starts its own background
    /// receive loop, so every socket substitute must behave safely with no further configuration.
    /// </summary>
    internal static System.Net.WebSockets.WebSocket CreateSocketSubstitute(WebSocketState state = WebSocketState.Open)
    {
        var socket = Substitute.For<System.Net.WebSockets.WebSocket>();

        socket.State.Returns(state);
        socket.SendAsync(Arg.Any<ArraySegment<byte>>(), Arg.Any<WebSocketMessageType>(), Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);
        socket.ReceiveAsync(Arg.Any<ArraySegment<byte>>(), Arg.Any<CancellationToken>())
            .Returns(callInfo => WaitForCancellationAsync((CancellationToken)callInfo[1]));

        return socket;
    }

    private static async Task<WebSocketReceiveResult> WaitForCancellationAsync(CancellationToken cancellationToken)
    {
        await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
        throw new OperationCanceledException(cancellationToken); // unreachable — Task.Delay throws first.
    }

    internal static Func<Uri, CancellationToken, Task<System.Net.WebSockets.WebSocket>> OutboundSocketFactoryReturning(System.Net.WebSockets.WebSocket socket)
        => (_, _) => Task.FromResult(socket);

    /// <summary>
    /// Builds a substitute <see cref="IWebSocketClusterListener"/> whose <c>AcceptConnectionsAsync</c>
    /// yields <paramref name="connectionsToAccept"/> (if any) and then blocks, respecting
    /// cancellation, exactly like a real listener that simply has no further inbound connections —
    /// never completing on its own.
    /// </summary>
    internal static IWebSocketClusterListener CreateListenerSubstitute(IEnumerable<System.Net.WebSockets.WebSocket>? connectionsToAccept = null)
    {
        var sockets = (connectionsToAccept ?? []).ToArray();

        var listener = Substitute.For<IWebSocketClusterListener>();
        listener.AcceptConnectionsAsync(Arg.Any<CancellationToken>())
            .Returns(callInfo => AcceptAsync(sockets, (CancellationToken)callInfo[0]));
        listener.DisposeAsync().Returns(ValueTask.CompletedTask);

        return listener;
    }

    private static async IAsyncEnumerable<System.Net.WebSockets.WebSocket> AcceptAsync(
        System.Net.WebSockets.WebSocket[] sockets,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        foreach (var socket in sockets)
        {
            yield return socket;
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

    internal static Func<CancellationToken, Task<IWebSocketClusterListener>> ListenerFactoryReturning(IWebSocketClusterListener listener)
        => _ => Task.FromResult(listener);

    /// <summary>Builds a substitute <see cref="IClusterNodeDiscovery"/> that always returns <paramref name="peers"/>.</summary>
    internal static IClusterNodeDiscovery CreateDiscoverySubstitute(IReadOnlyList<ClusterNodeEntry> peers)
    {
        var discovery = Substitute.For<IClusterNodeDiscovery>();
        discovery.GetPeersAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(peers));
        return discovery;
    }

    internal static async Task<WebSocketClusterMessageBus> CreateBusAsync(
        IEnumerable<System.Net.WebSockets.WebSocket>? acceptedConnections = null,
        IClusterNodeDiscovery? discovery = null,
        IClusterChannelResolver? channelResolver = null,
        Uri? nodeEndpoint = null,
        TimeSpan? requestTimeout = null,
        Func<Uri, CancellationToken, Task<System.Net.WebSockets.WebSocket>>? outboundSocketFactory = null)
    {
        discovery ??= CreateDiscoverySubstitute([]);
        channelResolver ??= Substitute.For<IClusterChannelResolver>();
        outboundSocketFactory ??= (_, _) => Task.FromResult(CreateSocketSubstitute());

        var listener = CreateListenerSubstitute(acceptedConnections);

        var options = Options.Create(new WebSocketClusterMessageBusOptions
        {
            RequestTimeout = requestTimeout ?? TimeSpan.FromSeconds(30),
        });

        var clusterConfiguration = new ClusterConfiguration { NodeEndpoint = nodeEndpoint ?? DefaultNodeEndpoint };

        var bus = new WebSocketClusterMessageBus(
            options,
            clusterConfiguration,
            channelResolver,
            discovery,
            NullLoggerFactory.Instance,
            ListenerFactoryReturning(listener),
            outboundSocketFactory);

        await bus.EnsureInitializedAsync(CancellationToken.None);

        return bus;
    }
}
