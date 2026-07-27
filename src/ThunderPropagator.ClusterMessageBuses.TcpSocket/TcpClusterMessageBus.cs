using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using ThunderPropagator.Application.Channels.Cluster;
using ThunderPropagator.Application.Channels.Cluster.Discovery;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.TcpSocket
{
    /// <summary>
    /// Raw-TCP-backed <see cref="IClusterMessageBus"/>: a discovery-based direct-peer-connection
    /// transport, structurally the closest sibling to the WebSocket transport in this repo — every
    /// node both runs its own TCP listener (<see cref="ITcpClusterListener"/>, bound to
    /// <see cref="TcpClusterMessageBusOptions.Port"/>) and maintains one persistent outbound
    /// connection per peer (opened lazily, cached in a <see cref="ClusterConnectionCache{TConnection}"/>
    /// keyed by peer endpoint) — except every message is framed by hand with a 4-byte length prefix,
    /// since raw TCP (unlike WebSocket) has no built-in message boundaries.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Peers are supplied by <see cref="IClusterNodeDiscovery"/> (this transport does not implement
    /// discovery itself). Fan-out and subscription-event publishes are sent over this node's own
    /// outbound connection to every discovered peer, best-effort per peer (one unreachable peer must
    /// not fail the whole publish). Delivery happens when this node's listener accepts an inbound
    /// connection from a peer and dispatches whatever it sends locally.
    /// </para>
    /// <para>
    /// A TCP connection has no native request/reply either, but — like WebSocket, and unlike every
    /// broker transport in this repo — there is no separate reply channel: since the connection is
    /// inherently bidirectional, the answering side simply writes its
    /// <see cref="TcpClusterResponseEnvelope"/> back over the exact same connection the request
    /// arrived on.
    /// </para>
    /// </remarks>
    internal sealed partial class TcpClusterMessageBus : AbstractClusterMessageBus, IAsyncDisposable
    {
        private readonly TcpClusterMessageBusOptions _options;
        private readonly Uri _nodeEndpoint;
        private readonly IClusterChannelResolver _channelResolver;
        private readonly IClusterNodeDiscovery _discovery;
        private readonly ILogger _logger;
        private readonly Guid _selfId = Guid.NewGuid();

        /// <summary>This node's process-lifetime self-echo identifier. Exposed for tests only.</summary>
        internal Guid SelfId => _selfId;

        private readonly Func<CancellationToken, Task<ITcpClusterListener>> _listenerFactory;
        private readonly Func<Uri, CancellationToken, Task<ITcpClusterConnection>> _outboundConnectionFactory;

        private readonly SemaphoreSlim _initLock = new(1, 1);
        private volatile bool _initialized;
        private ITcpClusterListener? _listener;

        private readonly ClusterConnectionCache<ITcpClusterConnection> _outboundConnections;

        private readonly ConcurrentDictionary<Guid, Func<ClusterFanOutMessage, CancellationToken, Task>> _fanOutHandlers = new();
        private readonly ConcurrentDictionary<Guid, Func<ClusterSubscriptionEvent, CancellationToken, Task>> _subscriptionEventHandlers = new();
        private readonly ConcurrentDictionary<Guid, TaskCompletionSource<TcpClusterResponseEnvelope>> _pendingRequests = new();

        private readonly ConcurrentBag<Task> _backgroundTasks = new();
        private readonly CancellationTokenSource _lifetimeCts = new();

        public TcpClusterMessageBus(
            IOptions<TcpClusterMessageBusOptions> options,
            ClusterConfiguration clusterConfiguration,
            IClusterChannelResolver channelResolver,
            IClusterNodeDiscovery discovery,
            ILoggerFactory loggerFactory,
            Func<CancellationToken, Task<ITcpClusterListener>>? listenerFactory = null,
            Func<Uri, CancellationToken, Task<ITcpClusterConnection>>? outboundConnectionFactory = null)
        {
            _options = options.Value;
            _nodeEndpoint = clusterConfiguration.NodeEndpoint
                ?? throw new InvalidOperationException(
                    $"{nameof(ClusterConfiguration)}.{nameof(ClusterConfiguration.NodeEndpoint)} must be set for " +
                    $"{nameof(TcpClusterMessageBus)} to identify this node to peers.");
            _channelResolver = channelResolver;
            _discovery = discovery;
            _logger = loggerFactory.CreateLogger<TcpClusterMessageBus>();
            _listenerFactory = listenerFactory ?? DefaultListenerFactoryAsync;
            _outboundConnectionFactory = outboundConnectionFactory ?? DefaultOutboundConnectionFactoryAsync;
            _outboundConnections = new ClusterConnectionCache<ITcpClusterConnection>(ConnectToPeerAsync);

            Log.Constructed(_logger, _nodeEndpoint.Host);
        }

        private Task<ITcpClusterListener> DefaultListenerFactoryAsync(CancellationToken cancellationToken)
        {
            ITcpClusterListener listener = new TcpListenerClusterListener(_options.Port, _options.MaxFrameSize);
            return Task.FromResult(listener);
        }

        private async Task<ITcpClusterConnection> DefaultOutboundConnectionFactoryAsync(Uri peerEndpoint, CancellationToken cancellationToken)
        {
            var tcpClient = new System.Net.Sockets.TcpClient();
            await tcpClient.ConnectAsync(peerEndpoint.Host, _options.Port, cancellationToken).ConfigureAwait(false);
            return new NetworkStreamTcpClusterConnection(tcpClient, _options.MaxFrameSize);
        }

        /// <summary>
        /// Lazily starts this node's listener and its background accept loop exactly once. Internal
        /// (rather than private) so tests can await it directly against a substitute
        /// <see cref="ITcpClusterListener"/> before exercising the bus.
        /// </summary>
        internal async Task EnsureInitializedAsync(CancellationToken cancellationToken)
        {
            if (_initialized)
                return;

            await _initLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                if (_initialized)
                    return;

                _listener = await _listenerFactory(cancellationToken).ConfigureAwait(false);
                _backgroundTasks.Add(RunAcceptLoopAsync(_listener, _lifetimeCts.Token));

                _initialized = true;
                Log.Started(_logger, _nodeEndpoint.Host);
            }
            finally
            {
                _initLock.Release();
            }
        }

        private async Task RunAcceptLoopAsync(ITcpClusterListener listener, CancellationToken cancellationToken)
        {
            try
            {
                await foreach (var connection in listener.AcceptConnectionsAsync(cancellationToken).ConfigureAwait(false))
                {
                    _backgroundTasks.Add(RunConnectionReceiveLoopAsync(connection, cancellationToken));
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Expected on shutdown.
            }
            catch (Exception exception)
            {
                Log.AcceptLoopFaulted(_logger, exception);
            }
        }

        /// <summary>
        /// Reads and dispatches every frame off one connection (inbound-accepted or outbound-to-peer
        /// — both use the same interface and the same dispatch logic) until it closes or faults.
        /// Internal (rather than private) so tests can drive it directly against a substitute
        /// connection.
        /// </summary>
        internal async Task RunConnectionReceiveLoopAsync(ITcpClusterConnection connection, CancellationToken cancellationToken)
        {
            try
            {
                await foreach (var frame in connection.ReceiveFramesAsync(cancellationToken).ConfigureAwait(false))
                {
                    await DispatchFrameAsync(connection, frame, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Expected on shutdown.
            }
            catch (Exception exception)
            {
                Log.ConnectionReceiveLoopFaulted(_logger, exception);
            }
            finally
            {
                await connection.DisposeAsync().ConfigureAwait(false);
            }
        }

        internal async Task DispatchFrameAsync(ITcpClusterConnection connection, TcpClusterFrame frame, CancellationToken cancellationToken)
        {
            switch (frame.Kind)
            {
                case TcpClusterFrameKind.FanOut:
                    await HandleFanOutDeliveryAsync(frame.PayloadJson, cancellationToken).ConfigureAwait(false);
                    break;
                case TcpClusterFrameKind.SubscriptionEvent:
                    await HandleSubscriptionEventDeliveryAsync(frame.PayloadJson, cancellationToken).ConfigureAwait(false);
                    break;
                case TcpClusterFrameKind.Request:
                    await HandleRequestFrameAsync(
                        frame.PayloadJson,
                        (responseFrame, ct) => connection.SendFrameAsync(responseFrame, ct),
                        cancellationToken).ConfigureAwait(false);
                    break;
                case TcpClusterFrameKind.Response:
                    await HandleResponseDeliveryAsync(frame.PayloadJson, cancellationToken).ConfigureAwait(false);
                    break;
                default:
                    Log.UnknownFrameKind(_logger, frame.Kind.ToString());
                    break;
            }
        }

        /// <summary>
        /// Lazily opens (or reuses) the outbound connection to <paramref name="peerEndpoint"/>.
        /// Internal (rather than private) so tests can force one against a substitute connection
        /// factory without going through <see cref="PublishAsync(Guid,ClusterFanOutMessage,CancellationToken)"/>.
        /// </summary>
        internal Task<ITcpClusterConnection> GetOrCreateOutboundConnectionAsync(Uri peerEndpoint, CancellationToken cancellationToken)
            => _outboundConnections.GetOrCreateAsync(peerEndpoint.ToString(), cancellationToken);

        private async Task<ITcpClusterConnection> ConnectToPeerAsync(string peerEndpointKey, CancellationToken cancellationToken)
        {
            var connection = await _outboundConnectionFactory(new Uri(peerEndpointKey), cancellationToken).ConfigureAwait(false);
            _backgroundTasks.Add(RunConnectionReceiveLoopAsync(connection, _lifetimeCts.Token));

            return connection;
        }

        public async ValueTask DisposeAsync()
        {
            await _lifetimeCts.CancelAsync().ConfigureAwait(false);

            foreach (var pending in _pendingRequests.Values)
            {
                pending.TrySetCanceled();
            }

            _fanOutHandlers.Clear();
            _subscriptionEventHandlers.Clear();

            await _outboundConnections.DisposeAsync().ConfigureAwait(false);

            if (_listener is not null)
            {
                await _listener.DisposeAsync().ConfigureAwait(false);
            }

            try
            {
                await Task.WhenAll(_backgroundTasks.ToArray()).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected — every background loop observes _lifetimeCts.
            }
            catch (Exception exception)
            {
                Log.BackgroundTaskFaultedDuringDispose(_logger, exception);
            }

            _initLock.Dispose();
            _lifetimeCts.Dispose();
        }

        private static partial class Log
        {
            [LoggerMessage(EventId = 91400, Level = LogLevel.Debug,
                Message = "[Cluster] TCP message bus constructed for node '{Host}'; listener is started lazily on first use.")]
            public static partial void Constructed(ILogger logger, string host);

            [LoggerMessage(EventId = 91401, Level = LogLevel.Information,
                Message = "[Cluster] TCP message bus started for node '{Host}'.")]
            public static partial void Started(ILogger logger, string host);

            [LoggerMessage(EventId = 91402, Level = LogLevel.Error,
                Message = "[Cluster] TCP accept loop faulted; no further inbound peer connections will be accepted.")]
            public static partial void AcceptLoopFaulted(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91403, Level = LogLevel.Warning,
                Message = "[Cluster] TCP connection receive loop faulted; the connection is being closed.")]
            public static partial void ConnectionReceiveLoopFaulted(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91404, Level = LogLevel.Warning,
                Message = "[Cluster] TCP frame with unknown kind '{Kind}' was ignored.")]
            public static partial void UnknownFrameKind(ILogger logger, string kind);

            [LoggerMessage(EventId = 91405, Level = LogLevel.Warning,
                Message = "[Cluster] A background TCP task faulted during dispose.")]
            public static partial void BackgroundTaskFaultedDuringDispose(ILogger logger, Exception exception);
        }
    }
}
