using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using ThunderPropagator.Application.Channels.Cluster;
using ThunderPropagator.Application.Channels.Cluster.Discovery;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.WebSocket
{
    /// <summary>
    /// WebSocket-backed <see cref="IClusterMessageBus"/>: a discovery-based direct-peer-connection
    /// transport, closer in shape to core's own <c>HttpClusterMessageBus</c> than the broker
    /// transports elsewhere in this repo (Kafka/RabbitMQ/NATS/Pulsar/MQTT/ActiveMQ/RedisPubSub all
    /// rely on a shared server; this one dials every peer directly).
    /// </summary>
    /// <remarks>
    /// <para>
    /// Every node both runs its own WebSocket listener (<see cref="IWebSocketClusterListener"/>,
    /// bound to this node's own <see cref="ClusterConfiguration.NodeEndpoint"/>) and maintains one
    /// persistent outbound <see cref="System.Net.WebSockets.ClientWebSocket"/> connection per peer
    /// (opened lazily, cached in a <see cref="ClusterConnectionCache{TConnection}"/> keyed by peer
    /// endpoint) — mirroring how <c>HttpClusterMessageBus</c> pairs an outbound
    /// <see cref="System.Net.Http.HttpClient"/> POST with an inbound ASP.NET endpoint, except each
    /// pair of nodes here holds a long-lived socket instead of issuing a POST per message.
    /// </para>
    /// <para>
    /// Peers are supplied by <see cref="IClusterNodeDiscovery"/> (this transport does not implement
    /// discovery itself — register e.g. core's <c>StaticClusterNodeDiscovery</c> alongside it).
    /// Fan-out and subscription-event publishes are sent over this node's own outbound connection to
    /// every discovered peer, best-effort per peer (one unreachable peer must not fail the whole
    /// publish), mirroring <c>HttpClusterMessageBus.PublishAsync</c>'s "POST to every peer in
    /// parallel" fan-out. Delivery happens when this node's listener accepts an inbound connection
    /// from a peer and dispatches whatever it sends locally.
    /// </para>
    /// <para>
    /// A WebSocket connection has no native request/reply either (like every transport except NATS),
    /// but unlike the broker transports there is no separate reply channel: since the connection is
    /// inherently bidirectional, the answering side simply writes its
    /// <see cref="WebSocketClusterResponseEnvelope"/> back over the exact same connection the
    /// request arrived on — see <c>WebSocketClusterRequestEnvelope</c>'s remarks.
    /// </para>
    /// </remarks>
    internal sealed partial class WebSocketClusterMessageBus : AbstractClusterMessageBus, IAsyncDisposable
    {
        private readonly WebSocketClusterMessageBusOptions _options;
        private readonly Uri _nodeEndpoint;
        private readonly IClusterChannelResolver _channelResolver;
        private readonly IClusterNodeDiscovery _discovery;
        private readonly ILogger _logger;
        private readonly Guid _selfId = Guid.NewGuid();

        /// <summary>This node's process-lifetime self-echo identifier. Exposed for tests only.</summary>
        internal Guid SelfId => _selfId;

        private readonly Func<CancellationToken, Task<IWebSocketClusterListener>> _listenerFactory;
        private readonly Func<Uri, CancellationToken, Task<System.Net.WebSockets.WebSocket>> _outboundSocketFactory;

        private readonly SemaphoreSlim _initLock = new(1, 1);
        private volatile bool _initialized;
        private IWebSocketClusterListener? _listener;

        private readonly ClusterConnectionCache<WebSocketPeerConnection> _outboundConnections;

        private readonly ConcurrentDictionary<Guid, Func<ClusterFanOutMessage, CancellationToken, Task>> _fanOutHandlers = new();
        private readonly ConcurrentDictionary<Guid, Func<ClusterSubscriptionEvent, CancellationToken, Task>> _subscriptionEventHandlers = new();
        private readonly ConcurrentDictionary<Guid, TaskCompletionSource<WebSocketClusterResponseEnvelope>> _pendingRequests = new();

        private readonly ConcurrentBag<Task> _backgroundTasks = new();
        private readonly CancellationTokenSource _lifetimeCts = new();

        public WebSocketClusterMessageBus(
            IOptions<WebSocketClusterMessageBusOptions> options,
            ClusterConfiguration clusterConfiguration,
            IClusterChannelResolver channelResolver,
            IClusterNodeDiscovery discovery,
            ILoggerFactory loggerFactory,
            Func<CancellationToken, Task<IWebSocketClusterListener>>? listenerFactory = null,
            Func<Uri, CancellationToken, Task<System.Net.WebSockets.WebSocket>>? outboundSocketFactory = null)
        {
            _options = options.Value;
            _nodeEndpoint = clusterConfiguration.NodeEndpoint
                ?? throw new InvalidOperationException(
                    $"{nameof(ClusterConfiguration)}.{nameof(ClusterConfiguration.NodeEndpoint)} must be set for " +
                    $"{nameof(WebSocketClusterMessageBus)} to bind its own listener and identify itself to peers.");
            _channelResolver = channelResolver;
            _discovery = discovery;
            _logger = loggerFactory.CreateLogger<WebSocketClusterMessageBus>();
            _listenerFactory = listenerFactory ?? DefaultListenerFactoryAsync;
            _outboundSocketFactory = outboundSocketFactory ?? DefaultOutboundSocketFactoryAsync;
            _outboundConnections = new ClusterConnectionCache<WebSocketPeerConnection>(ConnectToPeerAsync);

            Log.Constructed(_logger, _nodeEndpoint.Host);
        }

        private Task<IWebSocketClusterListener> DefaultListenerFactoryAsync(CancellationToken cancellationToken)
        {
            var prefix = WebSocketAddressing.ListenPrefix(_nodeEndpoint, _options.ListenPath);
            IWebSocketClusterListener listener = new HttpListenerWebSocketClusterListener(prefix);
            return Task.FromResult(listener);
        }

        private async Task<System.Net.WebSockets.WebSocket> DefaultOutboundSocketFactoryAsync(Uri peerConnectUri, CancellationToken cancellationToken)
        {
            var client = new System.Net.WebSockets.ClientWebSocket();
            _options.ConfigureClientWebSocket?.Invoke(client.Options);
            await client.ConnectAsync(peerConnectUri, cancellationToken).ConfigureAwait(false);
            return client;
        }

        /// <summary>
        /// Lazily starts this node's listener and its background accept loop exactly once. Internal
        /// (rather than private) so tests can await it directly against a substitute
        /// <see cref="IWebSocketClusterListener"/> before exercising the bus.
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

        private async Task RunAcceptLoopAsync(IWebSocketClusterListener listener, CancellationToken cancellationToken)
        {
            try
            {
                await foreach (var socket in listener.AcceptConnectionsAsync(cancellationToken).ConfigureAwait(false))
                {
                    var connection = new WebSocketPeerConnection(socket, _options.ReceiveBufferSize);
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
        /// — both use the same wrapper and the same dispatch logic) until it closes or faults.
        /// Internal (rather than private) so tests can drive it directly against a connection
        /// wrapping a substitute socket.
        /// </summary>
        internal async Task RunConnectionReceiveLoopAsync(WebSocketPeerConnection connection, CancellationToken cancellationToken)
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

        internal async Task DispatchFrameAsync(WebSocketPeerConnection connection, WebSocketClusterFrame frame, CancellationToken cancellationToken)
        {
            switch (frame.Kind)
            {
                case WebSocketClusterFrameKind.FanOut:
                    await HandleFanOutDeliveryAsync(frame.PayloadJson, cancellationToken).ConfigureAwait(false);
                    break;
                case WebSocketClusterFrameKind.SubscriptionEvent:
                    await HandleSubscriptionEventDeliveryAsync(frame.PayloadJson, cancellationToken).ConfigureAwait(false);
                    break;
                case WebSocketClusterFrameKind.Request:
                    await HandleRequestFrameAsync(
                        frame.PayloadJson,
                        (responseFrame, ct) => connection.SendFrameAsync(responseFrame, ct),
                        cancellationToken).ConfigureAwait(false);
                    break;
                case WebSocketClusterFrameKind.Response:
                    await HandleResponseDeliveryAsync(frame.PayloadJson, cancellationToken).ConfigureAwait(false);
                    break;
                default:
                    Log.UnknownFrameKind(_logger, frame.Kind.ToString());
                    break;
            }
        }

        /// <summary>
        /// Lazily opens (or reuses) the outbound connection to <paramref name="peerEndpoint"/>.
        /// Internal (rather than private) so tests can force one against a substitute socket
        /// factory without going through <see cref="PublishAsync(Guid,ClusterFanOutMessage,CancellationToken)"/>.
        /// </summary>
        internal Task<WebSocketPeerConnection> GetOrCreateOutboundConnectionAsync(Uri peerEndpoint, CancellationToken cancellationToken)
            => _outboundConnections.GetOrCreateAsync(peerEndpoint.ToString(), cancellationToken);

        private async Task<WebSocketPeerConnection> ConnectToPeerAsync(string peerEndpointKey, CancellationToken cancellationToken)
        {
            var peerConnectUri = WebSocketAddressing.PeerConnectUri(new Uri(peerEndpointKey), _options.ListenPath);
            var socket = await _outboundSocketFactory(peerConnectUri, cancellationToken).ConfigureAwait(false);
            var connection = new WebSocketPeerConnection(socket, _options.ReceiveBufferSize);

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
            [LoggerMessage(EventId = 91200, Level = LogLevel.Debug,
                Message = "[Cluster] WebSocket message bus constructed for node '{Host}'; listener is started lazily on first use.")]
            public static partial void Constructed(ILogger logger, string host);

            [LoggerMessage(EventId = 91201, Level = LogLevel.Information,
                Message = "[Cluster] WebSocket message bus started for node '{Host}'.")]
            public static partial void Started(ILogger logger, string host);

            [LoggerMessage(EventId = 91202, Level = LogLevel.Error,
                Message = "[Cluster] WebSocket accept loop faulted; no further inbound peer connections will be accepted.")]
            public static partial void AcceptLoopFaulted(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91203, Level = LogLevel.Warning,
                Message = "[Cluster] WebSocket connection receive loop faulted; the connection is being closed.")]
            public static partial void ConnectionReceiveLoopFaulted(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91204, Level = LogLevel.Warning,
                Message = "[Cluster] WebSocket frame with unknown kind '{Kind}' was ignored.")]
            public static partial void UnknownFrameKind(ILogger logger, string kind);

            [LoggerMessage(EventId = 91205, Level = LogLevel.Warning,
                Message = "[Cluster] A background WebSocket task faulted during dispose.")]
            public static partial void BackgroundTaskFaultedDuringDispose(ILogger logger, Exception exception);
        }
    }
}
