using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NetMQ;
using Polly;
using ThunderPropagator.Application.Channels.Cluster;
using ThunderPropagator.Application.Channels.Cluster.Discovery;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.ZeroMQ
{
    /// <summary>
    /// ZeroMQ-backed <see cref="IClusterMessageBus"/>: a discovery-based direct-peer-connection
    /// transport, brokerless like the WebSocket/WebApi/TcpSocket/UdpClient transports elsewhere in
    /// this repo. Every node is both a ROUTER (this node's own bound socket, accepting inbound
    /// traffic from every peer's DEALER) and, per peer, a DEALER (an outbound connection dialed to
    /// that peer's ROUTER) — a full mesh, matching the ticket's ROUTER/DEALER design exactly.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Fan-out, subscription-sync, and the three leader/peer-pull operations all ride over the same
    /// physical DEALER→ROUTER connection per peer pair, discriminated by <see cref="ZeroMqClusterFrame.Kind"/>
    /// — the same hand-rolled correlation-id/reply scheme <c>WebSocketClusterMessageBus</c> and the
    /// broker transports use, since ROUTER/DEALER has no native request/reply of its own (unlike
    /// gRPC's unary calls or HTTP's request/response). A request's reply is sent back over the exact
    /// same connection it arrived on — the ROUTER socket re-uses the identity frame it read the
    /// request with, never trusting a destination supplied on the wire.
    /// </para>
    /// <para>
    /// ZeroMQ's own auto-reconnect (leveraged at the socket level) keeps a DEALER connection alive
    /// through transient drops without this transport hand-rolling its own reconnect loop, unlike
    /// the gRPC transport — a dead peer only shows up here as a failed/timed-out send or request,
    /// logged and isolated per peer exactly like every other transport's per-peer try/catch.
    /// </para>
    /// </remarks>
    internal sealed partial class ZeroMqClusterMessageBus : AbstractClusterMessageBus, IAsyncDisposable
    {
        private readonly ClusterZeroMqOptions _options;
        private readonly Uri _nodeEndpoint;
        private readonly IClusterChannelResolver _channelResolver;
        private readonly IClusterNodeDiscovery _discovery;
        private readonly ILogger _logger;
        private readonly Guid _selfId = Guid.NewGuid();

        /// <summary>This node's process-lifetime self-echo identifier. Exposed for tests only.</summary>
        internal Guid SelfId => _selfId;

        private readonly Func<CancellationToken, Task<IZeroMqClusterHost>> _hostFactory;
        private readonly Func<Uri, CancellationToken, Task<IZeroMqPeerConnection>> _peerConnectionFactory;
        private readonly ResiliencePipeline _resiliencePipeline = ClusterResiliencePipelineFactory.Create();

        private readonly SemaphoreSlim _initLock = new(1, 1);
        private volatile bool _initialized;
        private IZeroMqClusterHost? _host;

        private readonly ClusterConnectionCache<IZeroMqPeerConnection> _outboundConnections;

        private readonly ConcurrentDictionary<Guid, Func<ClusterFanOutMessage, CancellationToken, Task>> _fanOutHandlers = new();
        private readonly ConcurrentDictionary<Guid, Func<ClusterSubscriptionEvent, CancellationToken, Task>> _subscriptionEventHandlers = new();
        private readonly ConcurrentDictionary<Guid, TaskCompletionSource<ZeroMqClusterResponseEnvelope>> _pendingRequests = new();

        private readonly CancellationTokenSource _lifetimeCts = new();

        // repo-wide constructor-injection-for-testability shape: both factories default to the real
        // production implementation, but tests substitute IZeroMqClusterHost/IZeroMqPeerConnection
        // directly instead of requiring a live network — RouterSocket/DealerSocket themselves have no
        // interface and cannot be substituted, so these two are the narrow seams for this transport.
        public ZeroMqClusterMessageBus(
            IOptions<ClusterZeroMqOptions> options,
            ClusterConfiguration clusterConfiguration,
            IClusterChannelResolver channelResolver,
            IClusterNodeDiscovery discovery,
            ILoggerFactory loggerFactory,
            Func<CancellationToken, Task<IZeroMqClusterHost>>? hostFactory = null,
            Func<Uri, CancellationToken, Task<IZeroMqPeerConnection>>? peerConnectionFactory = null)
        {
            _options = options.Value;
            _nodeEndpoint = clusterConfiguration.NodeEndpoint
                ?? throw new InvalidOperationException(
                    $"{nameof(ClusterConfiguration)}.{nameof(ClusterConfiguration.NodeEndpoint)} must be set for " +
                    $"{nameof(ZeroMqClusterMessageBus)} to bind its own ROUTER socket and identify itself to peers.");
            _channelResolver = channelResolver;
            _discovery = discovery;
            _logger = loggerFactory.CreateLogger<ZeroMqClusterMessageBus>();
            _hostFactory = hostFactory ?? DefaultHostFactoryAsync;
            _peerConnectionFactory = peerConnectionFactory ?? DefaultPeerConnectionFactoryAsync;
            _outboundConnections = new ClusterConnectionCache<IZeroMqPeerConnection>(
                (key, cancellationToken) => _peerConnectionFactory(new Uri(key), cancellationToken));

            Log.Constructed(_logger, _nodeEndpoint.Host);
        }

        private Task<IZeroMqClusterHost> DefaultHostFactoryAsync(CancellationToken cancellationToken)
        {
            IZeroMqClusterHost host = new NetMQClusterHost(_nodeEndpoint, _options, OnRouterReceiveReady);
            return Task.FromResult(host);
        }

        private Task<IZeroMqPeerConnection> DefaultPeerConnectionFactoryAsync(Uri peerEndpoint, CancellationToken cancellationToken)
        {
            IZeroMqPeerConnection connection = new ZeroMqPeerConnection(peerEndpoint, _options, _host!, OnPeerFrameReceived);
            return Task.FromResult(connection);
        }

        /// <summary>
        /// Lazily binds this node's own ROUTER socket exactly once. Internal (rather than private) so
        /// tests can await it directly against a substitute <see cref="IZeroMqClusterHost"/> before
        /// exercising the bus.
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

                _host = await _hostFactory(cancellationToken).ConfigureAwait(false);
                await _host.StartAsync(cancellationToken).ConfigureAwait(false);

                _initialized = true;
                Log.Started(_logger, _nodeEndpoint.Host);
            }
            finally
            {
                _initLock.Release();
            }
        }

        /// <summary>
        /// Lazily opens (or reuses) the cached outbound DEALER connection to <paramref name="peerEndpoint"/>.
        /// Internal so tests can force one against a substitute connection factory.
        /// </summary>
        internal Task<IZeroMqPeerConnection> GetOrCreateOutboundConnectionAsync(Uri peerEndpoint, CancellationToken cancellationToken) =>
            _outboundConnections.GetOrCreateAsync(peerEndpoint.ToString(), cancellationToken);

        /// <summary>
        /// This node's own ROUTER socket's <c>ReceiveReady</c> handler — reads the <c>[identity, payload]</c>
        /// frame pair every DEALER→ROUTER message carries and dispatches it. Fire-and-forget
        /// (<c>_ = DispatchRouterFrameAsync(...)</c>) since the poller thread that raises this event
        /// must never be blocked awaiting the (potentially async) handler, mirroring the same
        /// trade-off documented on <c>RedisPubSubClusterMessageBus</c>'s delegate-based subscription.
        /// </summary>
        private void OnRouterReceiveReady(object? sender, NetMQSocketEventArgs e)
        {
            while (true)
            {
                var message = new NetMQMessage();
                if (!e.Socket.TryReceiveMultipartMessage(ref message, 2))
                    break;

                var identity = message[0].ToByteArray();
                var frameJson = message[1].ConvertToString();

                _ = DispatchRouterFrameAsync(identity, frameJson, _lifetimeCts.Token);
            }
        }

        private async Task DispatchRouterFrameAsync(byte[] identity, string frameJson, CancellationToken cancellationToken)
        {
            ZeroMqClusterFrame? frame;
            try
            {
                frame = frameJson.FromNJson<ZeroMqClusterFrame>();
            }
            catch (Exception exception)
            {
                Log.RouterFrameUnparseable(_logger, exception);
                return;
            }

            if (frame is null)
                return;

            switch (frame.Kind)
            {
                case ZeroMqClusterFrameKind.FanOut:
                    await HandleFanOutDeliveryAsync(frame.PayloadJson, cancellationToken).ConfigureAwait(false);
                    break;
                case ZeroMqClusterFrameKind.SubscriptionEvent:
                    await HandleSubscriptionEventDeliveryAsync(frame.PayloadJson, cancellationToken).ConfigureAwait(false);
                    break;
                case ZeroMqClusterFrameKind.Request:
                    await HandleRequestFrameAsync(identity, frame.PayloadJson, cancellationToken).ConfigureAwait(false);
                    break;
                default:
                    Log.UnexpectedFrameKindOnRouter(_logger, frame.Kind.ToString());
                    break;
            }
        }

        /// <summary>
        /// A peer connection's DEALER socket's frame-received callback — a DEALER only ever receives
        /// the answer to a request this node itself sent (ZMQ strips the routing/identity frame for
        /// the DEALER side automatically), so this is always a <see cref="ZeroMqClusterFrameKind.Response"/>.
        /// </summary>
        private void OnPeerFrameReceived(string frameJson)
        {
            ZeroMqClusterFrame? frame;
            try
            {
                frame = frameJson.FromNJson<ZeroMqClusterFrame>();
            }
            catch (Exception exception)
            {
                Log.PeerFrameUnparseable(_logger, exception);
                return;
            }

            if (frame is null || frame.Kind != ZeroMqClusterFrameKind.Response)
            {
                Log.UnexpectedFrameKindOnPeerConnection(_logger, frame?.Kind.ToString() ?? "null");
                return;
            }

            ZeroMqClusterResponseEnvelope? response;
            try
            {
                response = frame.PayloadJson.FromNJson<ZeroMqClusterResponseEnvelope>();
            }
            catch (Exception exception)
            {
                Log.PeerFrameUnparseable(_logger, exception);
                return;
            }

            if (response is not null)
                TryCompletePendingRequest(response);
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

            if (_host is not null)
            {
                await _host.DisposeAsync().ConfigureAwait(false);
            }

            _initLock.Dispose();
            _lifetimeCts.Dispose();
        }

        private static partial class Log
        {
            [LoggerMessage(EventId = 91500, Level = LogLevel.Debug,
                Message = "[Cluster] ZeroMQ message bus constructed for node '{Host}'; ROUTER socket is bound lazily on first use.")]
            public static partial void Constructed(ILogger logger, string host);

            [LoggerMessage(EventId = 91501, Level = LogLevel.Information,
                Message = "[Cluster] ZeroMQ message bus started for node '{Host}'.")]
            public static partial void Started(ILogger logger, string host);

            [LoggerMessage(EventId = 91502, Level = LogLevel.Warning,
                Message = "[Cluster] ZeroMQ ROUTER frame could not be parsed; skipping it.")]
            public static partial void RouterFrameUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91503, Level = LogLevel.Warning,
                Message = "[Cluster] ZeroMQ peer connection frame could not be parsed; skipping it.")]
            public static partial void PeerFrameUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91504, Level = LogLevel.Warning,
                Message = "[Cluster] ZeroMQ ROUTER socket received an unexpected frame kind '{Kind}'; skipping it.")]
            public static partial void UnexpectedFrameKindOnRouter(ILogger logger, string kind);

            [LoggerMessage(EventId = 91505, Level = LogLevel.Warning,
                Message = "[Cluster] ZeroMQ peer connection received an unexpected frame kind '{Kind}'; skipping it.")]
            public static partial void UnexpectedFrameKindOnPeerConnection(ILogger logger, string kind);
        }
    }
}
