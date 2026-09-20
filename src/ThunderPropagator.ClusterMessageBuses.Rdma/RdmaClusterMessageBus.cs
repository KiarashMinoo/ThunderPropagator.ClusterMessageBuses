using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using ThunderPropagator.Application.Channels.Cluster;
using ThunderPropagator.Application.Channels.Cluster.Discovery;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.Rdma
{
    /// <summary>
    /// RDMA-backed (InfiniBand or RoCEv2, both served by the same <c>libibverbs</c>/<c>librdmacm</c>
    /// userspace API) <see cref="IClusterMessageBus"/>: a discovery-based direct-peer-connection
    /// transport, structurally the closest sibling to <c>TcpClusterMessageBus</c> in this repo --
    /// every node both runs its own RDMA CM listener (<see cref="IRdmaQueuePairListener"/>, bound
    /// to <see cref="RdmaClusterMessageBusOptions.Port"/>) and maintains one persistent outbound
    /// reliable-connected (RC) queue pair per peer (opened lazily, cached in a
    /// <see cref="ClusterConnectionCache{TConnection}"/> keyed by peer endpoint, reused directly
    /// from <c>ThunderPropagator.ClusterMessageBuses.SharedKernel</c> exactly like the TCP
    /// transport does -- RDMA connection setup/queue-pair creation/memory registration is exactly
    /// the kind of expensive-per-peer resource that cache exists for) -- except every message is a
    /// single two-sided RDMA SEND/RECV work request rather than a length-prefixed TCP byte stream,
    /// since RC send/recv preserves message boundaries on its own.
    /// </summary>
    /// <remarks>
    /// <para>
    /// !!! NOT VALIDATED ON REAL HARDWARE !!! This environment has no C compiler, no dotnet SDK,
    /// and no RDMA-capable NIC -- nothing in this project has been built, linked, or run. It is a
    /// best-effort implementation against the documented public <c>libibverbs</c>/<c>librdmacm</c>
    /// APIs, written for the user to compile and validate on real Linux RDMA hardware. See the
    /// banner comments in <see cref="LibIbVerbsNativeMethods"/>/<see cref="LibRdmaCmNativeMethods"/>
    /// for exactly what needs checking before this is trustworthy -- struct field offsets in
    /// particular are a correctness-critical, not cosmetic, risk in native interop.
    /// </para>
    /// <para>
    /// Peers are supplied by <see cref="IClusterNodeDiscovery"/> (this transport does not implement
    /// discovery itself). Fan-out and subscription-event publishes are sent over this node's own
    /// outbound connection to every discovered peer, best-effort per peer (one unreachable peer
    /// must not fail the whole publish). Delivery happens when this node's listener accepts an
    /// inbound connection from a peer and dispatches whatever it sends locally.
    /// </para>
    /// <para>
    /// Like TCP and WebSocket, an RC queue pair has no separate reply channel either: since the
    /// connection is inherently bidirectional, the answering side simply writes its
    /// <see cref="ThunderPropagator.ClusterMessageBuses.SharedKernel.ClusterResponseEnvelope"/> back
    /// over the exact same queue pair the request arrived on.
    /// </para>
    /// </remarks>
    internal sealed partial class RdmaClusterMessageBus : AbstractClusterMessageBus, IAsyncDisposable
    {
        private readonly RdmaClusterMessageBusOptions _options;
        private readonly Uri _nodeEndpoint;
        private readonly IClusterChannelResolver _channelResolver;
        private readonly IClusterNodeDiscovery _discovery;
        private readonly ILogger _logger;
        private readonly Guid _selfId = Guid.NewGuid();

        /// <summary>This node's process-lifetime self-echo identifier. Exposed for tests only.</summary>
        internal Guid SelfId => _selfId;

        private readonly Func<CancellationToken, Task<IRdmaQueuePairListener>> _listenerFactory;
        private readonly Func<Uri, CancellationToken, Task<IRdmaQueuePairConnection>> _outboundConnectionFactory;

        private readonly SemaphoreSlim _initLock = new(1, 1);
        private volatile bool _initialized;
        private IRdmaQueuePairListener? _listener;

        private readonly ClusterConnectionCache<IRdmaQueuePairConnection> _outboundConnections;

        private readonly ClusterHandlerRegistry<ClusterFanOutMessage> _fanOutHandlers = new();
        private readonly ClusterHandlerRegistry<ClusterSubscriptionEvent> _subscriptionEventHandlers = new();
        private readonly ClusterHandlerRegistry<ClusterByteMessage> _byteFanOutHandlers = new();
        private readonly PendingRequestTracker<ClusterResponseEnvelope> _pendingRequests = new();

        /// <summary>
        /// Answers this node's own <see cref="ThunderPropagator.ClusterMessageBuses.SharedKernel.ClusterRequestKind.PullSnapshotBytes"/> requests --
        /// see <see cref="IClusterByteSnapshotProvider"/>'s own doc comment. Left unregistered by a
        /// channel-based consumer (nothing here requires it); a non-channel consumer registers its
        /// own implementation.
        /// </summary>
        private readonly IClusterByteSnapshotProvider? _byteSnapshotProvider;

        private readonly ConcurrentBag<Task> _backgroundTasks = new();
        private readonly CancellationTokenSource _lifetimeCts = new();

        public RdmaClusterMessageBus(
            IOptions<RdmaClusterMessageBusOptions> options,
            ClusterConfiguration clusterConfiguration,
            IClusterChannelResolver channelResolver,
            IClusterNodeDiscovery discovery,
            ILoggerFactory loggerFactory,
            Func<CancellationToken, Task<IRdmaQueuePairListener>>? listenerFactory = null,
            Func<Uri, CancellationToken, Task<IRdmaQueuePairConnection>>? outboundConnectionFactory = null,
            IClusterByteSnapshotProvider? byteSnapshotProvider = null)
        {
            _options = options.Value;
            _nodeEndpoint = clusterConfiguration.NodeEndpoint
                ?? throw new InvalidOperationException(
                    $"{nameof(ClusterConfiguration)}.{nameof(ClusterConfiguration.NodeEndpoint)} must be set for " +
                    $"{nameof(RdmaClusterMessageBus)} to identify this node to peers.");
            _channelResolver = channelResolver;
            _discovery = discovery;
            _logger = loggerFactory.CreateLogger<RdmaClusterMessageBus>();
            _listenerFactory = listenerFactory ?? DefaultListenerFactoryAsync;
            _outboundConnectionFactory = outboundConnectionFactory ?? DefaultOutboundConnectionFactoryAsync;
            _outboundConnections = new ClusterConnectionCache<IRdmaQueuePairConnection>(ConnectToPeerAsync);
            _byteSnapshotProvider = byteSnapshotProvider;

            Log.Constructed(_logger, _nodeEndpoint.Host);
        }

        private Task<IRdmaQueuePairListener> DefaultListenerFactoryAsync(CancellationToken cancellationToken)
        {
            IRdmaQueuePairListener listener = new RdmaQueuePairListener(_options);
            return Task.FromResult(listener);
        }

        private async Task<IRdmaQueuePairConnection> DefaultOutboundConnectionFactoryAsync(Uri peerEndpoint, CancellationToken cancellationToken)
        {
            var address = await ResolveIPv4AddressAsync(peerEndpoint.Host, cancellationToken).ConfigureAwait(false);
            var endpoint = new IPEndPoint(address, _options.Port);
            return await RdmaQueuePairConnection.ConnectAsync(endpoint, _options, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Resolves <paramref name="host"/> to an IPv4 address -- this transport's native
        /// <see cref="SockAddrIn"/> binding only handles <c>AF_INET</c>, so an IPv6-only peer host
        /// is not currently supported (unlike the TCP transport, which delegates address family
        /// handling entirely to <see cref="System.Net.Sockets.TcpClient"/>).
        /// </summary>
        private static async Task<IPAddress> ResolveIPv4AddressAsync(string host, CancellationToken cancellationToken)
        {
            if (IPAddress.TryParse(host, out var parsed) && parsed.AddressFamily == AddressFamily.InterNetwork)
                return parsed;

            var addresses = await Dns.GetHostAddressesAsync(host, cancellationToken).ConfigureAwait(false);
            foreach (var address in addresses)
            {
                if (address.AddressFamily == AddressFamily.InterNetwork)
                    return address;
            }

            throw new InvalidOperationException(
                $"Could not resolve an IPv4 address for RDMA peer host '{host}' -- this transport currently supports IPv4 peers only.");
        }

        /// <summary>
        /// Lazily starts this node's listener and its background accept loop exactly once. Internal
        /// (rather than private) so tests can await it directly against a substitute
        /// <see cref="IRdmaQueuePairListener"/> before exercising the bus.
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

        private async Task RunAcceptLoopAsync(IRdmaQueuePairListener listener, CancellationToken cancellationToken)
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
        /// -- both use the same interface and the same dispatch logic) until it closes or faults.
        /// Internal (rather than private) so tests can drive it directly against a substitute
        /// connection.
        /// </summary>
        internal async Task RunConnectionReceiveLoopAsync(IRdmaQueuePairConnection connection, CancellationToken cancellationToken)
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

        internal async Task DispatchFrameAsync(IRdmaQueuePairConnection connection, RdmaClusterFrame frame, CancellationToken cancellationToken)
        {
            switch (frame.Kind)
            {
                case RdmaClusterFrameKind.FanOut:
                    await HandleFanOutDeliveryAsync(frame.PayloadJson, cancellationToken).ConfigureAwait(false);
                    break;
                case RdmaClusterFrameKind.SubscriptionEvent:
                    await HandleSubscriptionEventDeliveryAsync(frame.PayloadJson, cancellationToken).ConfigureAwait(false);
                    break;
                case RdmaClusterFrameKind.Request:
                    await HandleRequestFrameAsync(
                        frame.PayloadJson,
                        (responseFrame, ct) => connection.SendFrameAsync(responseFrame, ct),
                        cancellationToken).ConfigureAwait(false);
                    break;
                case RdmaClusterFrameKind.Response:
                    await HandleResponseDeliveryAsync(frame.PayloadJson, cancellationToken).ConfigureAwait(false);
                    break;
                case RdmaClusterFrameKind.ByteFanOut:
                    await HandleByteFanOutDeliveryAsync(frame.PayloadJson, cancellationToken).ConfigureAwait(false);
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
        internal Task<IRdmaQueuePairConnection> GetOrCreateOutboundConnectionAsync(Uri peerEndpoint, CancellationToken cancellationToken)
            => _outboundConnections.GetOrCreateAsync(peerEndpoint.ToString(), cancellationToken);

        private async Task<IRdmaQueuePairConnection> ConnectToPeerAsync(string peerEndpointKey, CancellationToken cancellationToken)
        {
            var connection = await _outboundConnectionFactory(new Uri(peerEndpointKey), cancellationToken).ConfigureAwait(false);
            _backgroundTasks.Add(RunConnectionReceiveLoopAsync(connection, _lifetimeCts.Token));

            return connection;
        }

        public async ValueTask DisposeAsync()
        {
            await _lifetimeCts.CancelAsync().ConfigureAwait(false);

            _pendingRequests.CancelAll();

            _fanOutHandlers.Clear();
            _subscriptionEventHandlers.Clear();
            _byteFanOutHandlers.Clear();

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
                // Expected -- every background loop observes _lifetimeCts.
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
            [LoggerMessage(EventId = 91700, Level = LogLevel.Debug,
                Message = "[Cluster] RDMA message bus constructed for node '{Host}'; listener is started lazily on first use.")]
            public static partial void Constructed(ILogger logger, string host);

            [LoggerMessage(EventId = 91701, Level = LogLevel.Information,
                Message = "[Cluster] RDMA message bus started for node '{Host}'.")]
            public static partial void Started(ILogger logger, string host);

            [LoggerMessage(EventId = 91702, Level = LogLevel.Error,
                Message = "[Cluster] RDMA accept loop faulted; no further inbound peer connections will be accepted.")]
            public static partial void AcceptLoopFaulted(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91703, Level = LogLevel.Warning,
                Message = "[Cluster] RDMA connection receive loop faulted; the connection is being closed.")]
            public static partial void ConnectionReceiveLoopFaulted(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91704, Level = LogLevel.Warning,
                Message = "[Cluster] RDMA frame with unknown kind '{Kind}' was ignored.")]
            public static partial void UnknownFrameKind(ILogger logger, string kind);

            [LoggerMessage(EventId = 91705, Level = LogLevel.Warning,
                Message = "[Cluster] A background RDMA task faulted during dispose.")]
            public static partial void BackgroundTaskFaultedDuringDispose(ILogger logger, Exception exception);
        }
    }
}
