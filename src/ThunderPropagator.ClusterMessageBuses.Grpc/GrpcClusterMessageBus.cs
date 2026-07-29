using System.Collections.Concurrent;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly;
using ThunderPropagator.Application.Channels.Cluster;
using ThunderPropagator.Application.Channels.Cluster.Discovery;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.ClusterMessageBuses.Grpc.Protos;
using ThunderPropagator.ClusterMessageBuses.Grpc.Services;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.Grpc
{
    /// <summary>
    /// gRPC-backed <see cref="IClusterMessageBus"/>: a discovery-based direct-peer-connection
    /// transport, fully self-hosted like every other transport in this repo — it binds its own
    /// embedded ASP.NET Core/Kestrel host (<see cref="IGrpcClusterHost"/>) for inbound gRPC calls
    /// rather than requiring the consuming application to already be an ASP.NET Core app with its
    /// own routing pipeline to map onto.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Fan-out and subscription-sync each use one long-lived bidirectional gRPC stream per peer
    /// pair, opened by whichever side dials the other (<see cref="GrpcPeerConnection"/>) — mirroring
    /// WebSocketClusterMessageBus's outbound-connect-plus-inbound-accept design exactly, except the
    /// "accept" side here is the embedded gRPC host's own service implementations
    /// (<see cref="ClusterFanOutGrpcService"/>/<see cref="ClusterSubscriptionSyncGrpcService"/>)
    /// rather than a raw socket-accept loop. A dedicated per-peer maintenance loop keeps each stream
    /// alive, reconnecting with exponential back-off on failure and resending this node's current
    /// subscription-sync state to the peer on every reconnect.
    /// </para>
    /// <para>
    /// The three leader/peer-pull operations (<see cref="RestoreFromLeaderAsync"/>,
    /// <see cref="SyncDeltaFromLeaderAsync"/>, <see cref="FetchPeerSubscriptionsAsync"/>) need no
    /// hand-rolled correlation-id/reply-destination scheme at all — like the WebApi transport's
    /// HTTP request/reply, gRPC's own unary calls already bind a response to its request — so these
    /// are plain unary RPCs against a freshly-dialed <see cref="GrpcUnaryClients"/> rather than
    /// anything drawn from the cached per-peer streaming connection.
    /// </para>
    /// </remarks>
    internal sealed partial class GrpcClusterMessageBus : AbstractClusterMessageBus, IAsyncDisposable
    {
        private readonly GrpcClusterMessageBusOptions _options;
        private readonly Uri _nodeEndpoint;
        private readonly IClusterChannelResolver _channelResolver;
        private readonly IClusterNodeDiscovery _discovery;
        private readonly ILogger _logger;
        private readonly Guid _selfId = Guid.NewGuid();

        /// <summary>This node's process-lifetime self-echo identifier. Exposed for tests only.</summary>
        internal Guid SelfId => _selfId;

        private readonly Func<CancellationToken, Task<IGrpcClusterHost>> _hostFactory;
        private readonly Func<Uri, CancellationToken, Task<GrpcPeerConnection>> _peerConnectionFactory;
        private readonly Func<Uri, CancellationToken, Task<GrpcUnaryClients>> _unaryClientsFactory;
        private readonly ResiliencePipeline _resiliencePipeline = ClusterResiliencePipelineFactory.Create();

        private readonly SemaphoreSlim _initLock = new(1, 1);
        private volatile bool _initialized;
        private IGrpcClusterHost? _host;

        private readonly ClusterConnectionCache<GrpcPeerConnection> _outboundConnections;
        private readonly ConcurrentDictionary<string, byte> _maintainedPeers = new(StringComparer.Ordinal);

        private readonly ConcurrentDictionary<Guid, Func<ClusterFanOutMessage, CancellationToken, Task>> _fanOutHandlers = new();
        private readonly ConcurrentDictionary<Guid, Func<ClusterSubscriptionEvent, CancellationToken, Task>> _subscriptionEventHandlers = new();

        // The most recent subscription-sync event this node has itself published per channel,
        // replayed to a peer whenever its subscription-sync stream (re)connects, so a peer that
        // missed events while the connection was down still converges on this node's current state.
        private readonly ConcurrentDictionary<Guid, ClusterSubscriptionEvent> _lastPublishedSubscriptionEvents = new();

        private readonly ConcurrentBag<Task> _backgroundTasks = new();
        private readonly CancellationTokenSource _lifetimeCts = new();

        // repo-wide constructor-injection-for-testability shape: every factory defaults to the real
        // production implementation, but tests substitute the generated gRPC clients (ClientBase<T>
        // is generated with a protected parameterless constructor and virtual RPC methods
        // specifically to support this, the same reasoning as GcpPubSub's GAPIC clients) and a
        // substitute IGrpcClusterHost instead of requiring a live network/embedded web server.
        public GrpcClusterMessageBus(
            IOptions<GrpcClusterMessageBusOptions> options,
            ClusterConfiguration clusterConfiguration,
            IClusterChannelResolver channelResolver,
            IClusterNodeDiscovery discovery,
            ILoggerFactory loggerFactory,
            Func<CancellationToken, Task<IGrpcClusterHost>>? hostFactory = null,
            Func<Uri, CancellationToken, Task<GrpcPeerConnection>>? peerConnectionFactory = null,
            Func<Uri, CancellationToken, Task<GrpcUnaryClients>>? unaryClientsFactory = null)
        {
            _options = options.Value;
            _nodeEndpoint = clusterConfiguration.NodeEndpoint
                ?? throw new InvalidOperationException(
                    $"{nameof(ClusterConfiguration)}.{nameof(ClusterConfiguration.NodeEndpoint)} must be set for " +
                    $"{nameof(GrpcClusterMessageBus)} to bind its own gRPC host and identify itself to peers.");
            _channelResolver = channelResolver;
            _discovery = discovery;
            _logger = loggerFactory.CreateLogger<GrpcClusterMessageBus>();
            _hostFactory = hostFactory ?? DefaultHostFactoryAsync;
            _peerConnectionFactory = peerConnectionFactory ?? DefaultPeerConnectionFactoryAsync;
            _unaryClientsFactory = unaryClientsFactory ?? DefaultUnaryClientsFactoryAsync;
            _outboundConnections = new ClusterConnectionCache<GrpcPeerConnection>(
                (key, cancellationToken) => _peerConnectionFactory(new Uri(key), cancellationToken));

            if (_options.FallbackToHttp)
            {
                // Required for Grpc.Net.Client to dial a plain-text http:// peer endpoint at all —
                // otherwise SocketsHttpHandler refuses to negotiate HTTP/2 without TLS.
                AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
            }

            Log.Constructed(_logger, _nodeEndpoint.Host);
        }

        private GrpcChannelOptions BuildChannelOptions()
        {
            var channelOptions = new GrpcChannelOptions
            {
                HttpHandler = new System.Net.Http.SocketsHttpHandler
                {
                    EnableMultipleHttp2Connections = true,
                    KeepAlivePingDelay = _options.KeepAliveInterval,
                    KeepAlivePingTimeout = _options.KeepAliveInterval,
                    KeepAlivePingPolicy = System.Net.Http.HttpKeepAlivePingPolicy.WithActiveRequests,
                },
            };
            _options.ConfigureChannel?.Invoke(channelOptions);
            return channelOptions;
        }

        private async Task<GrpcPeerConnection> DefaultPeerConnectionFactoryAsync(Uri peerEndpoint, CancellationToken cancellationToken)
        {
            var channel = GrpcChannel.ForAddress(peerEndpoint, BuildChannelOptions());
            var fanOutClient = new ClusterFanOut.ClusterFanOutClient(channel);
            var subscriptionSyncClient = new ClusterSubscriptionSync.ClusterSubscriptionSyncClient(channel);

            var connection = new GrpcPeerConnection(fanOutClient, subscriptionSyncClient, cancellationToken, channel);
            return await Task.FromResult(connection).ConfigureAwait(false);
        }

        private Task<GrpcUnaryClients> DefaultUnaryClientsFactoryAsync(Uri peerEndpoint, CancellationToken cancellationToken)
        {
            var channel = GrpcChannel.ForAddress(peerEndpoint, BuildChannelOptions());
            var snapshotClient = new ClusterSnapshot.ClusterSnapshotClient(channel);
            var subscriptionFetchClient = new ClusterSubscriptionFetch.ClusterSubscriptionFetchClient(channel);

            return Task.FromResult(new GrpcUnaryClients(snapshotClient, subscriptionFetchClient, channel));
        }

        private Task<IGrpcClusterHost> DefaultHostFactoryAsync(CancellationToken cancellationToken)
        {
            var builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.Services.AddGrpc();
            builder.Services.AddSingleton(this);

            builder.WebHost.ConfigureKestrel(kestrelOptions =>
            {
                var listenPort = _nodeEndpoint.Port;

                if (_options.FallbackToHttp)
                {
                    kestrelOptions.ListenAnyIP(listenPort, listenOptions =>
                        listenOptions.Protocols = Microsoft.AspNetCore.Server.Kestrel.Core.HttpProtocols.Http2);
                }
                else
                {
                    kestrelOptions.ListenAnyIP(listenPort, listenOptions =>
                    {
                        listenOptions.Protocols = Microsoft.AspNetCore.Server.Kestrel.Core.HttpProtocols.Http2;
                        listenOptions.UseHttps();
                    });
                }

                _options.ConfigureKestrel?.Invoke(kestrelOptions);
            });

            var app = builder.Build();
            app.MapGrpcService<ClusterFanOutGrpcService>();
            app.MapGrpcService<ClusterSubscriptionSyncGrpcService>();
            app.MapGrpcService<ClusterSnapshotGrpcService>();
            app.MapGrpcService<ClusterSubscriptionFetchGrpcService>();

            IGrpcClusterHost host = new AspNetCoreGrpcClusterHost(app);
            return Task.FromResult(host);
        }

        /// <summary>
        /// Lazily starts this node's embedded gRPC host exactly once. Internal (rather than private)
        /// so tests can await it directly against a substitute <see cref="IGrpcClusterHost"/> before
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
        /// Lazily opens (or reuses) the cached outbound streaming connection to
        /// <paramref name="peerEndpoint"/>, and starts that peer's reconnect-with-back-off
        /// maintenance loop exactly once. Internal so tests can force one against a substitute
        /// connection factory.
        /// </summary>
        internal Task<GrpcPeerConnection> GetOrCreateOutboundConnectionAsync(Uri peerEndpoint, CancellationToken cancellationToken)
        {
            EnsurePeerMaintained(peerEndpoint);
            return _outboundConnections.GetOrCreateAsync(peerEndpoint.ToString(), cancellationToken);
        }

        private void EnsurePeerMaintained(Uri peerEndpoint)
        {
            if (_maintainedPeers.TryAdd(peerEndpoint.ToString(), 0))
            {
                _backgroundTasks.Add(RunPeerMaintenanceLoopAsync(peerEndpoint, _lifetimeCts.Token));
            }
        }

        /// <summary>
        /// Keeps one peer's streaming connection alive for the lifetime of the bus: (re)connects,
        /// resends this node's current subscription-sync state, then blocks on the subscription-sync
        /// ack stream as a liveness signal — gRPC surfaces a broken HTTP/2 stream on the next read or
        /// write rather than failing the connect step itself, so this is what actually detects a dead
        /// connection. On failure, the connection is evicted and reconnection is retried with
        /// exponential back-off (reset after every successful reconnect).
        /// </summary>
        private async Task RunPeerMaintenanceLoopAsync(Uri peerEndpoint, CancellationToken cancellationToken)
        {
            var peerKey = peerEndpoint.ToString();
            var delay = _options.InitialReconnectDelay;

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var connection = await _outboundConnections.GetOrCreateAsync(peerKey, cancellationToken).ConfigureAwait(false);
                    await ResendActiveSubscriptionsAsync(connection, cancellationToken).ConfigureAwait(false);

                    delay = _options.InitialReconnectDelay;

                    await foreach (var _ in connection.SubscriptionSyncCall.ResponseStream
                        .ReadAllAsync(cancellationToken)
                        .ConfigureAwait(false))
                    {
                        // Acks are only consumed here as a liveness signal for this peer's stream —
                        // subscription-sync itself is fire-and-forget everywhere else in this repo.
                    }
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception exception)
                {
                    Log.PeerStreamFaulted(_logger, exception, peerEndpoint.Host);
                }

                await _outboundConnections.InvalidateAsync(peerKey).ConfigureAwait(false);

                try
                {
                    await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }

                delay = TimeSpan.FromTicks(Math.Min(delay.Ticks * 2, _options.MaxReconnectDelay.Ticks));
            }
        }

        public async ValueTask DisposeAsync()
        {
            await _lifetimeCts.CancelAsync().ConfigureAwait(false);

            _fanOutHandlers.Clear();
            _subscriptionEventHandlers.Clear();

            await _outboundConnections.DisposeAsync().ConfigureAwait(false);

            if (_host is not null)
            {
                await _host.DisposeAsync().ConfigureAwait(false);
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
                Message = "[Cluster] gRPC message bus constructed for node '{Host}'; host is started lazily on first use.")]
            public static partial void Constructed(ILogger logger, string host);

            [LoggerMessage(EventId = 91401, Level = LogLevel.Information,
                Message = "[Cluster] gRPC message bus started for node '{Host}'.")]
            public static partial void Started(ILogger logger, string host);

            [LoggerMessage(EventId = 91402, Level = LogLevel.Warning,
                Message = "[Cluster] gRPC peer stream to '{Host}' faulted; reconnecting with back-off.")]
            public static partial void PeerStreamFaulted(ILogger logger, Exception exception, string host);

            [LoggerMessage(EventId = 91403, Level = LogLevel.Warning,
                Message = "[Cluster] A background gRPC task faulted during dispose.")]
            public static partial void BackgroundTaskFaultedDuringDispose(ILogger logger, Exception exception);
        }
    }
}
