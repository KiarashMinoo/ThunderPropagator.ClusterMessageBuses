using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using ThunderPropagator.Application.Channels.Cluster;
using ThunderPropagator.Application.Channels.Cluster.Discovery;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.WebApi
{
    /// <summary>
    /// HTTP-backed <see cref="IClusterMessageBus"/>: a discovery-based direct-peer-connection
    /// transport, distinct from core's own <c>HttpClusterMessageBus</c> in one key way — it is fully
    /// self-hosted. <c>HttpClusterMessageBus</c> assumes the consuming application already exposes
    /// ASP.NET Core minimal-API endpoints (<c>ClusterMessageBusEndpoints</c> and siblings) for peers
    /// to call; this transport instead binds its own embedded <see cref="System.Net.HttpListener"/>
    /// (<see cref="IWebApiClusterListener"/>) for inbound calls and a plain
    /// <see cref="System.Net.Http.HttpClient"/>-backed sender (<see cref="IWebApiClusterHttpClient"/>)
    /// for outbound ones, so it works in hosts that never adopt ASP.NET Core (console apps, Windows
    /// services, worker services) without requiring any web-hosting infrastructure to already be
    /// wired up.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Fan-out and subscription-event publishes are sent as a plain HTTP POST over this node's shared
    /// <see cref="IWebApiClusterHttpClient"/> to every discovered peer in parallel, best-effort per
    /// peer (one unreachable peer must not fail the whole publish) — the same "POST to every peer"
    /// fan-out <c>HttpClusterMessageBus</c> uses. Delivery happens when this node's own listener
    /// accepts the matching inbound POST and dispatches it to a local handler registry.
    /// </para>
    /// <para>
    /// Unlike every other transport in this repo — including WebSocket — the three leader/peer-pull
    /// operations (<c>RestoreFromLeaderAsync</c>, <c>SyncDeltaFromLeaderAsync</c>,
    /// <c>FetchPeerSubscriptionsAsync</c>) need no hand-rolled correlation-id scheme at all: HTTP
    /// already has native request/reply, so these are plain GET calls whose response body is read
    /// directly, exactly like <c>HttpClusterMessageBus</c> implements them.
    /// </para>
    /// <para>
    /// A single shared <see cref="IWebApiClusterHttpClient"/> is used for every outbound call to
    /// every peer — unlike WebSocket, there is no per-peer connection to cache, since HTTP is
    /// stateless request/response (this mirrors the standard "reuse one HttpClient instance, vary
    /// the request URI" guidance, and how <c>HttpClusterMessageBus</c> itself uses one
    /// <see cref="System.Net.Http.HttpClient"/> via <c>IHttpClientFactory</c> for every peer call).
    /// </para>
    /// </remarks>
    internal sealed partial class WebApiClusterMessageBus : AbstractClusterMessageBus, IAsyncDisposable
    {
        private readonly WebApiClusterMessageBusOptions _options;
        private readonly Uri _nodeEndpoint;
        private readonly IClusterChannelResolver _channelResolver;
        private readonly IClusterNodeDiscovery _discovery;
        private readonly ILogger _logger;
        private readonly Guid _selfId = Guid.NewGuid();

        /// <summary>This node's process-lifetime self-echo identifier. Exposed for tests only.</summary>
        internal Guid SelfId => _selfId;

        private readonly Func<CancellationToken, Task<IWebApiClusterListener>> _listenerFactory;
        private readonly IWebApiClusterHttpClient _httpClient;

        private readonly SemaphoreSlim _initLock = new(1, 1);
        private volatile bool _initialized;
        private IWebApiClusterListener? _listener;

        private readonly ConcurrentDictionary<Guid, Func<ClusterFanOutMessage, CancellationToken, Task>> _fanOutHandlers = new();
        private readonly ConcurrentDictionary<Guid, Func<ClusterSubscriptionEvent, CancellationToken, Task>> _subscriptionEventHandlers = new();

        private readonly ConcurrentBag<Task> _backgroundTasks = new();
        private readonly CancellationTokenSource _lifetimeCts = new();

        public WebApiClusterMessageBus(
            IOptions<WebApiClusterMessageBusOptions> options,
            ClusterConfiguration clusterConfiguration,
            IClusterChannelResolver channelResolver,
            IClusterNodeDiscovery discovery,
            ILoggerFactory loggerFactory,
            Func<CancellationToken, Task<IWebApiClusterListener>>? listenerFactory = null,
            IWebApiClusterHttpClient? httpClient = null)
        {
            _options = options.Value;
            _nodeEndpoint = clusterConfiguration.NodeEndpoint
                ?? throw new InvalidOperationException(
                    $"{nameof(ClusterConfiguration)}.{nameof(ClusterConfiguration.NodeEndpoint)} must be set for " +
                    $"{nameof(WebApiClusterMessageBus)} to bind its own listener and identify itself to peers.");
            _channelResolver = channelResolver;
            _discovery = discovery;
            _logger = loggerFactory.CreateLogger<WebApiClusterMessageBus>();
            _listenerFactory = listenerFactory ?? DefaultListenerFactoryAsync;
            _httpClient = httpClient ?? CreateDefaultHttpClient();

            Log.Constructed(_logger, _nodeEndpoint.Host);
        }

        private Task<IWebApiClusterListener> DefaultListenerFactoryAsync(CancellationToken cancellationToken)
        {
            var prefix = WebApiClusterRouting.ListenPrefix(_nodeEndpoint, _options.ListenPath);
            IWebApiClusterListener listener = new HttpListenerWebApiClusterListener(prefix);
            return Task.FromResult(listener);
        }

        private IWebApiClusterHttpClient CreateDefaultHttpClient()
        {
            var httpClient = new System.Net.Http.HttpClient { Timeout = _options.RequestTimeout };
            _options.ConfigureHttpClient?.Invoke(httpClient);
            return new HttpClientWebApiClusterHttpClient(httpClient);
        }

        /// <summary>
        /// Lazily starts this node's listener and its background accept loop exactly once. Internal
        /// (rather than private) so tests can await it directly against a substitute
        /// <see cref="IWebApiClusterListener"/> before exercising the bus.
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

        private async Task RunAcceptLoopAsync(IWebApiClusterListener listener, CancellationToken cancellationToken)
        {
            try
            {
                await foreach (var request in listener.AcceptRequestsAsync(cancellationToken).ConfigureAwait(false))
                {
                    _backgroundTasks.Add(DispatchRequestAsync(request, cancellationToken));
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

        public async ValueTask DisposeAsync()
        {
            await _lifetimeCts.CancelAsync().ConfigureAwait(false);

            _fanOutHandlers.Clear();
            _subscriptionEventHandlers.Clear();

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

            if (_httpClient is IAsyncDisposable asyncDisposableClient)
            {
                await asyncDisposableClient.DisposeAsync().ConfigureAwait(false);
            }

            _initLock.Dispose();
            _lifetimeCts.Dispose();
        }

        private static partial class Log
        {
            [LoggerMessage(EventId = 91300, Level = LogLevel.Debug,
                Message = "[Cluster] WebApi message bus constructed for node '{Host}'; listener is started lazily on first use.")]
            public static partial void Constructed(ILogger logger, string host);

            [LoggerMessage(EventId = 91301, Level = LogLevel.Information,
                Message = "[Cluster] WebApi message bus started for node '{Host}'.")]
            public static partial void Started(ILogger logger, string host);

            [LoggerMessage(EventId = 91302, Level = LogLevel.Error,
                Message = "[Cluster] WebApi accept loop faulted; no further inbound peer requests will be accepted.")]
            public static partial void AcceptLoopFaulted(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91303, Level = LogLevel.Warning,
                Message = "[Cluster] A background WebApi task faulted during dispose.")]
            public static partial void BackgroundTaskFaultedDuringDispose(ILogger logger, Exception exception);
        }
    }
}
