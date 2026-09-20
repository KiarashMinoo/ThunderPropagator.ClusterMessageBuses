using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using ThunderPropagator.Application.Channels.Cluster;
using ThunderPropagator.Application.Channels.Cluster.Discovery;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.Srd
{
    /// <summary>
    /// Native-libfabric-backed <see cref="IClusterMessageBus"/>, over AWS's Elastic Fabric Adapter
    /// (EFA) using its Scalable Reliable Datagram (SRD) transport via libfabric's <c>efa</c> provider
    /// and RDM (Reliable Datagram Messaging) endpoint type — NOT via libibverbs, and structurally
    /// distinct from RDMA RC (Reliable Connected) queue-pair transports: SRD/RDM is connectionless
    /// (closer in spirit to UDP) rather than connection-oriented.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>THIS TRANSPORT HAS NOT BEEN BUILT, LINKED, OR RUN.</b> It was written in an environment with
    /// no dotnet SDK, no C compiler, and no EFA hardware available at all. It is a best-effort
    /// implementation against documented native libfabric APIs, structured to mirror this repo's
    /// existing transports as closely as EFA/SRD's semantics allow — it needs full validation on a
    /// real AWS EC2 instance with an EFA attached before it can be trusted. See
    /// <c>Native/LibFabricNativeMethods.cs</c>'s banner, <c>Endpoint/SrdAddressing.cs</c>'s remarks,
    /// and this project's DI extension (<see cref="SrdClusterMessageBusExtensions"/>) for
    /// the full list of what specifically needs checking.
    /// </para>
    /// <para>
    /// Structurally this mirrors <c>UdpClusterMessageBus</c>, not <c>TcpClusterMessageBus</c>: every
    /// node binds exactly one shared <see cref="ISrdClusterEndpoint"/> — there is no per-peer
    /// connection to open or cache, because libfabric's RDM endpoint type is connectionless, just like
    /// UDP. Peers (supplied by <see cref="IClusterNodeDiscovery"/>, same as every other transport in
    /// this repo) are resolved once into an address-vector handle rather than requiring a persistent
    /// per-peer connection handshake.
    /// </para>
    /// <para>
    /// Unlike UDP, though, EFA's SRD transport <em>is</em> reliable — so unlike
    /// <c>UdpClusterMessageBus</c>, this transport does not resend requests on a timer to compensate
    /// for possible silent drops. The three (four, counting <c>PullSnapshotAsync</c>) leader/peer-pull
    /// request/reply operations instead layer
    /// <see cref="ThunderPropagator.ClusterMessageBuses.SharedKernel.ClusterResiliencePipelineFactory"/>'s
    /// retry/circuit-breaker policy on top of a single send, the same way the connection-oriented
    /// transports (TcpSocket, WebSocket) do — see <c>SrdClusterMessageBus.RequestReply.cs</c>.
    /// </para>
    /// <para>
    /// Also unlike UDP, an unreachable/unresolvable peer during fan-out is logged as a real,
    /// surfacing-worthy failure (<see cref="LogLevel.Warning"/>) rather than an accepted "best-effort,
    /// no guarantee" drop — SRD's reliability means a send failure here reflects an actual problem
    /// (misconfigured peer, native call failure), not routine best-effort delivery semantics.
    /// </para>
    /// </remarks>
    internal sealed partial class SrdClusterMessageBus : AbstractClusterMessageBus, IAsyncDisposable
    {
        private readonly SrdClusterMessageBusOptions _options;
        private readonly Uri _nodeEndpoint;
        private readonly IClusterChannelResolver _channelResolver;
        private readonly IClusterNodeDiscovery _discovery;
        private readonly ILogger _logger;
        private readonly Guid _selfId = Guid.NewGuid();

        /// <summary>This node's process-lifetime self-echo identifier. Exposed for tests only.</summary>
        internal Guid SelfId => _selfId;

        private readonly Func<CancellationToken, Task<ISrdClusterEndpoint>> _endpointFactory;

        private readonly SemaphoreSlim _initLock = new(1, 1);
        private volatile bool _initialized;
        private ISrdClusterEndpoint? _endpoint;

        private readonly ClusterHandlerRegistry<ClusterFanOutMessage> _fanOutHandlers = new();
        private readonly ClusterHandlerRegistry<ClusterSubscriptionEvent> _subscriptionEventHandlers = new();
        private readonly ClusterHandlerRegistry<ClusterByteMessage> _byteFanOutHandlers = new();
        private readonly PendingRequestTracker<ClusterResponseEnvelope> _pendingRequests = new();

        /// <summary>
        /// Answers this node's own <see cref="ClusterRequestKind.PullSnapshotBytes"/> requests --
        /// see <see cref="IClusterByteSnapshotProvider"/>'s own doc comment. Left unregistered by a
        /// channel-based consumer (nothing here requires it); a non-channel consumer registers its
        /// own implementation.
        /// </summary>
        private readonly IClusterByteSnapshotProvider? _byteSnapshotProvider;

        private readonly ConcurrentBag<Task> _backgroundTasks = new();
        private readonly CancellationTokenSource _lifetimeCts = new();

        public SrdClusterMessageBus(
            IOptions<SrdClusterMessageBusOptions> options,
            ClusterConfiguration clusterConfiguration,
            IClusterChannelResolver channelResolver,
            IClusterNodeDiscovery discovery,
            ILoggerFactory loggerFactory,
            Func<CancellationToken, Task<ISrdClusterEndpoint>>? endpointFactory = null,
            IClusterByteSnapshotProvider? byteSnapshotProvider = null)
        {
            _options = options.Value;
            _nodeEndpoint = clusterConfiguration.NodeEndpoint
                ?? throw new InvalidOperationException(
                    $"{nameof(ClusterConfiguration)}.{nameof(ClusterConfiguration.NodeEndpoint)} must be set for " +
                    $"{nameof(SrdClusterMessageBus)} to identify this node to peers.");
            _channelResolver = channelResolver;
            _discovery = discovery;
            _logger = loggerFactory.CreateLogger<SrdClusterMessageBus>();
            _endpointFactory = endpointFactory ?? DefaultEndpointFactoryAsync;
            _byteSnapshotProvider = byteSnapshotProvider;

            Log.Constructed(_logger, _nodeEndpoint.Host);
        }

        private Task<ISrdClusterEndpoint> DefaultEndpointFactoryAsync(CancellationToken cancellationToken)
        {
            ISrdClusterEndpoint endpoint = new LibFabricSrdClusterEndpoint(_options, _logger);
            return Task.FromResult(endpoint);
        }

        /// <summary>
        /// Lazily binds this node's single shared libfabric endpoint and starts its background receive
        /// loop exactly once. Internal (rather than private) so tests can await it directly against a
        /// substitute <see cref="ISrdClusterEndpoint"/> before exercising the bus.
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

                _endpoint = await _endpointFactory(cancellationToken).ConfigureAwait(false);
                _backgroundTasks.Add(RunReceiveLoopAsync(_endpoint, _lifetimeCts.Token));

                _initialized = true;
                Log.Started(_logger, _nodeEndpoint.Host);
            }
            finally
            {
                _initLock.Release();
            }
        }

        /// <summary>
        /// Reads and dispatches every frame off this node's shared endpoint until the lifetime token
        /// is cancelled. Internal (rather than private) so tests can drive it directly against a
        /// substitute endpoint.
        /// </summary>
        internal async Task RunReceiveLoopAsync(ISrdClusterEndpoint endpoint, CancellationToken cancellationToken)
        {
            try
            {
                await foreach (var frame in endpoint.ReceiveFramesAsync(cancellationToken).ConfigureAwait(false))
                {
                    await DispatchFrameAsync(frame, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Expected on shutdown.
            }
            catch (Exception exception)
            {
                Log.ReceiveLoopFaulted(_logger, exception);
            }
        }

        internal async Task DispatchFrameAsync(SrdClusterFrame frame, CancellationToken cancellationToken)
        {
            switch (frame.Kind)
            {
                case SrdClusterFrameKind.FanOut:
                    await HandleFanOutDeliveryAsync(frame.PayloadJson, cancellationToken).ConfigureAwait(false);
                    break;
                case SrdClusterFrameKind.SubscriptionEvent:
                    await HandleSubscriptionEventDeliveryAsync(frame.PayloadJson, cancellationToken).ConfigureAwait(false);
                    break;
                case SrdClusterFrameKind.Request:
                    await HandleRequestFrameAsync(
                        frame.PayloadJson,
                        (responseFrame, requesterEndpoint, ct) => _endpoint!.SendFrameAsync(responseFrame, requesterEndpoint, ct),
                        cancellationToken).ConfigureAwait(false);
                    break;
                case SrdClusterFrameKind.Response:
                    await HandleResponseDeliveryAsync(frame.PayloadJson, cancellationToken).ConfigureAwait(false);
                    break;
                case SrdClusterFrameKind.ByteFanOut:
                    await HandleByteFanOutDeliveryAsync(frame.PayloadJson, cancellationToken).ConfigureAwait(false);
                    break;
                default:
                    Log.UnknownFrameKind(_logger, frame.Kind.ToString());
                    break;
            }
        }

        public async ValueTask DisposeAsync()
        {
            await _lifetimeCts.CancelAsync().ConfigureAwait(false);

            _pendingRequests.CancelAll();

            _fanOutHandlers.Clear();
            _subscriptionEventHandlers.Clear();
            _byteFanOutHandlers.Clear();

            if (_endpoint is not null)
            {
                await _endpoint.DisposeAsync().ConfigureAwait(false);
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
            [LoggerMessage(EventId = 91800, Level = LogLevel.Debug,
                Message = "[Cluster] SRD message bus constructed for node '{Host}'; the libfabric endpoint is bound lazily on first use.")]
            public static partial void Constructed(ILogger logger, string host);

            [LoggerMessage(EventId = 91801, Level = LogLevel.Information,
                Message = "[Cluster] SRD message bus started for node '{Host}'.")]
            public static partial void Started(ILogger logger, string host);

            [LoggerMessage(EventId = 91802, Level = LogLevel.Error,
                Message = "[Cluster] SRD receive loop faulted; no further messages will be processed.")]
            public static partial void ReceiveLoopFaulted(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91803, Level = LogLevel.Warning,
                Message = "[Cluster] SRD frame with unknown kind '{Kind}' was ignored.")]
            public static partial void UnknownFrameKind(ILogger logger, string kind);

            [LoggerMessage(EventId = 91804, Level = LogLevel.Warning,
                Message = "[Cluster] A background SRD task faulted during dispose.")]
            public static partial void BackgroundTaskFaultedDuringDispose(ILogger logger, Exception exception);
        }
    }
}
