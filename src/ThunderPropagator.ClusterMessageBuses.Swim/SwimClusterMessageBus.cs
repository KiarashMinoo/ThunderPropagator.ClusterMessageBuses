using System.Collections.Concurrent;
using System.Net;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using ThunderPropagator.Application.Channels.Cluster;
using ThunderPropagator.Application.Channels.Cluster.Discovery;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.ClusterMessageBuses.Swim
{
    /// <summary>
    /// SWIM-protocol gossip-backed <see cref="IClusterMessageBus"/>: reuses this repo's raw-UDP
    /// socket plumbing (see the sibling <c>UdpClusterMessageBus</c>, this transport's closest
    /// structural relative) but replaces its direct-unicast-to-every-peer fan-out with genuine
    /// epidemic (gossip) dissemination, and adds SWIM's own scalable failure detector on top.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Failure detection.</b> Every <see cref="SwimClusterMessageBusOptions.GossipInterval"/>, the
    /// SWIM probe loop picks one random peer it currently believes <see cref="SwimMemberState.Alive"/>
    /// and sends it a direct <see cref="SwimMessageKind.Ping"/>. If no <see cref="SwimMessageKind.Ack"/>
    /// arrives within <see cref="SwimClusterMessageBusOptions.ProbeTimeout"/>, this node asks
    /// <see cref="SwimClusterMessageBusOptions.IndirectProbeRelayCount"/> other random peers to probe
    /// the same target on its behalf (<see cref="SwimMessageKind.PingReq"/>) -- each relay pings the
    /// target itself and forwards back any ack it gets. Only if that also produces nothing does this
    /// node mark the target <see cref="SwimMemberState.Suspect"/> rather than immediately
    /// <see cref="SwimMemberState.Dead"/>, specifically to absorb a transient hiccup or a one-way
    /// partition that affects this node but not the relays. A member that hears itself reported
    /// Suspect/Dead refutes the claim by gossiping a fresh <see cref="SwimMemberState.Alive"/> update
    /// at a strictly higher incarnation number; a Suspect that goes unrefuted past
    /// <see cref="SwimClusterMessageBusOptions.SuspicionTimeout"/> is declared
    /// <see cref="SwimMemberState.Dead"/>. This mirrors the original SWIM paper and, more directly,
    /// HashiCorp's <c>memberlist</c> library (the production Go implementation backing Serf and
    /// Consul) -- this transport's defaults (200ms gossip interval, retransmit multiplier of 4, ...)
    /// intentionally match <c>memberlist</c>'s own.
    /// </para>
    /// <para>
    /// <b>Piggybacked dissemination.</b> This cluster's actual payloads
    /// (<see cref="ClusterFanOutMessage"/>, <see cref="ClusterSubscriptionEvent"/>,
    /// <see cref="ClusterByteMessage"/>) are never unicast to every peer the way every other
    /// transport in this repo (including the raw-UDP one) sends them. <c>PublishAsync</c> only
    /// stamps the message and enqueues it into a bounded <see cref="SwimBroadcastQueue{T}"/> (see its
    /// own doc comment); the message actually leaves this node only when the gossip-round loop's
    /// periodic dissemination, or the probe loop's own Ping/PingReq/Ack traffic, happens to carry it.
    /// A second, independent <see cref="SwimBroadcastQueue{T}"/> instance carries membership updates
    /// the same way. Each queued item is retransmitted only <c>ceil(RetransmitMult * log10(N+1))</c>
    /// times (N = known member count) before this node stops forwarding it, on the assumption that by
    /// then it has reached the whole cluster -- <c>memberlist</c>'s own <c>TransmitLimitedQueue</c>
    /// formula, with its own default multiplier of 4. Any node that receives a not-yet-seen item
    /// (standalone or piggybacked) re-queues it into its own outgoing queue before delivering it
    /// locally -- see e.g. <c>SwimClusterMessageBus.FanOut.cs</c>'s <c>HandleFanOutDeliveryAsync</c> --
    /// which is what actually makes dissemination epidemic rather than single-hop.
    /// </para>
    /// <para>
    /// <b>Tradeoff.</b> This makes fan-out delivery both eventually consistent and materially
    /// higher-latency than every other transport in this repo's direct-unicast-to-everyone delivery:
    /// a message needs several gossip rounds (each <see cref="SwimClusterMessageBusOptions.GossipInterval"/>
    /// apart) to plausibly reach the whole cluster, not one round trip. Swim is therefore the right
    /// choice for large clusters where direct unicast-to-everyone stops scaling, and for
    /// control-plane traffic that can tolerate that latency -- e.g. leader-endpoint announcements,
    /// cluster-wide configuration changes -- and deliberately not the right choice for
    /// latency-critical processed-data fan-out, which should stay on a transport (UDP, TCP,
    /// WebSocket, a broker) that delivers in close to one hop.
    /// </para>
    /// <para>
    /// The three leader/peer-pull operations (<see cref="RestoreFromLeaderAsync"/>,
    /// <see cref="SyncDeltaFromLeaderAsync"/>, <see cref="FetchPeerSubscriptionsAsync"/>) and the
    /// byte-oriented <see cref="PullSnapshotAsync"/> are point-to-point pulls against a specific
    /// known leader/peer, not general cluster knowledge -- gossiping them would make no sense, so
    /// they never are. Instead they reuse the same hand-rolled, resend-on-timer direct-unicast
    /// request/reply pattern <c>UdpClusterMessageBus.RequestReply.cs</c> already established, since
    /// plain UDP still gives no delivery guarantee for them either.
    /// </para>
    /// </remarks>
    internal sealed partial class SwimClusterMessageBus : AbstractClusterMessageBus, IAsyncDisposable
    {
        private readonly SwimClusterMessageBusOptions _options;
        private readonly Uri _nodeEndpoint;
        private readonly string _selfHost;
        private readonly IClusterChannelResolver _channelResolver;
        private readonly IClusterNodeDiscovery _discovery;
        private readonly ILogger _logger;
        private readonly Guid _selfId = Guid.NewGuid();

        /// <summary>This node's process-lifetime self-echo identifier. Exposed for tests only.</summary>
        internal Guid SelfId => _selfId;

        private readonly Func<CancellationToken, Task<ISwimClusterSocket>> _socketFactory;
        private readonly Func<string, CancellationToken, Task<IPEndPoint>> _peerEndpointResolver;

        private readonly SemaphoreSlim _initLock = new(1, 1);
        private volatile bool _initialized;
        private ISwimClusterSocket? _socket;

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

        /// <summary>This node's SWIM view of every peer's liveness -- see <see cref="SwimMembershipTable"/>'s own doc comment.</summary>
        private readonly SwimMembershipTable _membershipTable;

        /// <summary>Exposed for tests only.</summary>
        internal SwimMembershipTable MembershipTable => _membershipTable;

        /// <summary>Not-yet-fully-disseminated application payloads -- see this class's own remarks.</summary>
        private readonly SwimBroadcastQueue<SwimDatagram> _appBroadcastQueue;

        /// <summary>Not-yet-fully-disseminated membership updates -- see this class's own remarks.</summary>
        private readonly SwimBroadcastQueue<SwimMembershipUpdate> _membershipBroadcastQueue;

        private readonly object _incarnationLock = new();
        private long _selfIncarnation;

        private readonly ConcurrentDictionary<Guid, TaskCompletionSource<bool>> _pendingAcks = new();
        private readonly ConcurrentDictionary<Guid, IPEndPoint> _relayForwardTargets = new();

        private readonly ConcurrentBag<Task> _backgroundTasks = new();
        private readonly CancellationTokenSource _lifetimeCts = new();

        public SwimClusterMessageBus(
            IOptions<SwimClusterMessageBusOptions> options,
            ClusterConfiguration clusterConfiguration,
            IClusterChannelResolver channelResolver,
            IClusterNodeDiscovery discovery,
            ILoggerFactory loggerFactory,
            Func<CancellationToken, Task<ISwimClusterSocket>>? socketFactory = null,
            Func<string, CancellationToken, Task<IPEndPoint>>? peerEndpointResolver = null,
            IClusterByteSnapshotProvider? byteSnapshotProvider = null)
        {
            _options = options.Value;
            _nodeEndpoint = clusterConfiguration.NodeEndpoint
                ?? throw new InvalidOperationException(
                    $"{nameof(ClusterConfiguration)}.{nameof(ClusterConfiguration.NodeEndpoint)} must be set for " +
                    $"{nameof(SwimClusterMessageBus)} to identify this node to peers.");
            _selfHost = _nodeEndpoint.Host;
            _channelResolver = channelResolver;
            _discovery = discovery;
            _logger = loggerFactory.CreateLogger<SwimClusterMessageBus>();
            _socketFactory = socketFactory ?? DefaultSocketFactoryAsync;
            _peerEndpointResolver = peerEndpointResolver ?? ((host, ct) => SwimAddressing.ResolveEndpointAsync(host, _options.Port, ct));
            _byteSnapshotProvider = byteSnapshotProvider;

            _membershipTable = new SwimMembershipTable(_selfHost);
            _appBroadcastQueue = new SwimBroadcastQueue<SwimDatagram>(_options.MaxBroadcastQueueSize);
            _membershipBroadcastQueue = new SwimBroadcastQueue<SwimMembershipUpdate>(_options.MaxBroadcastQueueSize);

            Log.Constructed(_logger, _selfHost);
        }

        private Task<ISwimClusterSocket> DefaultSocketFactoryAsync(CancellationToken cancellationToken)
        {
            ISwimClusterSocket socket = new UdpSwimClusterSocket(_options.Port);
            return Task.FromResult(socket);
        }

        /// <summary>
        /// Lazily binds this node's single shared socket and starts its background receive loop, the
        /// gossip-round loop, and the SWIM probe loop, exactly once. Internal (rather than private)
        /// so tests can await it directly against a substitute <see cref="ISwimClusterSocket"/>
        /// before exercising the bus.
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

                _socket = await _socketFactory(cancellationToken).ConfigureAwait(false);
                _backgroundTasks.Add(RunReceiveLoopAsync(_socket, _lifetimeCts.Token));
                _backgroundTasks.Add(RunGossipRoundLoopAsync(_lifetimeCts.Token));
                _backgroundTasks.Add(RunProbeLoopAsync(_lifetimeCts.Token));

                _initialized = true;
                Log.Started(_logger, _selfHost);
            }
            finally
            {
                _initLock.Release();
            }
        }

        /// <summary>
        /// Reads and dispatches every datagram off this node's shared socket until the lifetime
        /// token is cancelled. Internal (rather than private) so tests can drive it directly against
        /// a substitute socket.
        /// </summary>
        internal async Task RunReceiveLoopAsync(ISwimClusterSocket socket, CancellationToken cancellationToken)
        {
            try
            {
                await foreach (var datagram in socket.ReceiveDatagramsAsync(cancellationToken).ConfigureAwait(false))
                {
                    await DispatchDatagramAsync(datagram, cancellationToken).ConfigureAwait(false);
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

        internal async Task DispatchDatagramAsync(SwimReceivedDatagram datagram, CancellationToken cancellationToken)
        {
            SwimDatagram? envelope;
            try
            {
                envelope = System.Text.Encoding.UTF8.GetString(datagram.Payload)
                    .FromNJson<SwimDatagram>();
            }
            catch (Exception exception)
            {
                Log.DatagramUnparseable(_logger, exception);
                return;
            }

            if (envelope is null)
                return;

            switch (envelope.Kind)
            {
                case SwimMessageKind.Ping:
                    await HandlePingAsync(envelope.PayloadJson, datagram.RemoteEndPoint, cancellationToken).ConfigureAwait(false);
                    break;
                case SwimMessageKind.PingReq:
                    await HandlePingReqAsync(envelope.PayloadJson, datagram.RemoteEndPoint, cancellationToken).ConfigureAwait(false);
                    break;
                case SwimMessageKind.Ack:
                    await HandleAckAsync(envelope.PayloadJson, cancellationToken).ConfigureAwait(false);
                    break;
                case SwimMessageKind.GossipFanOut:
                    await HandleFanOutDeliveryAsync(envelope.PayloadJson, cancellationToken).ConfigureAwait(false);
                    break;
                case SwimMessageKind.GossipSubscriptionEvent:
                    await HandleSubscriptionEventDeliveryAsync(envelope.PayloadJson, cancellationToken).ConfigureAwait(false);
                    break;
                case SwimMessageKind.GossipByteFanOut:
                    await HandleByteFanOutDeliveryAsync(envelope.PayloadJson, cancellationToken).ConfigureAwait(false);
                    break;
                case SwimMessageKind.Request:
                    await HandleRequestFrameAsync(
                        envelope.PayloadJson,
                        datagram.RemoteEndPoint,
                        (responseDatagram, remoteEndpoint, ct) => SendDatagramAsync(responseDatagram, remoteEndpoint, ct),
                        cancellationToken).ConfigureAwait(false);
                    break;
                case SwimMessageKind.Response:
                    await HandleResponseDeliveryAsync(envelope.PayloadJson, cancellationToken).ConfigureAwait(false);
                    break;
                default:
                    Log.UnknownDatagramKind(_logger, envelope.Kind.ToString());
                    break;
            }
        }

        /// <summary>
        /// Serializes and sends one <see cref="SwimDatagram"/> as a single datagram to
        /// <paramref name="remoteEndpoint"/>, over this node's shared socket. Internal (rather than
        /// private) so tests can invoke it directly against a substitute socket.
        /// </summary>
        internal async Task SendDatagramAsync(SwimDatagram datagram, IPEndPoint remoteEndpoint, CancellationToken cancellationToken)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var json = datagram.ToNJson();
            var bytes = System.Text.Encoding.UTF8.GetBytes(json);

            if (bytes.Length > _options.MaxDatagramSize)
            {
                Log.DatagramTooLarge(_logger, bytes.Length, _options.MaxDatagramSize);
                throw new InvalidOperationException(
                    $"Serialized SWIM cluster datagram is {bytes.Length} bytes, exceeding the configured " +
                    $"{nameof(SwimClusterMessageBusOptions.MaxDatagramSize)} of {_options.MaxDatagramSize} bytes.");
            }

            await _socket!.SendDatagramAsync(bytes, remoteEndpoint, cancellationToken).ConfigureAwait(false);
        }

        public async ValueTask DisposeAsync()
        {
            await _lifetimeCts.CancelAsync().ConfigureAwait(false);

            _pendingRequests.CancelAll();

            foreach (var pendingAck in _pendingAcks.Values)
            {
                pendingAck.TrySetCanceled();
            }

            _fanOutHandlers.Clear();
            _subscriptionEventHandlers.Clear();
            _byteFanOutHandlers.Clear();
            _relayForwardTargets.Clear();

            if (_socket is not null)
            {
                await _socket.DisposeAsync().ConfigureAwait(false);
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
            [LoggerMessage(EventId = 91600, Level = LogLevel.Debug,
                Message = "[Cluster] SWIM message bus constructed for node '{Host}'; socket is bound lazily on first use.")]
            public static partial void Constructed(ILogger logger, string host);

            [LoggerMessage(EventId = 91601, Level = LogLevel.Information,
                Message = "[Cluster] SWIM message bus started for node '{Host}'.")]
            public static partial void Started(ILogger logger, string host);

            [LoggerMessage(EventId = 91602, Level = LogLevel.Error,
                Message = "[Cluster] SWIM receive loop faulted; no further datagrams will be processed.")]
            public static partial void ReceiveLoopFaulted(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91603, Level = LogLevel.Warning,
                Message = "[Cluster] SWIM datagram could not be parsed as a cluster envelope; skipping it.")]
            public static partial void DatagramUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91604, Level = LogLevel.Warning,
                Message = "[Cluster] SWIM datagram with unknown kind '{Kind}' was ignored.")]
            public static partial void UnknownDatagramKind(ILogger logger, string kind);

            [LoggerMessage(EventId = 91605, Level = LogLevel.Error,
                Message = "[Cluster] Serialized SWIM cluster datagram of {ActualBytes} bytes exceeds the configured MaxDatagramSize of {MaxBytes} bytes.")]
            public static partial void DatagramTooLarge(ILogger logger, int actualBytes, int maxBytes);

            [LoggerMessage(EventId = 91606, Level = LogLevel.Warning,
                Message = "[Cluster] A background SWIM task faulted during dispose.")]
            public static partial void BackgroundTaskFaultedDuringDispose(ILogger logger, Exception exception);
        }
    }

    /// <summary>
    /// Generic, thread-safe, retransmit-limited broadcast queue -- the piggyback-dissemination
    /// engine behind this transport's infection-style gossip, modeled directly on HashiCorp
    /// <c>memberlist</c>'s <c>TransmitLimitedQueue</c>. <see cref="SwimClusterMessageBus"/> keeps two
    /// independent instances: one queuing <see cref="SwimDatagram"/> application-payload gossip items
    /// (fan-out/subscription-event/byte-fan-out), one queuing <see cref="SwimMembershipUpdate"/>
    /// liveness changes -- each item is retransmitted only until it has likely reached the whole
    /// cluster, then evicted, bounding both how long stale information keeps circulating and how much
    /// memory the queue itself uses.
    /// </summary>
    /// <typeparam name="T">The queued item's own (wire-serializable) payload type.</typeparam>
    internal sealed class SwimBroadcastQueue<T>
    {
        private readonly object _lock = new();
        private readonly List<Entry> _items = new();
        private readonly int _maxSize;

        internal SwimBroadcastQueue(int maxSize)
        {
            _maxSize = maxSize;
        }

        /// <summary>
        /// Queues <paramref name="payload"/> for dissemination, starting its retransmit count at
        /// zero. If the queue is already at <c>maxSize</c>, evicts whichever currently-queued item
        /// has been retransmitted the most (i.e. the one most likely already fully disseminated) to
        /// make room -- bounding memory without needing to know in advance which item that will turn
        /// out to be.
        /// </summary>
        internal void Enqueue(T payload)
        {
            lock (_lock)
            {
                _items.Add(new Entry(payload));

                if (_items.Count > _maxSize)
                {
                    var mostTransmitted = _items.OrderByDescending(e => e.TransmitCount).First();
                    _items.Remove(mostTransmitted);
                }
            }
        }

        /// <summary>
        /// Returns up to <paramref name="maxItems"/> queued items, least-retransmitted first (so
        /// newer/less-disseminated information is prioritized over items that have already gone out
        /// several times), and increments each returned item's retransmit counter. Any item whose
        /// counter reaches <c>ceil(retransmitMult * log10(knownMemberCount + 1))</c> -- the same
        /// formula <c>memberlist</c> uses -- is retired from the queue after this call, since by then
        /// it has almost certainly reached every member.
        /// </summary>
        internal IReadOnlyList<T> TakeBatch(int maxItems, int knownMemberCount, int retransmitMult)
        {
            var limit = ComputeRetransmitLimit(knownMemberCount, retransmitMult);

            lock (_lock)
            {
                var selected = _items.OrderBy(e => e.TransmitCount).Take(maxItems).ToList();
                var result = new List<T>(selected.Count);

                foreach (var entry in selected)
                {
                    result.Add(entry.Payload);
                    entry.TransmitCount++;
                }

                _items.RemoveAll(e => e.TransmitCount >= limit);

                return result;
            }
        }

        private static int ComputeRetransmitLimit(int knownMemberCount, int retransmitMult)
        {
            var limit = (int)Math.Ceiling(retransmitMult * Math.Log10(knownMemberCount + 1));
            return Math.Max(limit, 1);
        }

        private sealed class Entry
        {
            internal Entry(T payload)
            {
                Payload = payload;
            }

            internal T Payload { get; }
            internal int TransmitCount { get; set; }
        }
    }
}
