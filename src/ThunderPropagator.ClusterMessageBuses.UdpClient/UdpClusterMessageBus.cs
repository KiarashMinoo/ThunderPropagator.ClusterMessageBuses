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

namespace ThunderPropagator.ClusterMessageBuses.UdpClient
{
    /// <summary>
    /// Raw-UDP-backed <see cref="IClusterMessageBus"/>: a discovery-based direct-peer-messaging
    /// transport, structurally distinct from every other transport in this repo because UDP is
    /// connectionless and delivery is not guaranteed. Every node binds exactly one shared
    /// <see cref="IUdpClusterSocket"/> on <see cref="UdpClusterMessageBusOptions.Port"/> — there is
    /// no per-peer connection to open or cache (unlike WebSocket/TcpSocket) and no per-peer client
    /// pool needed (unlike WebApi's stateless-but-still-TCP-backed <c>HttpClient</c>): the one socket
    /// both sends to and receives from every peer.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Peers are supplied by <see cref="IClusterNodeDiscovery"/> (this transport does not implement
    /// discovery itself). Fan-out and subscription-event publishes are sent as a single best-effort
    /// datagram per peer — already consistent with every other transport's "one unreachable peer
    /// must not fail the whole publish" rule, but here a peer can also silently drop a datagram that
    /// technically "sent" successfully, which fan-out/subscription-event simply accept as an
    /// inherent property of best-effort delivery (a later fan-out/resync will eventually catch up
    /// any node that missed one).
    /// </para>
    /// <para>
    /// The three leader/peer-pull request/reply operations cannot tolerate silent drops the same
    /// way, so <see cref="SendRequestAsync"/> layers its own reliability on top of raw UDP: it
    /// resends the request datagram on a timer (<see cref="UdpClusterMessageBusOptions.ResendInterval"/>)
    /// until either a matching <see cref="UdpClusterResponseEnvelope"/> arrives or the overall
    /// <see cref="UdpClusterMessageBusOptions.RequestTimeout"/> elapses — no other transport in this
    /// repo needs to resend the request itself, since TCP/WebSocket/every broker already guarantee
    /// in-order, exactly-once delivery of whatever they did manage to send.
    /// </para>
    /// <para>
    /// A UDP "connection" doesn't exist for the answering side to reply over either, unlike
    /// WebSocket/TcpSocket's bidirectional connections — instead, the answering side sends its
    /// response datagram directly back to whatever <see cref="IPEndPoint"/> the request datagram was
    /// received from (captured by the receive loop itself, not carried in the request payload).
    /// </para>
    /// </remarks>
    internal sealed partial class UdpClusterMessageBus : AbstractClusterMessageBus, IAsyncDisposable
    {
        private readonly UdpClusterMessageBusOptions _options;
        private readonly Uri _nodeEndpoint;
        private readonly IClusterChannelResolver _channelResolver;
        private readonly IClusterNodeDiscovery _discovery;
        private readonly ILogger _logger;
        private readonly Guid _selfId = Guid.NewGuid();

        /// <summary>This node's process-lifetime self-echo identifier. Exposed for tests only.</summary>
        internal Guid SelfId => _selfId;

        private readonly Func<CancellationToken, Task<IUdpClusterSocket>> _socketFactory;
        private readonly Func<Uri, CancellationToken, Task<IPEndPoint>> _peerEndpointResolver;

        private readonly SemaphoreSlim _initLock = new(1, 1);
        private volatile bool _initialized;
        private IUdpClusterSocket? _socket;

        private readonly ConcurrentDictionary<Guid, Func<ClusterFanOutMessage, CancellationToken, Task>> _fanOutHandlers = new();
        private readonly ConcurrentDictionary<Guid, Func<ClusterSubscriptionEvent, CancellationToken, Task>> _subscriptionEventHandlers = new();
        private readonly ConcurrentDictionary<Guid, TaskCompletionSource<UdpClusterResponseEnvelope>> _pendingRequests = new();

        private readonly ConcurrentBag<Task> _backgroundTasks = new();
        private readonly CancellationTokenSource _lifetimeCts = new();

        public UdpClusterMessageBus(
            IOptions<UdpClusterMessageBusOptions> options,
            ClusterConfiguration clusterConfiguration,
            IClusterChannelResolver channelResolver,
            IClusterNodeDiscovery discovery,
            ILoggerFactory loggerFactory,
            Func<CancellationToken, Task<IUdpClusterSocket>>? socketFactory = null,
            Func<Uri, CancellationToken, Task<IPEndPoint>>? peerEndpointResolver = null)
        {
            _options = options.Value;
            _nodeEndpoint = clusterConfiguration.NodeEndpoint
                ?? throw new InvalidOperationException(
                    $"{nameof(ClusterConfiguration)}.{nameof(ClusterConfiguration.NodeEndpoint)} must be set for " +
                    $"{nameof(UdpClusterMessageBus)} to identify this node to peers.");
            _channelResolver = channelResolver;
            _discovery = discovery;
            _logger = loggerFactory.CreateLogger<UdpClusterMessageBus>();
            _socketFactory = socketFactory ?? DefaultSocketFactoryAsync;
            _peerEndpointResolver = peerEndpointResolver ?? ((peer, ct) => UdpAddressing.ResolveEndpointAsync(peer, _options.Port, ct));

            Log.Constructed(_logger, _nodeEndpoint.Host);
        }

        private Task<IUdpClusterSocket> DefaultSocketFactoryAsync(CancellationToken cancellationToken)
        {
            IUdpClusterSocket socket = new UdpClientClusterSocket(_options.Port);
            return Task.FromResult(socket);
        }

        /// <summary>
        /// Lazily binds this node's single shared socket and starts its background receive loop
        /// exactly once. Internal (rather than private) so tests can await it directly against a
        /// substitute <see cref="IUdpClusterSocket"/> before exercising the bus.
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

                _initialized = true;
                Log.Started(_logger, _nodeEndpoint.Host);
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
        internal async Task RunReceiveLoopAsync(IUdpClusterSocket socket, CancellationToken cancellationToken)
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

        internal async Task DispatchDatagramAsync(UdpReceivedDatagram datagram, CancellationToken cancellationToken)
        {
            UdpClusterFrame? frame;
            try
            {
                frame = System.Text.Encoding.UTF8.GetString(datagram.Payload)
                    .FromNJson<UdpClusterFrame>();
            }
            catch (Exception exception)
            {
                Log.FrameUnparseable(_logger, exception);
                return;
            }

            if (frame is null)
                return;

            switch (frame.Kind)
            {
                case UdpClusterFrameKind.FanOut:
                    await HandleFanOutDeliveryAsync(frame.PayloadJson, cancellationToken).ConfigureAwait(false);
                    break;
                case UdpClusterFrameKind.SubscriptionEvent:
                    await HandleSubscriptionEventDeliveryAsync(frame.PayloadJson, cancellationToken).ConfigureAwait(false);
                    break;
                case UdpClusterFrameKind.Request:
                    await HandleRequestFrameAsync(
                        frame.PayloadJson,
                        datagram.RemoteEndPoint,
                        (responseFrame, remoteEndpoint, ct) => SendFrameAsync(responseFrame, remoteEndpoint, ct),
                        cancellationToken).ConfigureAwait(false);
                    break;
                case UdpClusterFrameKind.Response:
                    await HandleResponseDeliveryAsync(frame.PayloadJson, cancellationToken).ConfigureAwait(false);
                    break;
                default:
                    Log.UnknownFrameKind(_logger, frame.Kind.ToString());
                    break;
            }
        }

        /// <summary>
        /// Serializes and sends one <see cref="UdpClusterFrame"/> as a single datagram to
        /// <paramref name="remoteEndpoint"/>, over this node's shared socket. Internal (rather than
        /// private) so tests can invoke it directly against a substitute socket.
        /// </summary>
        internal async Task SendFrameAsync(UdpClusterFrame frame, IPEndPoint remoteEndpoint, CancellationToken cancellationToken)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var json = frame.ToNJson();
            var bytes = System.Text.Encoding.UTF8.GetBytes(json);

            if (bytes.Length > _options.MaxDatagramSize)
            {
                Log.DatagramTooLarge(_logger, bytes.Length, _options.MaxDatagramSize);
                throw new InvalidOperationException(
                    $"Serialized UDP cluster frame is {bytes.Length} bytes, exceeding the configured " +
                    $"{nameof(UdpClusterMessageBusOptions.MaxDatagramSize)} of {_options.MaxDatagramSize} bytes.");
            }

            await _socket!.SendDatagramAsync(bytes, remoteEndpoint, cancellationToken).ConfigureAwait(false);
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
            [LoggerMessage(EventId = 91500, Level = LogLevel.Debug,
                Message = "[Cluster] UDP message bus constructed for node '{Host}'; socket is bound lazily on first use.")]
            public static partial void Constructed(ILogger logger, string host);

            [LoggerMessage(EventId = 91501, Level = LogLevel.Information,
                Message = "[Cluster] UDP message bus started for node '{Host}'.")]
            public static partial void Started(ILogger logger, string host);

            [LoggerMessage(EventId = 91502, Level = LogLevel.Error,
                Message = "[Cluster] UDP receive loop faulted; no further datagrams will be processed.")]
            public static partial void ReceiveLoopFaulted(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91503, Level = LogLevel.Warning,
                Message = "[Cluster] UDP datagram could not be parsed as a cluster frame; skipping it.")]
            public static partial void FrameUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91504, Level = LogLevel.Warning,
                Message = "[Cluster] UDP frame with unknown kind '{Kind}' was ignored.")]
            public static partial void UnknownFrameKind(ILogger logger, string kind);

            [LoggerMessage(EventId = 91505, Level = LogLevel.Error,
                Message = "[Cluster] Serialized UDP cluster frame of {ActualBytes} bytes exceeds the configured MaxDatagramSize of {MaxBytes} bytes.")]
            public static partial void DatagramTooLarge(ILogger logger, int actualBytes, int maxBytes);

            [LoggerMessage(EventId = 91506, Level = LogLevel.Warning,
                Message = "[Cluster] A background UDP task faulted during dispose.")]
            public static partial void BackgroundTaskFaultedDuringDispose(ILogger logger, Exception exception);
        }
    }
}
