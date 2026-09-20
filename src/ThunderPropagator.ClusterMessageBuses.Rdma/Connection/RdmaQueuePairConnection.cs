using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Channels;
using ThunderPropagator.BuildingBlocks.Application.Helpers;

namespace ThunderPropagator.ClusterMessageBuses.Rdma
{
    /// <summary>
    /// Production <see cref="IRdmaQueuePairConnection"/>: one established RDMA reliable-connected
    /// (RC) queue pair, driven with two-sided <c>ibv_post_send</c>(<c>IBV_WR_SEND</c>)/<c>ibv_post_recv</c>
    /// work requests rather than one-sided RDMA read/write. Two-sided send/recv is the natural fit
    /// for a message bus: the sender never needs the receiver's memory address ahead of time (which
    /// one-sided RDMA read/write requires exchanging out of band), and -- like UDP, unlike TCP's
    /// raw byte stream -- each posted receive buffer receives exactly one posted send, so message
    /// boundaries are preserved automatically; <see cref="RdmaClusterFrame"/> needs no length-prefix
    /// framing the way <c>TcpClusterFrame</c> does.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Completion notification is event-driven, never busy-polled: one dedicated background thread
    /// per connection blocks on <c>ibv_get_cq_event</c> and, once woken, re-arms notifications
    /// (<c>ibv_req_notify_cq</c>) and drains every ready completion with <c>ibv_poll_cq</c> before
    /// blocking again. A completed send resolves the <see cref="TaskCompletionSource{TResult}"/>
    /// that <see cref="SendFrameAsync"/> is awaiting (sends are serialized through
    /// <see cref="_sendLock"/>, so only one is ever outstanding, needing only one reusable send
    /// buffer/registration); a completed receive is deserialized and written to
    /// <see cref="_receiveChannel"/>, which backs <see cref="ReceiveFramesAsync"/>, and its buffer
    /// is immediately re-posted so the queue pair never starves of receive capacity.
    /// </para>
    /// <para>
    /// Mirrors <c>NetworkStreamTcpClusterConnection</c>'s shape deliberately: no
    /// <see cref="Microsoft.Extensions.Logging.ILogger"/> dependency here (a single malformed
    /// frame or one bad work completion must not take down an otherwise-healthy connection, so
    /// this type recovers silently the same way <c>NetworkStreamTcpClusterConnection</c> does) --
    /// <see cref="RdmaClusterMessageBus"/> is the layer that logs, at the call sites where a
    /// send/receive failure actually needs surfacing to an operator. An unexpected fault on the
    /// completion thread completes <see cref="_receiveChannel"/>'s writer with that exception, so
    /// it surfaces naturally to whichever caller is enumerating <see cref="ReceiveFramesAsync"/> --
    /// exactly like a TCP stream throwing out of <c>ReceiveFramesAsync</c> today.
    /// </para>
    /// <para>
    /// NOT VALIDATED ON REAL HARDWARE -- see the banner comments in <see cref="LibIbVerbsNativeMethods"/>
    /// and <see cref="LibRdmaCmNativeMethods"/>. This class in particular reads two fields directly
    /// out of the deliberately-unbound native <c>struct rdma_cm_id</c> by fixed byte offset
    /// (<see cref="ReadVerbsContext"/>, <see cref="ReadQueuePairHandle"/>) rather than declaring a
    /// full managed struct for it -- see those methods' doc comments for the risk/rationale.
    /// </para>
    /// </remarks>
    internal sealed class RdmaQueuePairConnection : IRdmaQueuePairConnection
    {
        /// <summary><c>wr_id</c> used for every posted send -- disambiguated from receive completions, which use <c>index + 1</c> (see <see cref="PostRecv"/>).</summary>
        private const ulong SendWorkRequestId = 0UL;

        private const short AfInet = 2; // AF_INET on Linux.

        private readonly IntPtr _cmId;
        private readonly IntPtr _eventChannel;
        private readonly bool _ownsEventChannel;
        private readonly IntPtr _verbsContext;
        private readonly IntPtr _protectionDomain;
        private readonly IntPtr _completionChannel;
        private readonly IntPtr _completionQueue;
        private readonly IntPtr _queuePair;

        private readonly IntPtr _sendMr;
        private readonly IntPtr _sendBuffer;
        private readonly int _maxMessageSize;

        private readonly IntPtr[] _recvMrs;
        private readonly IntPtr[] _recvBuffers;

        private readonly SemaphoreSlim _sendLock = new(1, 1);
        private volatile TaskCompletionSource<bool>? _pendingSend;

        private readonly Channel<RdmaClusterFrame> _receiveChannel =
            Channel.CreateUnbounded<RdmaClusterFrame>(new UnboundedChannelOptions { SingleWriter = true });

        private readonly Thread _completionThread;
        private readonly CancellationTokenSource _lifetimeCts = new();
        private volatile bool _disposed;

        private RdmaQueuePairConnection(
            IntPtr cmId,
            IntPtr eventChannel,
            bool ownsEventChannel,
            IntPtr verbsContext,
            IntPtr protectionDomain,
            IntPtr completionChannel,
            IntPtr completionQueue,
            IntPtr queuePair,
            IntPtr sendMr,
            IntPtr sendBuffer,
            int maxMessageSize,
            IntPtr[] recvMrs,
            IntPtr[] recvBuffers)
        {
            _cmId = cmId;
            _eventChannel = eventChannel;
            _ownsEventChannel = ownsEventChannel;
            _verbsContext = verbsContext;
            _protectionDomain = protectionDomain;
            _completionChannel = completionChannel;
            _completionQueue = completionQueue;
            _queuePair = queuePair;
            _sendMr = sendMr;
            _sendBuffer = sendBuffer;
            _maxMessageSize = maxMessageSize;
            _recvMrs = recvMrs;
            _recvBuffers = recvBuffers;

            _completionThread = new Thread(RunCompletionLoop)
            {
                IsBackground = true,
                Name = "Rdma-CompletionThread",
            };
            _completionThread.Start();
        }

        /// <summary>
        /// Active/client side: resolves <paramref name="peerEndpoint"/> over RDMA CM, negotiates a
        /// route, builds the local data-path resources (protection domain, completion
        /// queue/channel, queue pair, registered buffers), then connects and waits for the
        /// <see cref="RdmaCmEventType.Established"/> event before returning.
        /// </summary>
        internal static async Task<IRdmaQueuePairConnection> ConnectAsync(
            IPEndPoint peerEndpoint,
            RdmaClusterMessageBusOptions options,
            CancellationToken cancellationToken)
        {
            if (peerEndpoint.AddressFamily != AddressFamily.InterNetwork)
                throw new NotSupportedException("RdmaQueuePairConnection currently supports IPv4 peer endpoints only.");

            var eventChannel = LibRdmaCmNativeMethods.rdma_create_event_channel();
            if (eventChannel == IntPtr.Zero)
            {
                throw new InvalidOperationException(
                    "rdma_create_event_channel failed -- confirm librdmacm.so.1 is installed and this host has an RDMA-capable device.");
            }

            IntPtr cmId;
            try
            {
                ThrowIfNonZero(
                    LibRdmaCmNativeMethods.rdma_create_id(eventChannel, out cmId, IntPtr.Zero, LibRdmaCmNativeMethods.RdmaPortSpace.Tcp),
                    "rdma_create_id");

                var destination = BuildSockAddrIn(peerEndpoint);
                ThrowIfNonZero(
                    LibRdmaCmNativeMethods.rdma_resolve_addr(cmId, IntPtr.Zero, ref destination, (int)options.ConnectTimeout.TotalMilliseconds),
                    "rdma_resolve_addr");
                await RdmaCmEventReader.WaitForAsync(eventChannel, RdmaCmEventType.AddrResolved, options.ConnectTimeout).ConfigureAwait(false);

                ThrowIfNonZero(
                    LibRdmaCmNativeMethods.rdma_resolve_route(cmId, (int)options.ConnectTimeout.TotalMilliseconds),
                    "rdma_resolve_route");
                await RdmaCmEventReader.WaitForAsync(eventChannel, RdmaCmEventType.RouteResolved, options.ConnectTimeout).ConfigureAwait(false);
            }
            catch
            {
                LibRdmaCmNativeMethods.rdma_destroy_event_channel(eventChannel);
                throw;
            }

            var connection = BuildDataPath(cmId, eventChannel, ownsEventChannel: true, options);

            try
            {
                var connParam = default(RdmaConnParam);
                ThrowIfNonZero(LibRdmaCmNativeMethods.rdma_connect(cmId, ref connParam), "rdma_connect");
                await RdmaCmEventReader.WaitForAsync(eventChannel, RdmaCmEventType.Established, options.ConnectTimeout).ConfigureAwait(false);
            }
            catch
            {
                await connection.DisposeAsync().ConfigureAwait(false);
                throw;
            }

            return connection;
        }

        /// <summary>
        /// Passive/server side: called by <see cref="RdmaQueuePairListener"/> once it has already
        /// received a <see cref="RdmaCmEventType.ConnectRequest"/> event for <paramref name="childCmId"/>.
        /// Builds the same data-path resources as <see cref="ConnectAsync"/>, then accepts the
        /// connection. Does not own <paramref name="eventChannel"/> -- inbound child ids share the
        /// listener's event channel (this transport does not use <c>rdma_migrate_id</c>), so the
        /// listener is responsible for destroying it once, at shutdown.
        /// </summary>
        internal static IRdmaQueuePairConnection Accept(IntPtr childCmId, IntPtr eventChannel, RdmaClusterMessageBusOptions options)
        {
            var connection = BuildDataPath(childCmId, eventChannel, ownsEventChannel: false, options);

            var connParam = default(RdmaConnParam);
            var acceptRc = LibRdmaCmNativeMethods.rdma_accept(childCmId, ref connParam);
            if (acceptRc != 0)
            {
                connection.DisposeAsync().AsTask().GetAwaiter().GetResult();
                throw new InvalidOperationException($"rdma_accept failed (rc={acceptRc}).");
            }

            // Unlike ConnectAsync, this method does not itself wait for RDMA_CM_EVENT_ESTABLISHED --
            // that event arrives later, interleaved with every other event on the shared listener
            // channel, so RdmaQueuePairListener's own event loop is responsible for observing it
            // (or REJECTED/CONNECT_ERROR) for this specific child id before treating the connection
            // as usable. See RdmaQueuePairListener.AcceptConnectionsAsync.
            return connection;
        }

        /// <summary>
        /// Builds every local verbs resource for one queue pair: opens the device RDMA CM already
        /// resolved (<see cref="ReadVerbsContext"/>), allocates a protection domain, an
        /// event-driven completion channel/queue, the queue pair itself (via
        /// <c>rdma_create_qp</c>, not raw <c>ibv_create_qp</c> -- see the design note on
        /// <c>ibv_destroy_qp</c> in <see cref="LibIbVerbsNativeMethods"/>), and registers one send
        /// buffer plus <see cref="RdmaClusterMessageBusOptions.CompletionQueueDepth"/> receive
        /// buffers, pre-posting every receive buffer before returning (RC send/recv requires a
        /// receive work request to already be posted before the peer's matching send arrives).
        /// </summary>
        private static RdmaQueuePairConnection BuildDataPath(IntPtr cmId, IntPtr eventChannel, bool ownsEventChannel, RdmaClusterMessageBusOptions options)
        {
            // RDMA CM already opened (and, for the active side, selected via route resolution) the
            // verbs device by the time address/route resolution completes -- read it back rather
            // than calling ibv_open_device ourselves, which would open a second, unrelated context
            // not associated with this connection id. See ReadVerbsContext's doc comment for the
            // fixed-offset-read caveat.
            var context = ReadVerbsContext(cmId);
            if (context == IntPtr.Zero)
                throw new InvalidOperationException("Could not read rdma_cm_id.verbs -- RDMA CM has not resolved a device for this connection yet.");

            var pd = LibIbVerbsNativeMethods.ibv_alloc_pd(context);
            if (pd == IntPtr.Zero)
                throw new InvalidOperationException("ibv_alloc_pd failed.");

            var completionChannel = LibIbVerbsNativeMethods.ibv_create_comp_channel(context);
            if (completionChannel == IntPtr.Zero)
                throw new InvalidOperationException("ibv_create_comp_channel failed.");

            var completionQueue = LibIbVerbsNativeMethods.ibv_create_cq(context, options.CompletionQueueDepth * 2, IntPtr.Zero, completionChannel, 0);
            if (completionQueue == IntPtr.Zero)
                throw new InvalidOperationException("ibv_create_cq failed.");

            ThrowIfNonZero(LibIbVerbsNativeMethods.ibv_req_notify_cq(completionQueue, 0), "ibv_req_notify_cq");

            var qpInitAttr = new IbvQpInitAttr
            {
                QpContext = IntPtr.Zero,
                SendCq = completionQueue,
                RecvCq = completionQueue,
                Srq = IntPtr.Zero,
                Cap = new IbvQpCap
                {
                    MaxSendWr = (uint)options.CompletionQueueDepth,
                    MaxRecvWr = (uint)options.CompletionQueueDepth,
                    MaxSendSge = 1,
                    MaxRecvSge = 1,
                    MaxInlineData = 0,
                },
                QpType = LibIbVerbsNativeMethods.IbvQpType.Rc,
                SqSigAll = 0,
            };
            ThrowIfNonZero(LibRdmaCmNativeMethods.rdma_create_qp(cmId, pd, ref qpInitAttr), "rdma_create_qp");

            var queuePair = ReadQueuePairHandle(cmId);
            if (queuePair == IntPtr.Zero)
            {
                throw new InvalidOperationException(
                    "rdma_create_qp succeeded but the resulting ibv_qp handle could not be read back off rdma_cm_id -- see ReadQueuePairHandle's fixed-offset-read caveat.");
            }

            var sendBuffer = Marshal.AllocHGlobal(options.MaxMessageSize);
            var sendMr = LibIbVerbsNativeMethods.ibv_reg_mr(pd, sendBuffer, (nuint)options.MaxMessageSize, 0);
            if (sendMr == IntPtr.Zero)
                throw new InvalidOperationException("ibv_reg_mr (send buffer) failed.");

            var recvBuffers = new IntPtr[options.CompletionQueueDepth];
            var recvMrs = new IntPtr[options.CompletionQueueDepth];
            for (var i = 0; i < recvBuffers.Length; i++)
            {
                recvBuffers[i] = Marshal.AllocHGlobal(options.MaxMessageSize);
                recvMrs[i] = LibIbVerbsNativeMethods.ibv_reg_mr(
                    pd, recvBuffers[i], (nuint)options.MaxMessageSize, LibIbVerbsNativeMethods.IbvAccessFlags.LocalWrite);

                if (recvMrs[i] == IntPtr.Zero)
                    throw new InvalidOperationException($"ibv_reg_mr (receive buffer {i}) failed.");
            }

            var connection = new RdmaQueuePairConnection(
                cmId, eventChannel, ownsEventChannel, context, pd, completionChannel, completionQueue, queuePair,
                sendMr, sendBuffer, options.MaxMessageSize, recvMrs, recvBuffers);

            for (var i = 0; i < recvBuffers.Length; i++)
            {
                connection.PostRecv(i);
            }

            return connection;
        }

        public async Task SendFrameAsync(RdmaClusterFrame frame, CancellationToken cancellationToken)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            var payloadBytes = Encoding.UTF8.GetBytes(frame.ToNJson());
            if (payloadBytes.Length > _maxMessageSize)
            {
                throw new InvalidOperationException(
                    $"RDMA frame of {payloadBytes.Length} bytes exceeds RdmaClusterMessageBusOptions.MaxMessageSize " +
                    $"({_maxMessageSize} bytes) -- two-sided RDMA send/recv has no partial-message fallback, unlike TCP's byte stream.");
            }

            await _sendLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                Marshal.Copy(payloadBytes, 0, _sendBuffer, payloadBytes.Length);

                var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                _pendingSend = tcs;

                var sge = new IbvSge { Addr = (ulong)_sendBuffer.ToInt64(), Length = (uint)payloadBytes.Length, LKey = ReadLKey(_sendMr) };
                var sgeHandle = GCHandle.Alloc(sge, GCHandleType.Pinned);
                int postRc;
                try
                {
                    var wr = new IbvSendWr
                    {
                        WrId = SendWorkRequestId,
                        Next = IntPtr.Zero,
                        SgList = sgeHandle.AddrOfPinnedObject(),
                        NumSge = 1,
                        Opcode = LibIbVerbsNativeMethods.IbvWrOpcode.Send,
                        SendFlags = LibIbVerbsNativeMethods.IbvSendFlags.Signaled,
                        ImmData = 0,
                    };
                    postRc = LibIbVerbsNativeMethods.ibv_post_send(_queuePair, ref wr, out _);
                }
                finally
                {
                    sgeHandle.Free();
                }

                if (postRc != 0)
                {
                    _pendingSend = null;
                    throw new InvalidOperationException($"ibv_post_send failed (rc={postRc}).");
                }

                await using var registration = cancellationToken.Register(() => tcs.TrySetCanceled(cancellationToken));
                var succeeded = await tcs.Task.ConfigureAwait(false);
                if (!succeeded)
                    throw new InvalidOperationException("RDMA send work completion reported a non-success status.");
            }
            finally
            {
                _pendingSend = null;
                _sendLock.Release();
            }
        }

        public IAsyncEnumerable<RdmaClusterFrame> ReceiveFramesAsync(CancellationToken cancellationToken)
            => _receiveChannel.Reader.ReadAllAsync(cancellationToken);

        /// <summary>(Re-)posts <see cref="_recvBuffers"/>[<paramref name="index"/>] as a fresh <c>ibv_post_recv</c> work request, using <c>index + 1</c> as its <c>wr_id</c>.</summary>
        private void PostRecv(int index)
        {
            var sge = new IbvSge
            {
                Addr = (ulong)_recvBuffers[index].ToInt64(),
                Length = (uint)_maxMessageSize,
                LKey = ReadLKey(_recvMrs[index]),
            };
            var sgeHandle = GCHandle.Alloc(sge, GCHandleType.Pinned);
            try
            {
                var wr = new IbvRecvWr
                {
                    WrId = (ulong)(index + 1),
                    Next = IntPtr.Zero,
                    SgList = sgeHandle.AddrOfPinnedObject(),
                    NumSge = 1,
                };
                var rc = LibIbVerbsNativeMethods.ibv_post_recv(_queuePair, ref wr, out _);
                if (rc != 0)
                {
                    // Best-effort, mirroring NetworkStreamTcpClusterConnection's silent recovery --
                    // the connection stays alive with one fewer usable receive slot rather than
                    // tearing itself down over a single failed re-post.
                    return;
                }
            }
            finally
            {
                sgeHandle.Free();
            }
        }

        private void RunCompletionLoop()
        {
            try
            {
                while (!_lifetimeCts.IsCancellationRequested)
                {
                    var rc = LibIbVerbsNativeMethods.ibv_get_cq_event(_completionChannel, out var cq, out _);
                    if (rc != 0)
                    {
                        // Expected on shutdown once DisposeAsync destroys the completion channel to
                        // unblock this thread -- see DisposeAsync's remarks on shutdown ordering.
                        _receiveChannel.Writer.TryComplete();
                        return;
                    }

                    LibIbVerbsNativeMethods.ibv_ack_cq_events(cq, 1);

                    if (LibIbVerbsNativeMethods.ibv_req_notify_cq(cq, 0) != 0)
                    {
                        _receiveChannel.Writer.TryComplete();
                        return;
                    }

                    DrainCompletions(cq);
                }

                _receiveChannel.Writer.TryComplete();
            }
            catch (Exception exception)
            {
                _receiveChannel.Writer.TryComplete(exception);
            }
        }

        private void DrainCompletions(IntPtr cq)
        {
            const int batchSize = 16;
            var entrySize = Marshal.SizeOf<IbvWc>();
            var wcArray = Marshal.AllocHGlobal(entrySize * batchSize);
            try
            {
                int count;
                while ((count = LibIbVerbsNativeMethods.ibv_poll_cq(cq, batchSize, wcArray)) > 0)
                {
                    for (var i = 0; i < count; i++)
                    {
                        var wc = Marshal.PtrToStructure<IbvWc>(IntPtr.Add(wcArray, i * entrySize));
                        HandleCompletion(wc);
                    }
                }
            }
            finally
            {
                Marshal.FreeHGlobal(wcArray);
            }
        }

        private void HandleCompletion(IbvWc wc)
        {
            if (wc.WrId == SendWorkRequestId && wc.Opcode == LibIbVerbsNativeMethods.IbvWcOpcode.Send)
            {
                _pendingSend?.TrySetResult(wc.Status == LibIbVerbsNativeMethods.IbvWcStatus.Success);
                return;
            }

            var index = (int)wc.WrId - 1;
            if (index < 0 || index >= _recvBuffers.Length)
                return; // Unrecognized completion -- nothing to safely re-post; drop it.

            if (wc.Status != LibIbVerbsNativeMethods.IbvWcStatus.Success)
            {
                // Same "skip and keep going" reasoning as a malformed TCP frame.
                PostRecv(index);
                return;
            }

            try
            {
                var bytes = new byte[wc.ByteLen];
                Marshal.Copy(_recvBuffers[index], bytes, 0, (int)wc.ByteLen);
                var frame = Encoding.UTF8.GetString(bytes).FromNJson<RdmaClusterFrame>();
                if (frame is not null)
                    _receiveChannel.Writer.TryWrite(frame);
            }
            catch (Exception)
            {
                // A single malformed message must not take down an otherwise-healthy connection --
                // mirrors NetworkStreamTcpClusterConnection's ReceiveFramesAsync exactly.
            }
            finally
            {
                PostRecv(index);
            }
        }

        /// <summary>
        /// Reads <c>rdma_cm_id.verbs</c> (a <c>struct ibv_context*</c>) directly out of native
        /// memory at a fixed byte offset, rather than declaring a full managed <c>struct
        /// rdma_cm_id</c> -- see the design note in <see cref="LibRdmaCmNativeMethods.rdma_create_id"/>'s
        /// doc comment for why the full struct is deliberately not bound. <c>verbs</c> is
        /// documented as <c>struct rdma_cm_id</c>'s FIRST field in every rdma-core release this was
        /// written against, so this reads offset 0 -- the lowest-risk possible fixed-offset read
        /// (it does not depend on the size or alignment of any preceding field, unlike
        /// <see cref="ReadQueuePairHandle"/>). Still NOT verified against an installed header --
        /// see the file-level banners in <see cref="LibIbVerbsNativeMethods"/>/<see cref="LibRdmaCmNativeMethods"/>.
        /// </summary>
        private static IntPtr ReadVerbsContext(IntPtr cmId) => Marshal.ReadIntPtr(cmId, 0);

        /// <summary>
        /// Reads <c>rdma_cm_id.qp</c> (a <c>struct ibv_qp*</c>) directly out of native memory at a
        /// fixed byte offset. <c>qp</c> is documented as <c>struct rdma_cm_id</c>'s FOURTH field
        /// (after <c>verbs</c>, <c>channel</c>, <c>context</c> -- three pointer-sized fields ahead
        /// of it) in every rdma-core release this was written against, hence offset
        /// <c>3 * IntPtr.Size</c>. THIS IS RISKIER than <see cref="ReadVerbsContext"/>'s offset-0
        /// read, because it silently assumes no padding or reordering among those three leading
        /// pointer fields -- verify this offset against the installed
        /// <c>/usr/include/rdma/rdma_cma.h</c> before trusting it on real hardware.
        /// </summary>
        private static IntPtr ReadQueuePairHandle(IntPtr cmId) => Marshal.ReadIntPtr(cmId, 3 * IntPtr.Size);

        private static uint ReadLKey(IntPtr memoryRegion) => Marshal.PtrToStructure<IbvMr>(memoryRegion).LKey;

        private static SockAddrIn BuildSockAddrIn(IPEndPoint endpoint)
        {
            var addressBytes = endpoint.Address.GetAddressBytes();
            return new SockAddrIn
            {
                SinFamily = AfInet,
                SinPort = (ushort)IPAddress.HostToNetworkOrder((short)endpoint.Port),
                SinAddr = BitConverter.ToUInt32(addressBytes, 0),
                SinZero = new byte[8],
            };
        }

        private static void ThrowIfNonZero(int rc, string nativeCall)
        {
            if (rc != 0)
                throw new InvalidOperationException($"{nativeCall} failed (rc={rc}).");
        }

        public async ValueTask DisposeAsync()
        {
            if (_disposed)
                return;
            _disposed = true;

            await _lifetimeCts.CancelAsync().ConfigureAwait(false);
            _pendingSend?.TrySetCanceled();

            try
            {
                LibRdmaCmNativeMethods.rdma_disconnect(_cmId);
            }
            catch
            {
                // Best-effort -- the connection is torn down regardless.
            }

            // Destroying the completion channel closes its underlying fd, which is what actually
            // unblocks RunCompletionLoop's blocking ibv_get_cq_event call -- there is no
            // CancellationToken-based way to interrupt that native call. This deliberately runs
            // BEFORE ibv_destroy_cq below, which is the OPPOSITE of libibverbs' documented teardown
            // order (destroy every CQ using a channel before destroying the channel itself). This
            // ordering conflict is a known, unresolved risk of this shutdown design -- validate on
            // real hardware whether ibv_destroy_cq still succeeds cleanly afterward, or whether a
            // different unblocking mechanism (e.g. a sentinel work request) is needed instead.
            try
            {
                LibIbVerbsNativeMethods.ibv_destroy_comp_channel(_completionChannel);
            }
            catch
            {
                // Best-effort.
            }

            if (_completionThread.IsAlive)
            {
                _completionThread.Join(TimeSpan.FromSeconds(5));
            }

            foreach (var mr in _recvMrs)
            {
                try { LibIbVerbsNativeMethods.ibv_dereg_mr(mr); } catch { /* best-effort */ }
            }

            foreach (var buffer in _recvBuffers)
            {
                Marshal.FreeHGlobal(buffer);
            }

            try { LibIbVerbsNativeMethods.ibv_dereg_mr(_sendMr); } catch { /* best-effort */ }
            Marshal.FreeHGlobal(_sendBuffer);

            try { LibRdmaCmNativeMethods.rdma_destroy_qp(_cmId); } catch { /* best-effort */ }
            try { LibIbVerbsNativeMethods.ibv_destroy_cq(_completionQueue); } catch { /* best-effort -- see ordering note above */ }
            try { LibIbVerbsNativeMethods.ibv_dealloc_pd(_protectionDomain); } catch { /* best-effort */ }
            try { LibIbVerbsNativeMethods.ibv_close_device(_verbsContext); } catch { /* best-effort */ }
            try { LibRdmaCmNativeMethods.rdma_destroy_id(_cmId); } catch { /* best-effort */ }

            if (_ownsEventChannel)
            {
                try { LibRdmaCmNativeMethods.rdma_destroy_event_channel(_eventChannel); } catch { /* best-effort */ }
            }

            _sendLock.Dispose();
            _lifetimeCts.Dispose();
        }
    }
}
