using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using ThunderPropagator.BuildingBlocks.Application.Helpers;

namespace ThunderPropagator.ClusterMessageBuses.Srd
{
    /// <summary>
    /// Production <see cref="ISrdClusterEndpoint"/>: binds one libfabric RDM endpoint over the "efa"
    /// provider, following the sequence documented on <see cref="LibFabricNativeMethods"/>'s bound
    /// functions: <c>fi_getinfo</c> → <c>fi_fabric</c> → <c>fi_domain</c> → <c>fi_cq_open</c> →
    /// <c>fi_av_open</c> → <c>fi_endpoint</c> → <c>fi_ep_bind</c> (×2) → <c>fi_enable</c> → post
    /// <c>fi_recv</c> buffers.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>NOTHING IN THIS CLASS HAS RUN AGAINST REAL LIBFABRIC OR REAL EFA HARDWARE.</b> It was
    /// written in an environment with no dotnet SDK, no C compiler, and no EFA-equipped machine
    /// available — see this project's DI extension
    /// (<see cref="SrdClusterMessageBusExtensions"/>) and
    /// <c>Native/LibFabricNativeMethods.cs</c>'s banner for the full list of what needs real-hardware
    /// validation before this is trustworthy.
    /// </para>
    /// <para>
    /// One shared completion queue is used for both send and receive completions (rather than
    /// separate send/receive CQs) — simpler to reason about for a single-endpoint, moderate-throughput
    /// control-plane transport like this one; a higher-throughput deployment might split them.
    /// </para>
    /// <para>
    /// A single dedicated background thread blocks on <c>fi_cq_sread</c> (with
    /// <see cref="SrdClusterMessageBusOptions.CompletionPollTimeout"/> as its timeout) in a loop,
    /// because libfabric's completion-queue APIs are blocking native calls with no .NET async
    /// integration. Completions are correlated back to either a pending <c>fi_send</c> (via a
    /// <see cref="TaskCompletionSource{TResult}"/> keyed by the send's native context pointer) or a
    /// posted <c>fi_recv</c> buffer (keyed the same way) — a receive completion's payload is copied out,
    /// parsed into a <see cref="SrdClusterFrame"/>, handed to <see cref="ReceiveFramesAsync"/>'s
    /// consumer via an unbounded <see cref="System.Threading.Channels.Channel{T}"/>, and its buffer is
    /// immediately reposted via <c>fi_recv</c> — libfabric does not buffer an "unexpected" message the
    /// way a socket does, so a receive buffer must always be posted before it can receive.
    /// </para>
    /// </remarks>
    internal sealed partial class LibFabricSrdClusterEndpoint : ISrdClusterEndpoint
    {
        private readonly SrdClusterMessageBusOptions _options;
        private readonly ILogger _logger;

        private readonly SemaphoreSlim _initLock = new(1, 1);
        private volatile bool _initialized;
        private volatile bool _stopping;

        // Native handles, populated by InitializeNative() in open order; torn down in DisposeAsync()
        // in the reverse order, per the teardown sequence documented on the task this class implements.
        private IntPtr _infoListHead;
        private IntPtr _chosenInfo;
        private IntPtr _fabricAttrNative;
        private IntPtr _epAttrNative;
        private IntPtr _provNameNative;
        private IntPtr _fabric;
        private IntPtr _domain;
        private IntPtr _cq;
        private IntPtr _av;
        private IntPtr _ep;

        private readonly ConcurrentDictionary<Uri, ulong> _peerAddresses = new();
        private readonly ConcurrentDictionary<IntPtr, TaskCompletionSource<bool>> _pendingSends = new();
        private readonly ConcurrentDictionary<IntPtr, RecvSlot> _recvSlots = new();

        private readonly Channel<SrdClusterFrame> _inbound = Channel.CreateUnbounded<SrdClusterFrame>(
            new UnboundedChannelOptions { SingleReader = false, SingleWriter = false });

        private Thread? _completionThread;

        internal LibFabricSrdClusterEndpoint(SrdClusterMessageBusOptions options, ILogger logger)
        {
            _options = options;
            _logger = logger;
        }

        private async Task EnsureInitializedAsync(CancellationToken cancellationToken)
        {
            if (_initialized)
                return;

            await _initLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                if (_initialized)
                    return;

                InitializeNative();

                _completionThread = new Thread(CompletionLoop)
                {
                    IsBackground = true,
                    Name = "Srd-Cq-Completion",
                };
                _completionThread.Start();

                _initialized = true;
                Log.EndpointInitialized(_logger, _options.ProviderName);
            }
            finally
            {
                _initLock.Release();
            }
        }

        /// <summary>
        /// Runs the full open sequence documented on this class's own doc comment. Throws
        /// <see cref="InvalidOperationException"/> with a clear message (not a cryptic P/Invoke
        /// failure) if <c>fi_getinfo</c> finds no matching "efa" provider — expected on any host
        /// without an Elastic Fabric Adapter attached, including this sandbox.
        /// </summary>
        private void InitializeNative()
        {
            Log.EndpointInitializing(_logger, _options.ProviderName);

            _provNameNative = Marshal.StringToHGlobalAnsi(_options.ProviderName);

            _fabricAttrNative = Marshal.AllocHGlobal(Marshal.SizeOf<LibFabricNativeMethods.fi_fabric_attr>());
            var fabricAttr = new LibFabricNativeMethods.fi_fabric_attr { prov_name = _provNameNative };
            Marshal.StructureToPtr(fabricAttr, _fabricAttrNative, false);

            _epAttrNative = Marshal.AllocHGlobal(Marshal.SizeOf<LibFabricNativeMethods.fi_ep_attr>());
            var epAttr = new LibFabricNativeMethods.fi_ep_attr { type = LibFabricNativeMethods.FI_EP_RDM };
            Marshal.StructureToPtr(epAttr, _epAttrNative, false);

            var hints = new LibFabricNativeMethods.fi_info
            {
                caps = LibFabricNativeMethods.FI_MSG | LibFabricNativeMethods.FI_SEND | LibFabricNativeMethods.FI_RECV,
                ep_attr = _epAttrNative,
                fabric_attr = _fabricAttrNative,
            };

            var rc = LibFabricNativeMethods.fi_getinfo(LibFabricNativeMethods.FiVersion, IntPtr.Zero, IntPtr.Zero, 0, ref hints, out var infoResult);
            if (rc != 0 || infoResult == IntPtr.Zero)
            {
                throw new InvalidOperationException(
                    $"libfabric's '{_options.ProviderName}' provider was not found (fi_getinfo returned {rc}). " +
                    "The EFA provider is only available on AWS EC2 instance types with an Elastic Fabric Adapter " +
                    "attached, and libfabric itself must be installed on the host (e.g. the aws-efa-installer " +
                    "package). This is expected — not a bug — on any non-AWS host, an AWS instance without EFA " +
                    "attached, or a host missing the libfabric.so.1 native library entirely.");
            }

            _infoListHead = infoResult;
            _chosenInfo = FindMatchingProvider(_infoListHead, _options.ProviderName)
                ?? throw new InvalidOperationException(
                    $"fi_getinfo returned results, but none matched the requested provider '{_options.ProviderName}'. " +
                    "This should not happen given the hints passed to fi_getinfo already filtered on provider name — " +
                    "treat this as a sign the fi_info/fi_fabric_attr struct layout in LibFabricNativeMethods.cs has " +
                    "drifted from the installed libfabric version and needs re-verification against its headers.");

            var chosenInfo = Marshal.PtrToStructure<LibFabricNativeMethods.fi_info>(_chosenInfo);

            ThrowIfNativeCallFailed(LibFabricNativeMethods.fi_fabric(chosenInfo.fabric_attr, out _fabric, IntPtr.Zero), nameof(LibFabricNativeMethods.fi_fabric));
            ThrowIfNativeCallFailed(LibFabricNativeMethods.fi_domain(_fabric, _chosenInfo, out _domain, IntPtr.Zero), nameof(LibFabricNativeMethods.fi_domain));

            var cqAttr = new LibFabricNativeMethods.fi_cq_attr
            {
                size = (UIntPtr)(_options.PostedReceiveBufferCount + 16),
                format = LibFabricNativeMethods.FI_CQ_FORMAT_DATA,
                wait_obj = LibFabricNativeMethods.FI_WAIT_NONE,
            };
            ThrowIfNativeCallFailed(LibFabricNativeMethods.fi_cq_open(_domain, ref cqAttr, out _cq, IntPtr.Zero), nameof(LibFabricNativeMethods.fi_cq_open));

            var avAttr = new LibFabricNativeMethods.fi_av_attr
            {
                type = LibFabricNativeMethods.FI_AV_TABLE,
                count = (UIntPtr)64,
            };
            ThrowIfNativeCallFailed(LibFabricNativeMethods.fi_av_open(_domain, ref avAttr, out _av, IntPtr.Zero), nameof(LibFabricNativeMethods.fi_av_open));

            ThrowIfNativeCallFailed(LibFabricNativeMethods.fi_endpoint(_domain, _chosenInfo, out _ep, IntPtr.Zero), nameof(LibFabricNativeMethods.fi_endpoint));

            // Bind the endpoint to the (shared) completion queue for both transmit and receive
            // completions, and to the address vector. The FI_TRANSMIT/FI_RECV flag values below are
            // written from memory of <rdma/fi_endpoint.h> and, like everything else in this file's
            // native surface, need verification against the real header.
            const ulong FI_TRANSMIT = 1UL << 10;
            const ulong FI_RECV_BIND_FLAG = LibFabricNativeMethods.FI_RECV;
            ThrowIfNativeCallFailed(LibFabricNativeMethods.fi_ep_bind(_ep, _cq, FI_TRANSMIT | FI_RECV_BIND_FLAG), nameof(LibFabricNativeMethods.fi_ep_bind) + "(cq)");
            ThrowIfNativeCallFailed(LibFabricNativeMethods.fi_ep_bind(_ep, _av, 0), nameof(LibFabricNativeMethods.fi_ep_bind) + "(av)");

            ThrowIfNativeCallFailed(LibFabricNativeMethods.fi_enable(_ep), nameof(LibFabricNativeMethods.fi_enable));

            for (var i = 0; i < _options.PostedReceiveBufferCount; i++)
            {
                PostNewReceiveBuffer();
            }
        }

        /// <summary>
        /// Walks the <c>fi_info</c> linked list <c>fi_getinfo</c> returns (via each node's <c>next</c>
        /// pointer) looking for one whose <c>fabric_attr.prov_name</c> matches
        /// <paramref name="providerName"/>. Belt-and-suspenders: the hints already asked for this
        /// provider, but providers are free to return additional results, and defensively re-filtering
        /// here costs nothing.
        /// </summary>
        private static IntPtr? FindMatchingProvider(IntPtr infoListHead, string providerName)
        {
            var current = infoListHead;
            while (current != IntPtr.Zero)
            {
                var info = Marshal.PtrToStructure<LibFabricNativeMethods.fi_info>(current);
                if (info.fabric_attr != IntPtr.Zero)
                {
                    var fabricAttr = Marshal.PtrToStructure<LibFabricNativeMethods.fi_fabric_attr>(info.fabric_attr);
                    var name = fabricAttr.prov_name != IntPtr.Zero ? Marshal.PtrToStringAnsi(fabricAttr.prov_name) : null;
                    if (string.Equals(name, providerName, StringComparison.OrdinalIgnoreCase))
                    {
                        return current;
                    }
                }

                current = info.next;
            }

            return null;
        }

        private static void ThrowIfNativeCallFailed(int returnCode, string callName)
        {
            if (returnCode != 0)
            {
                throw new InvalidOperationException($"Native libfabric call '{callName}' failed with return code {returnCode}.");
            }
        }

        private void PostNewReceiveBuffer()
        {
            var buffer = new byte[_options.MaxMessageSize];
            var handle = GCHandle.Alloc(buffer, GCHandleType.Pinned);
            var contextPtr = GCHandle.ToIntPtr(handle);
            var slot = new RecvSlot(buffer, handle, contextPtr);
            _recvSlots[contextPtr] = slot;

            var rc = LibFabricNativeMethods.fi_recv(_ep, handle.AddrOfPinnedObject(), (UIntPtr)buffer.Length, IntPtr.Zero, 0 /* FI_ADDR_UNSPEC */, contextPtr);
            if (rc != 0)
            {
                _recvSlots.TryRemove(contextPtr, out _);
                handle.Free();
                Log.ReceiveBufferPostFailed(_logger, rc);
            }
        }

        /// <inheritdoc />
        public async Task SendFrameAsync(SrdClusterFrame frame, Uri peerEndpoint, CancellationToken cancellationToken)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var json = frame.ToNJson();
            var bytes = System.Text.Encoding.UTF8.GetBytes(json);
            if (bytes.Length > _options.MaxMessageSize)
            {
                throw new InvalidOperationException(
                    $"Serialized SRD cluster frame is {bytes.Length} bytes, exceeding the configured " +
                    $"{nameof(SrdClusterMessageBusOptions.MaxMessageSize)} of {_options.MaxMessageSize} bytes.");
            }

            var destAddr = await ResolvePeerAsync(peerEndpoint, cancellationToken).ConfigureAwait(false);

            var handle = GCHandle.Alloc(bytes, GCHandleType.Pinned);
            var contextPtr = GCHandle.ToIntPtr(handle);
            var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            _pendingSends[contextPtr] = tcs;

            try
            {
                var rc = LibFabricNativeMethods.fi_send(_ep, handle.AddrOfPinnedObject(), (UIntPtr)bytes.Length, IntPtr.Zero, destAddr, contextPtr);
                if (rc != 0)
                {
                    throw new InvalidOperationException($"Native fi_send failed with return code {rc} while sending to '{peerEndpoint.Host}'.");
                }

                using (cancellationToken.Register(() => tcs.TrySetCanceled(cancellationToken)))
                {
                    await tcs.Task.ConfigureAwait(false);
                }
            }
            finally
            {
                _pendingSends.TryRemove(contextPtr, out _);
                if (handle.IsAllocated)
                {
                    handle.Free();
                }
            }
        }

        /// <summary>
        /// Resolves <paramref name="peerEndpoint"/> into a cached <c>fi_addr_t</c> (represented here as
        /// <see cref="ulong"/>, per <c>fi_addr_t</c>'s real typedef to <c>uint64_t</c>), inserting it
        /// into the address vector via <c>fi_av_insert</c> on first use. See
        /// <see cref="SrdAddressing"/>'s remarks for why the raw address bytes actually inserted are a
        /// placeholder, not a real EFA address.
        /// </summary>
        private async Task<ulong> ResolvePeerAsync(Uri peerEndpoint, CancellationToken cancellationToken)
        {
            if (_peerAddresses.TryGetValue(peerEndpoint, out var cached))
            {
                return cached;
            }

            var rawAddress = await SrdAddressing.ResolvePlaceholderRawAddressAsync(peerEndpoint, _options.Port, cancellationToken).ConfigureAwait(false);

            var addressHandle = GCHandle.Alloc(rawAddress, GCHandleType.Pinned);
            var resultBuffer = new ulong[1];
            var resultHandle = GCHandle.Alloc(resultBuffer, GCHandleType.Pinned);
            try
            {
                var inserted = LibFabricNativeMethods.fi_av_insert(
                    _av, addressHandle.AddrOfPinnedObject(), (UIntPtr)1, resultHandle.AddrOfPinnedObject(), 0, IntPtr.Zero);

                if (inserted != 1)
                {
                    throw new InvalidOperationException(
                        $"fi_av_insert failed to insert peer '{peerEndpoint.Host}' into the address vector (returned {inserted}).");
                }

                var resolved = resultBuffer[0];
                _peerAddresses[peerEndpoint] = resolved;
                Log.PeerAddressResolved(_logger, peerEndpoint.Host);
                return resolved;
            }
            finally
            {
                addressHandle.Free();
                resultHandle.Free();
            }
        }

        /// <inheritdoc />
        public async IAsyncEnumerable<SrdClusterFrame> ReceiveFramesAsync([EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            await foreach (var frame in _inbound.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                yield return frame;
            }
        }

        /// <summary>
        /// Runs on the dedicated <see cref="_completionThread"/>: blocks on <c>fi_cq_sread</c> in a
        /// loop, translating each returned <c>fi_cq_data_entry</c> into either a completed pending send
        /// or a dispatched inbound frame (with its receive buffer immediately reposted).
        /// </summary>
        private void CompletionLoop()
        {
            const int batchSize = 16;
            var entrySize = Marshal.SizeOf<LibFabricNativeMethods.fi_cq_data_entry>();
            var nativeBuffer = Marshal.AllocHGlobal(entrySize * batchSize);

            try
            {
                while (!_stopping)
                {
                    nint read;
                    try
                    {
                        read = LibFabricNativeMethods.fi_cq_sread(
                            _cq, nativeBuffer, (UIntPtr)batchSize, IntPtr.Zero, (int)_options.CompletionPollTimeout.TotalMilliseconds);
                    }
                    catch (Exception exception)
                    {
                        // A native call itself should never throw a managed exception -- P/Invoke
                        // marshaling failures land here instead. Log and keep the loop alive rather
                        // than letting one bad iteration silently kill all future completions.
                        Log.CompletionPollFaulted(_logger, exception);
                        continue;
                    }

                    if (read <= 0)
                    {
                        // A negative return is libfabric's error convention (commonly -FI_EAGAIN on a
                        // plain timeout with nothing to report, which is the expected common case here)
                        // -- without fi_cq_readerr bound (out of scope for this transport, see the
                        // Native file's banner), a real CQ error can't be distinguished from a routine
                        // timeout. Either way, loop around and try again.
                        continue;
                    }

                    for (var i = 0; i < (int)read; i++)
                    {
                        var entry = Marshal.PtrToStructure<LibFabricNativeMethods.fi_cq_data_entry>(nativeBuffer + i * entrySize);
                        HandleCompletion(entry);
                    }
                }
            }
            finally
            {
                Marshal.FreeHGlobal(nativeBuffer);
            }
        }

        private void HandleCompletion(LibFabricNativeMethods.fi_cq_data_entry entry)
        {
            var context = entry.op_context;

            if (_pendingSends.TryRemove(context, out var sendTcs))
            {
                sendTcs.TrySetResult(true);
                return;
            }

            if (_recvSlots.TryGetValue(context, out var slot))
            {
                try
                {
                    var length = (int)entry.len;
                    if (length > 0 && length <= slot.Buffer.Length)
                    {
                        var payload = new byte[length];
                        Array.Copy(slot.Buffer, payload, length);

                        var json = System.Text.Encoding.UTF8.GetString(payload);
                        var frame = json.FromNJson<SrdClusterFrame>();
                        if (frame is not null)
                        {
                            _inbound.Writer.TryWrite(frame);
                        }
                    }
                }
                catch (Exception exception)
                {
                    Log.InboundFrameUnparseable(_logger, exception);
                }
                finally
                {
                    RepostReceiveBuffer(slot);
                }
            }
        }

        private void RepostReceiveBuffer(RecvSlot slot)
        {
            var rc = LibFabricNativeMethods.fi_recv(_ep, slot.Handle.AddrOfPinnedObject(), (UIntPtr)slot.Buffer.Length, IntPtr.Zero, 0, slot.ContextPtr);
            if (rc != 0)
            {
                Log.ReceiveBufferPostFailed(_logger, rc);
            }
        }

        /// <inheritdoc />
        public async ValueTask DisposeAsync()
        {
            _stopping = true;

            if (_completionThread is { IsAlive: true })
            {
                // The completion thread wakes up on its own within CompletionPollTimeout since
                // fi_cq_sread is called with that as its timeout -- join with a generous grace period
                // rather than blocking indefinitely on a native thread that, in the worst case, is
                // stuck inside a native call.
                await Task.Run(() => _completionThread.Join(TimeSpan.FromSeconds(5))).ConfigureAwait(false);
            }

            _inbound.Writer.TryComplete();

            foreach (var pending in _pendingSends.Values)
            {
                pending.TrySetCanceled();
            }

            // Teardown in strict reverse-open order: endpoint, address vector, completion queue,
            // domain, fabric, then fi_freeinfo on the fi_info list fi_getinfo returned.
            CloseIfOpen(ref _ep, nameof(_ep));
            CloseIfOpen(ref _av, nameof(_av));
            CloseIfOpen(ref _cq, nameof(_cq));
            CloseIfOpen(ref _domain, nameof(_domain));
            CloseIfOpen(ref _fabric, nameof(_fabric));

            if (_infoListHead != IntPtr.Zero)
            {
                LibFabricNativeMethods.fi_freeinfo(_infoListHead);
                _infoListHead = IntPtr.Zero;
            }

            foreach (var slot in _recvSlots.Values)
            {
                if (slot.Handle.IsAllocated)
                {
                    slot.Handle.Free();
                }
            }
            _recvSlots.Clear();

            if (_fabricAttrNative != IntPtr.Zero)
            {
                Marshal.FreeHGlobal(_fabricAttrNative);
                _fabricAttrNative = IntPtr.Zero;
            }
            if (_epAttrNative != IntPtr.Zero)
            {
                Marshal.FreeHGlobal(_epAttrNative);
                _epAttrNative = IntPtr.Zero;
            }
            if (_provNameNative != IntPtr.Zero)
            {
                Marshal.FreeHGlobal(_provNameNative);
                _provNameNative = IntPtr.Zero;
            }

            _initLock.Dispose();
            Log.EndpointDisposed(_logger);
        }

        private void CloseIfOpen(ref IntPtr fid, string name)
        {
            if (fid == IntPtr.Zero)
            {
                return;
            }

            var rc = LibFabricNativeMethods.fi_close(fid);
            if (rc != 0)
            {
                Log.TeardownFailed(_logger, name, rc);
            }

            fid = IntPtr.Zero;
        }

        private sealed class RecvSlot
        {
            internal RecvSlot(byte[] buffer, GCHandle handle, IntPtr contextPtr)
            {
                Buffer = buffer;
                Handle = handle;
                ContextPtr = contextPtr;
            }

            internal byte[] Buffer { get; }
            internal GCHandle Handle { get; }
            internal IntPtr ContextPtr { get; }
        }

        private static partial class Log
        {
            [LoggerMessage(EventId = 91870, Level = LogLevel.Debug,
                Message = "[Cluster] SRD endpoint initializing against libfabric provider '{Provider}'.")]
            public static partial void EndpointInitializing(ILogger logger, string provider);

            [LoggerMessage(EventId = 91871, Level = LogLevel.Information,
                Message = "[Cluster] SRD endpoint initialized against libfabric provider '{Provider}'.")]
            public static partial void EndpointInitialized(ILogger logger, string provider);

            [LoggerMessage(EventId = 91872, Level = LogLevel.Warning,
                Message = "[Cluster] Failed to (re)post an SRD receive buffer; fi_recv returned {ReturnCode}.")]
            public static partial void ReceiveBufferPostFailed(ILogger logger, nint returnCode);

            [LoggerMessage(EventId = 91873, Level = LogLevel.Error,
                Message = "[Cluster] SRD completion-queue poll faulted; the completion thread continues.")]
            public static partial void CompletionPollFaulted(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91874, Level = LogLevel.Warning,
                Message = "[Cluster] An inbound SRD message could not be parsed as a cluster frame; skipping it.")]
            public static partial void InboundFrameUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91875, Level = LogLevel.Debug,
                Message = "[Cluster] SRD peer '{Host}' resolved into the address vector.")]
            public static partial void PeerAddressResolved(ILogger logger, string host);

            [LoggerMessage(EventId = 91876, Level = LogLevel.Warning,
                Message = "[Cluster] Closing native libfabric resource '{Resource}' failed; fi_close returned {ReturnCode}.")]
            public static partial void TeardownFailed(ILogger logger, string resource, int returnCode);

            [LoggerMessage(EventId = 91877, Level = LogLevel.Debug,
                Message = "[Cluster] SRD endpoint disposed.")]
            public static partial void EndpointDisposed(ILogger logger);
        }
    }
}
