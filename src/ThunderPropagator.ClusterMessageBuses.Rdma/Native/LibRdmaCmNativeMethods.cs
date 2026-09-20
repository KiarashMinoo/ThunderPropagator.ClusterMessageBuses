using System.Runtime.InteropServices;

namespace ThunderPropagator.ClusterMessageBuses.Rdma
{
    // ════════════════════════════════════════════════════════════════════════════════════════
    // !!! NATIVE INTEROP -- NOT VALIDATED AGAINST A REAL COMPILER, LINKER, OR HEADER !!!
    // ════════════════════════════════════════════════════════════════════════════════════════
    //
    // Every DllImport signature and struct layout in this file is a best-effort, hand-transcribed
    // approximation of the public C API declared in <rdma/rdma_cma.h> (plus the plain sockaddr_in
    // shape from <netinet/in.h> it is built on), as shipped by the rdma-core project (package
    // "librdmacm-dev" / "rdma-core-devel" depending on distro). It has NOT been checked
    // field-by-field against an actual installed copy of those headers, has NEVER been compiled
    // (this environment has no C compiler, no dotnet SDK, and no RDMA hardware), and struct
    // layouts documented here CAN vary slightly between rdma-core/kernel versions -- the
    // <c>struct rdma_cm_event</c> shape in particular has changed shape across rdma-core releases
    // and is the single riskiest struct in this transport.
    //
    // A single wrong field, wrong field ORDER, or wrong field SIZE in a [StructLayout(Sequential)]
    // struct that crosses this P/Invoke boundary causes silent memory corruption in native code --
    // not a managed exception, not a compiler error. This is a correctness-critical risk, not a
    // cosmetic one.
    //
    // BEFORE TRUSTING ANY OF THIS CODE:
    //   1. Install librdmacm-dev / rdma-core-devel on the actual target machine.
    //   2. Diff every struct below, field by field (name, type, order, size), against the
    //      installed /usr/include/rdma/rdma_cma.h and /usr/include/netinet/in.h.
    //   3. Compile a tiny native test harness (or use an existing rdma-core example like rping.c
    //      or cmtime.c) to confirm sizeof() for each struct matches what .NET's Marshal.SizeOf
    //      reports for its managed counterpart.
    //   4. Only then run this against real hardware, and only then trust its output.
    //
    // DELIBERATE DEVIATION FROM A LITERAL READING OF THE TASK'S FUNCTION LIST: the task named
    // rdma_create_id/rdma_resolve_addr/rdma_resolve_route/rdma_connect/rdma_listen/rdma_accept/
    // rdma_get_cm_event/rdma_ack_cm_event/rdma_disconnect/rdma_destroy_id explicitly, but did not
    // name rdma_create_event_channel, rdma_destroy_event_channel, rdma_bind_addr, rdma_create_qp,
    // or rdma_destroy_qp. All five are bound here anyway because they are required for a correct
    // RDMA CM handshake:
    //   - rdma_create_id's first parameter IS an event channel -- one must exist first.
    //   - A passive (listening) side needs rdma_bind_addr before rdma_listen; there is no other
    //     way to tell RDMA CM which local address to listen on.
    //   - The queue pair must be created via rdma_create_qp (not raw ibv_create_qp) so that RDMA
    //     CM can embed its queue-pair number in the CM handshake messages and track its lifetime
    //     against the connection ID -- see the note on ibv_destroy_qp in
    //     LibIbVerbsNativeMethods for why raw ibv_create_qp/ibv_destroy_qp are not used instead.
    // ════════════════════════════════════════════════════════════════════════════════════════

    /// <summary>
    /// P/Invoke bindings for the subset of <c>librdmacm</c> ("RDMA CM") this transport uses for
    /// out-of-band connection establishment. Verbs has no wire handshake protocol of its own --
    /// RDMA CM supplies one (modeled after sockets: resolve/connect on the active side,
    /// bind/listen/accept on the passive side, an event queue instead of blocking calls) and,
    /// once <see cref="RdmaCmEventType.Established"/> fires, hands control of the data path to
    /// plain verbs (<see cref="LibIbVerbsNativeMethods"/>).
    /// </summary>
    internal static class LibRdmaCmNativeMethods
    {
        private const string LibraryName = "librdmacm.so.1";

        // ---- Event channel ----------------------------------------------------------------------

        /// <summary>
        /// <c>struct rdma_event_channel *rdma_create_event_channel(void);</c> -- see the deviation
        /// note above for why this is bound despite not being named in the task's literal list.
        /// </summary>
        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "rdma_create_event_channel")]
        internal static extern IntPtr rdma_create_event_channel();

        /// <summary><c>void rdma_destroy_event_channel(struct rdma_event_channel *channel);</c></summary>
        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "rdma_destroy_event_channel")]
        internal static extern void rdma_destroy_event_channel(IntPtr channel);

        // ---- Connection identifier lifecycle ----------------------------------------------------

        /// <summary>
        /// <c>int rdma_create_id(struct rdma_event_channel *channel, struct rdma_cm_id **id, void *context, enum rdma_port_space ps);</c>
        /// <paramref name="id"/> is treated as an opaque handle everywhere in this transport --
        /// this binding deliberately does NOT declare a managed <c>struct rdma_cm_id</c> layout
        /// (unlike every other struct in these two files) because <c>struct rdma_cm_id</c> is
        /// large, contains several nested structs (<c>struct rdma_route</c> in particular) whose
        /// exact shape is even more version-sensitive than the structs already approximated here,
        /// and nothing in this transport needs to read its fields directly -- every value this
        /// transport needs (the queue pair, the resolved route, etc.) is obtained through RDMA
        /// CM's own accessor-style calls (<c>rdma_create_qp</c>, <c>rdma_resolve_route</c>,
        /// <c>rdma_connect</c>) instead of by reaching into the struct by hand.
        /// </summary>
        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "rdma_create_id")]
        internal static extern int rdma_create_id(IntPtr channel, out IntPtr id, IntPtr context, int ps);

        /// <summary><c>int rdma_destroy_id(struct rdma_cm_id *id);</c></summary>
        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "rdma_destroy_id")]
        internal static extern int rdma_destroy_id(IntPtr id);

        // ---- Active (connecting) side ------------------------------------------------------------

        /// <summary>
        /// <c>int rdma_resolve_addr(struct rdma_cm_id *id, struct sockaddr *src_addr, struct sockaddr *dst_addr, int timeout_ms);</c>
        /// <paramref name="srcAddr"/> is <see cref="IntPtr.Zero"/> to let RDMA CM pick the local
        /// source address/route automatically. Asynchronous: completion is signalled by an
        /// <see cref="RdmaCmEventType.AddrResolved"/> (or <see cref="RdmaCmEventType.AddrError"/>)
        /// event on the id's event channel, not by this call's return value alone.
        /// </summary>
        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "rdma_resolve_addr")]
        internal static extern int rdma_resolve_addr(IntPtr id, IntPtr srcAddr, ref SockAddrIn dstAddr, int timeoutMs);

        /// <summary>
        /// <c>int rdma_resolve_route(struct rdma_cm_id *id, int timeout_ms);</c> Asynchronous, like
        /// <see cref="rdma_resolve_addr"/> -- completion is an
        /// <see cref="RdmaCmEventType.RouteResolved"/>/<see cref="RdmaCmEventType.RouteError"/> event.
        /// </summary>
        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "rdma_resolve_route")]
        internal static extern int rdma_resolve_route(IntPtr id, int timeoutMs);

        /// <summary>
        /// <c>int rdma_connect(struct rdma_cm_id *id, struct rdma_conn_param *conn_param);</c> Must
        /// be called only after route resolution completes and <c>rdma_create_qp</c> has already
        /// been called on <paramref name="id"/> (RDMA CM embeds the queue pair's number in the
        /// connection request it sends). Asynchronous -- completion is an
        /// <see cref="RdmaCmEventType.Established"/>/<see cref="RdmaCmEventType.Rejected"/>/
        /// <see cref="RdmaCmEventType.ConnectError"/> event.
        /// </summary>
        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "rdma_connect")]
        internal static extern int rdma_connect(IntPtr id, ref RdmaConnParam connParam);

        // ---- Passive (listening) side -------------------------------------------------------------

        /// <summary>
        /// <c>int rdma_bind_addr(struct rdma_cm_id *id, struct sockaddr *addr);</c> -- see the
        /// deviation note above for why this is bound despite not being named in the task's
        /// literal list: there is no other way to tell RDMA CM which local address a listening id
        /// should bind before <see cref="rdma_listen"/>.
        /// </summary>
        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "rdma_bind_addr")]
        internal static extern int rdma_bind_addr(IntPtr id, ref SockAddrIn addr);

        /// <summary>
        /// <c>int rdma_listen(struct rdma_cm_id *id, int backlog);</c> After this, inbound
        /// connection attempts surface as <see cref="RdmaCmEventType.ConnectRequest"/> events on
        /// <paramref name="id"/>'s event channel, each carrying a freshly-allocated child
        /// <c>rdma_cm_id</c> (in the event's <c>id</c> field) representing that specific peer --
        /// mirrors <c>TcpListener.AcceptTcpClientAsync</c> handing back a new <c>TcpClient</c> per
        /// inbound connection.
        /// </summary>
        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "rdma_listen")]
        internal static extern int rdma_listen(IntPtr id, int backlog);

        /// <summary>
        /// <c>int rdma_accept(struct rdma_cm_id *id, struct rdma_conn_param *conn_param);</c>
        /// Called on the child id from a <see cref="RdmaCmEventType.ConnectRequest"/> event, after
        /// <c>rdma_create_qp</c> has been called on that same child id.
        /// </summary>
        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "rdma_accept")]
        internal static extern int rdma_accept(IntPtr id, ref RdmaConnParam connParam);

        // ---- Queue pair (created/destroyed through RDMA CM, not raw verbs) -----------------------

        /// <summary>
        /// <c>int rdma_create_qp(struct rdma_cm_id *id, struct ibv_pd *pd, struct ibv_qp_init_attr *qp_init_attr);</c>
        /// See the deviation note at the top of this file for why the queue pair is created here
        /// rather than via raw <c>ibv_create_qp</c>.
        /// </summary>
        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "rdma_create_qp")]
        internal static extern int rdma_create_qp(IntPtr id, IntPtr pd, ref IbvQpInitAttr qpInitAttr);

        /// <summary><c>void rdma_destroy_qp(struct rdma_cm_id *id);</c></summary>
        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "rdma_destroy_qp")]
        internal static extern void rdma_destroy_qp(IntPtr id);

        // ---- Event queue and teardown --------------------------------------------------------------

        /// <summary>
        /// <c>int rdma_get_cm_event(struct rdma_event_channel *channel, struct rdma_cm_event **event);</c>
        /// Blocks the calling (dedicated background) thread until a CM event is available.
        /// </summary>
        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "rdma_get_cm_event")]
        internal static extern int rdma_get_cm_event(IntPtr channel, out IntPtr @event);

        /// <summary>
        /// <c>int rdma_ack_cm_event(struct rdma_cm_event *event);</c> Must be called exactly once
        /// per event returned by <c>rdma_get_cm_event</c> -- an un-acked event leaks and, per
        /// rdma-core's own documentation, can wedge subsequent event delivery on that channel.
        /// </summary>
        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "rdma_ack_cm_event")]
        internal static extern int rdma_ack_cm_event(IntPtr @event);

        /// <summary><c>int rdma_disconnect(struct rdma_cm_id *id);</c></summary>
        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "rdma_disconnect")]
        internal static extern int rdma_disconnect(IntPtr id);

        // ---- Constants (from <rdma/rdma_cma.h> enums) -----------------------------------------------

        /// <summary>
        /// <c>enum rdma_port_space</c> (partial). <see cref="Tcp"/> is used unconditionally: it is
        /// the conventional port space for reliable-connected RDMA CM connections (used by
        /// essentially every rdma-core RC example, e.g. rping.c) even though no actual TCP/IP
        /// socket is involved -- the name is a historical artifact of RDMA CM's sockets-shaped API.
        /// </summary>
        internal static class RdmaPortSpace
        {
            internal const int Tcp = 0x0106;
        }
    }

    /// <summary>
    /// <c>enum rdma_cm_event_type</c> (partial -- only the values this transport's event loop
    /// switches on). Numeric values approximate current rdma-core; NOT verified.
    /// </summary>
    internal enum RdmaCmEventType
    {
        AddrResolved = 0,
        AddrError = 1,
        RouteResolved = 2,
        RouteError = 3,
        ConnectRequest = 4,
        ConnectResponse = 5,
        ConnectError = 6,
        Unreachable = 7,
        Rejected = 8,
        Established = 9,
        Disconnected = 10,
        DeviceRemoval = 11,
        MulticastJoin = 12,
        MulticastError = 13,
        AddrChange = 14,
        TimewaitExit = 15,
    }

    /// <summary>
    /// <c>struct sockaddr_in</c> from <c>&lt;netinet/in.h&gt;</c>, Linux layout: 2-byte address
    /// family, 2-byte port (network byte order), 4-byte IPv4 address (network byte order), and an
    /// 8-byte zero-padding tail to make it the same size as the generic <c>struct sockaddr</c>.
    /// IPv6 peers are out of scope for this first pass -- see the connect/listen paths in
    /// <see cref="RdmaQueuePairConnection"/> and <see cref="RdmaQueuePairListener"/>.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    internal struct SockAddrIn
    {
        /// <summary><c>AF_INET</c> on Linux is 2.</summary>
        internal short SinFamily;

        /// <summary>Port, network (big-endian) byte order.</summary>
        internal ushort SinPort;

        /// <summary>IPv4 address, network (big-endian) byte order.</summary>
        internal uint SinAddr;

        [MarshalAs(UnmanagedType.ByValArray, SizeConst = 8)]
        internal byte[] SinZero;
    }

    /// <summary>
    /// <c>struct rdma_conn_param</c> -- passed to <c>rdma_connect</c>/<c>rdma_accept</c>. This
    /// transport does not use RDMA CM's private-data exchange for anything (frames carry their own
    /// self-describing envelope over the established connection instead), so
    /// <see cref="PrivateData"/>/<see cref="PrivateDataLen"/> are always zero/null.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    internal struct RdmaConnParam
    {
        internal IntPtr PrivateData;
        internal byte PrivateDataLen;
        internal byte ResponderResources;
        internal byte InitiatorDepth;
        internal byte FlowControl;
        internal byte RetryCount;
        internal byte RnrRetryCount;
        internal byte Srq;
        internal uint QpNum;
    }

    /// <summary>
    /// <c>struct rdma_cm_event</c> -- THE RISKIEST STRUCT IN THIS TRANSPORT. The real struct's
    /// shape (particularly whether/how the <c>param</c> union is laid out, and whether a given
    /// rdma-core version also carries a compatibility <c>conn_param</c> field alongside it) has
    /// changed across rdma-core releases; this approximation carries only the fields this
    /// transport's event loop actually reads (which event id, which cm_id, and -- for a
    /// <see cref="RdmaCmEventType.ConnectRequest"/> event -- the inbound peer's queue-pair number
    /// out of the embedded <see cref="RdmaConnParam"/>) and does not attempt the <c>ud</c> (unreliable-
    /// datagram) union variant at all, since this transport only ever uses RC queue pairs.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    internal struct RdmaCmEvent
    {
        internal IntPtr Id;
        internal IntPtr ListenId;
        internal int Event;
        internal int Status;
        internal RdmaConnParam Param;
    }
}
