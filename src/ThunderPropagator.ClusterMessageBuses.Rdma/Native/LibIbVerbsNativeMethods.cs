using System.Runtime.InteropServices;

namespace ThunderPropagator.ClusterMessageBuses.Rdma
{
    // ════════════════════════════════════════════════════════════════════════════════════════
    // !!! NATIVE INTEROP -- NOT VALIDATED AGAINST A REAL COMPILER, LINKER, OR HEADER !!!
    // ════════════════════════════════════════════════════════════════════════════════════════
    //
    // Every DllImport signature and struct layout in this file is a best-effort, hand-transcribed
    // approximation of the public C API declared in <infiniband/verbs.h>, as shipped by the
    // rdma-core project (package "libibverbs-dev" / "rdma-core-devel" depending on distro). It has
    // NOT been checked field-by-field against an actual installed copy of that header, has NEVER
    // been compiled (this environment has no C compiler, no dotnet SDK, and no RDMA hardware), and
    // struct layouts documented here CAN vary slightly between rdma-core/kernel versions.
    //
    // A single wrong field, wrong field ORDER, or wrong field SIZE in a [StructLayout(Sequential)]
    // struct that crosses this P/Invoke boundary causes silent memory corruption in native code --
    // not a managed exception, not a compiler error. This is a correctness-critical risk, not a
    // cosmetic one.
    //
    // BEFORE TRUSTING ANY OF THIS CODE:
    //   1. Install libibverbs-dev / rdma-core-devel on the actual target machine.
    //   2. Diff every struct below, field by field (name, type, order, size), against the
    //      installed /usr/include/infiniband/verbs.h.
    //   3. Compile a tiny native test harness (or use an existing rdma-core example like
    //      ibv_rc_pingpong) to confirm sizeof() for each struct matches what .NET's Marshal.SizeOf
    //      reports for its managed counterpart.
    //   4. Only then run this against real hardware, and only then trust its output.
    //
    // ════════════════════════════════════════════════════════════════════════════════════════

    /// <summary>
    /// P/Invoke bindings for the subset of <c>libibverbs</c> ("verbs") this transport uses: device
    /// enumeration, protection domains, memory registration, completion queues/channels, queue-pair
    /// teardown, and posting/polling two-sided SEND/RECV work requests. Connection establishment
    /// itself (the wire handshake verbs has no protocol for) lives in
    /// <see cref="LibRdmaCmNativeMethods"/> instead.
    /// </summary>
    internal static class LibIbVerbsNativeMethods
    {
        private const string LibraryName = "libibverbs.so.1";

        // ---- Device enumeration -------------------------------------------------------------

        /// <summary><c>struct ibv_device **ibv_get_device_list(int *num_devices);</c></summary>
        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ibv_get_device_list")]
        internal static extern IntPtr ibv_get_device_list(out int numDevices);

        /// <summary><c>void ibv_free_device_list(struct ibv_device **list);</c></summary>
        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ibv_free_device_list")]
        internal static extern void ibv_free_device_list(IntPtr list);

        /// <summary>
        /// <c>const char *ibv_get_device_name(struct ibv_device *device);</c> -- NOT in the task's
        /// literal function list, but bound here because <see cref="RdmaClusterMessageBusOptions.DeviceName"/>
        /// can only be honored by comparing each enumerated device's name against the configured
        /// one. Returns a pointer owned by libibverbs (do not free it); marshal with
        /// <see cref="Marshal.PtrToStringAnsi(IntPtr)"/>.
        /// </summary>
        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ibv_get_device_name")]
        internal static extern IntPtr ibv_get_device_name(IntPtr device);

        /// <summary>
        /// <c>struct ibv_context *ibv_open_device(struct ibv_device *device);</c> -- NOT in the
        /// task's literal function list, but required: <c>ibv_get_device_list</c> only yields
        /// unopened <c>struct ibv_device*</c> handles, and every other verbs call here
        /// (<c>ibv_alloc_pd</c> in particular) needs an opened <c>struct ibv_context*</c>.
        /// </summary>
        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ibv_open_device")]
        internal static extern IntPtr ibv_open_device(IntPtr device);

        /// <summary><c>int ibv_close_device(struct ibv_context *context);</c></summary>
        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ibv_close_device")]
        internal static extern int ibv_close_device(IntPtr context);

        // ---- Protection domain ----------------------------------------------------------------

        /// <summary><c>struct ibv_pd *ibv_alloc_pd(struct ibv_context *context);</c></summary>
        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ibv_alloc_pd")]
        internal static extern IntPtr ibv_alloc_pd(IntPtr context);

        /// <summary><c>int ibv_dealloc_pd(struct ibv_pd *pd);</c></summary>
        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ibv_dealloc_pd")]
        internal static extern int ibv_dealloc_pd(IntPtr pd);

        // ---- Memory registration ---------------------------------------------------------------

        /// <summary>
        /// <c>struct ibv_mr *ibv_reg_mr(struct ibv_pd *pd, void *addr, size_t length, int access);</c>
        /// <paramref name="access"/> should be <see cref="IbvAccessFlags.LocalWrite"/> for a
        /// receive buffer, and 0 (no special flags needed for the local processor to fill a buffer
        /// this process only reads back out of after an <c>IBV_WR_SEND</c> completion) for a send
        /// buffer -- two-sided send/recv never needs <c>IBV_ACCESS_REMOTE_*</c> flags, since the
        /// remote side never addresses this memory directly (that is what one-sided RDMA
        /// read/write needs, which this transport deliberately does not use -- see the design
        /// rationale in <see cref="RdmaQueuePairConnection"/>'s doc comment).
        /// </summary>
        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ibv_reg_mr")]
        internal static extern IntPtr ibv_reg_mr(IntPtr pd, IntPtr addr, nuint length, int access);

        /// <summary><c>int ibv_dereg_mr(struct ibv_mr *mr);</c></summary>
        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ibv_dereg_mr")]
        internal static extern int ibv_dereg_mr(IntPtr mr);

        // ---- Completion channel / completion queue ---------------------------------------------

        /// <summary><c>struct ibv_comp_channel *ibv_create_comp_channel(struct ibv_context *context);</c></summary>
        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ibv_create_comp_channel")]
        internal static extern IntPtr ibv_create_comp_channel(IntPtr context);

        /// <summary>
        /// <c>int ibv_destroy_comp_channel(struct ibv_comp_channel *channel);</c> -- NOT in the
        /// task's literal function list, but required to tear down what
        /// <c>ibv_create_comp_channel</c> allocated; there is no other verbs call that frees it.
        /// </summary>
        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ibv_destroy_comp_channel")]
        internal static extern int ibv_destroy_comp_channel(IntPtr channel);

        /// <summary>
        /// <c>struct ibv_cq *ibv_create_cq(struct ibv_context *context, int cqe, void *cq_context, struct ibv_comp_channel *channel, int comp_vector);</c>
        /// Deliberately the classic (non-<c>_ex</c>) verbs call -- it takes its parameters directly
        /// rather than through an <c>ibv_cq_init_attr_ex</c> struct, so that struct is not needed
        /// here at all.
        /// </summary>
        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ibv_create_cq")]
        internal static extern IntPtr ibv_create_cq(IntPtr context, int cqe, IntPtr cqContext, IntPtr channel, int compVector);

        /// <summary>
        /// <c>int ibv_destroy_cq(struct ibv_cq *cq);</c> -- NOT in the task's literal function
        /// list, but required to tear down what <c>ibv_create_cq</c> allocated.
        /// </summary>
        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ibv_destroy_cq")]
        internal static extern int ibv_destroy_cq(IntPtr cq);

        /// <summary>
        /// <c>int ibv_req_notify_cq(struct ibv_cq *cq, int solicited_only);</c> arms the completion
        /// channel for exactly one more <c>ibv_get_cq_event</c> notification -- verbs delivers one
        /// event per arm, not a persistent subscription, so this must be re-called after every
        /// drained event (see <see cref="RdmaQueuePairConnection"/>'s completion-thread loop).
        /// </summary>
        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ibv_req_notify_cq")]
        internal static extern int ibv_req_notify_cq(IntPtr cq, int solicitedOnly);

        /// <summary>
        /// <c>int ibv_get_cq_event(struct ibv_comp_channel *channel, struct ibv_cq **cq, void **cq_context);</c>
        /// Blocks the calling (dedicated background) thread until a completion event is available --
        /// this is the "event-driven" half of the design; nothing here busy-polls.
        /// </summary>
        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ibv_get_cq_event")]
        internal static extern int ibv_get_cq_event(IntPtr channel, out IntPtr cq, out IntPtr cqContext);

        /// <summary>
        /// <c>void ibv_ack_cq_events(struct ibv_cq *cq, unsigned int nevents);</c> -- NOT in the
        /// task's literal function list, but required: every event returned by
        /// <c>ibv_get_cq_event</c> must eventually be acknowledged or <c>ibv_destroy_cq</c>
        /// blocks forever waiting for outstanding acks (a well-known rdma-core footgun). Acking once
        /// per drained event (rather than batching) is the simplest correct approach and is what
        /// this transport does.
        /// </summary>
        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ibv_ack_cq_events")]
        internal static extern void ibv_ack_cq_events(IntPtr cq, uint nEvents);

        /// <summary>
        /// <c>int ibv_poll_cq(struct ibv_cq *cq, int num_entries, struct ibv_wc *wc);</c> drains up
        /// to <paramref name="numEntries"/> completions into the caller-owned unmanaged array at
        /// <paramref name="wc"/> (each element <see cref="IbvWc"/>-shaped), called once per
        /// notification from the background completion thread -- never in a tight busy-poll loop.
        /// Returns the number of completions written, 0 if none are ready, or negative on error.
        /// </summary>
        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ibv_poll_cq")]
        internal static extern int ibv_poll_cq(IntPtr cq, int numEntries, IntPtr wc);

        // ---- Queue pair: post send/recv ---------------------------------------------------------

        /// <summary>
        /// <c>int ibv_post_send(struct ibv_qp *qp, struct ibv_send_wr *wr, struct ibv_send_wr **bad_wr);</c>
        /// This transport always posts a single-element work-request chain (<c>wr.next == NULL</c>),
        /// so <paramref name="badWr"/> is purely a diagnostic out-param, never chased further.
        /// </summary>
        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ibv_post_send")]
        internal static extern int ibv_post_send(IntPtr qp, ref IbvSendWr wr, out IntPtr badWr);

        /// <summary>
        /// <c>int ibv_post_recv(struct ibv_qp *qp, struct ibv_recv_wr *wr, struct ibv_recv_wr **bad_wr);</c>
        /// Posted once per pre-allocated receive buffer at connection setup, and again every time a
        /// posted buffer's completion is consumed (a queue pair services exactly one
        /// <c>ibv_post_send</c> from the peer per outstanding <c>ibv_post_recv</c> here).
        /// </summary>
        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ibv_post_recv")]
        internal static extern int ibv_post_recv(IntPtr qp, ref IbvRecvWr wr, out IntPtr badWr);

        /// <summary>
        /// <c>int ibv_destroy_qp(struct ibv_qp *qp);</c> -- bound because the task's spec names it
        /// explicitly. NOT used on the primary code path in this transport: the queue pair here is
        /// created via <c>rdma_create_qp</c> (see <see cref="LibRdmaCmNativeMethods.rdma_create_qp"/>)
        /// so that RDMA CM's connection handshake can embed the correct queue-pair number in its
        /// wire messages, and RDMA CM then owns that queue pair's association with the connection
        /// ID -- tearing it down must go through the matching <c>rdma_destroy_qp</c>
        /// (<see cref="LibRdmaCmNativeMethods.rdma_destroy_qp"/>) instead, or librdmacm's internal
        /// bookkeeping for that connection ID desyncs. This raw <c>ibv_destroy_qp</c> binding is
        /// kept only for a queue pair created the other way (raw <c>ibv_create_qp</c>, not used
        /// here) -- see the design note in <see cref="RdmaQueuePairConnection"/>.
        /// </summary>
        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ibv_destroy_qp")]
        internal static extern int ibv_destroy_qp(IntPtr qp);

        // ---- Constants (from <infiniband/verbs.h> enums) ---------------------------------------

        /// <summary><c>enum ibv_access_flags</c> (partial -- only the bit this transport needs).</summary>
        internal static class IbvAccessFlags
        {
            internal const int LocalWrite = 1;
        }

        /// <summary><c>enum ibv_qp_type</c> (partial -- only the value this transport needs).</summary>
        internal static class IbvQpType
        {
            /// <summary>Reliable Connected -- the only queue-pair type this transport uses.</summary>
            internal const int Rc = 2;
        }

        /// <summary><c>enum ibv_wr_opcode</c> (partial -- only the value this transport uses: two-sided send, never RDMA read/write).</summary>
        internal static class IbvWrOpcode
        {
            internal const int Send = 0;
        }

        /// <summary><c>enum ibv_send_flags</c> (partial).</summary>
        internal static class IbvSendFlags
        {
            /// <summary>Request a completion-queue entry for this send -- this transport signals every send.</summary>
            internal const uint Signaled = 1 << 1;
        }

        /// <summary><c>enum ibv_wc_status</c> (partial -- 0 is the only "everything is fine" value; every other value is some failure mode).</summary>
        internal static class IbvWcStatus
        {
            internal const int Success = 0;
        }

        /// <summary><c>enum ibv_wc_opcode</c> (partial). Used to distinguish a completed send from a completed receive when draining <see cref="IbvWc"/> entries.</summary>
        internal static class IbvWcOpcode
        {
            internal const int Send = 0;
            internal const int Recv = 128;
        }
    }

    // ════════════════════════════════════════════════════════════════════════════════════════
    // Structs below mirror <infiniband/verbs.h> layouts. Field ORDER matters for
    // [StructLayout(LayoutKind.Sequential)] -- it must match the C struct exactly, padding
    // included. Padding fields inserted here are a best-effort guess at what a typical x86-64/
    // ARM64 Linux C compiler would insert for natural alignment; NOT verified against a real
    // compiler. See the file-level banner above.
    // ════════════════════════════════════════════════════════════════════════════════════════

    /// <summary><c>struct ibv_sge</c> -- one scatter/gather element referencing a registered memory region.</summary>
    [StructLayout(LayoutKind.Sequential)]
    internal struct IbvSge
    {
        internal ulong Addr;
        internal uint Length;
        internal uint LKey;
    }

    /// <summary>
    /// <c>struct ibv_send_wr</c> -- trimmed to the fields a plain <c>IBV_WR_SEND</c> (no
    /// immediate data, no RDMA read/write, no atomics) actually reads. The real struct's
    /// opcode-specific union (RDMA remote-addr/rkey, atomic compare-and-swap, UD address handle)
    /// is omitted entirely rather than approximated, since this transport never populates it --
    /// but omitting union members that a real send WR ~could~ carry means this layout is only
    /// valid for exactly the <c>IBV_WR_SEND</c> path this transport uses, not a general-purpose
    /// binding of the struct.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    internal struct IbvSendWr
    {
        internal ulong WrId;
        internal IntPtr Next;
        internal IntPtr SgList;
        internal int NumSge;
        internal int Opcode;
        internal uint SendFlags;
        internal uint ImmData;

        // The real struct's wr/qp/xrc union (RDMA remote addr/rkey, atomics, UD) follows here and
        // is large enough to matter for sizeof() -- approximated as a fixed-size reserved block
        // sized off the largest known variant (the RDMA-write variant: remote_addr(8) + rkey(4) +
        // 4 bytes padding = 16 bytes) since this transport never writes to it, only needs the
        // struct's total size to be at least as large as the real one so ibv_post_send does not
        // read past the end of a too-small allocation. THIS IS A GUESS -- verify sizeof(struct
        // ibv_send_wr) on the target system before trusting it.
        private ulong _reservedUnion0;
        private ulong _reservedUnion1;
    }

    /// <summary><c>struct ibv_recv_wr</c>.</summary>
    [StructLayout(LayoutKind.Sequential)]
    internal struct IbvRecvWr
    {
        internal ulong WrId;
        internal IntPtr Next;
        internal IntPtr SgList;
        internal int NumSge;
    }

    /// <summary>
    /// <c>struct ibv_wc</c> (work completion) -- field order and the two 16-bit tail fields
    /// (<c>pkey_index</c>/<c>slid</c>/<c>sl</c>/<c>dlid_path_bits</c>) are exactly the kind of
    /// detail that drifts across rdma-core versions; treat this as the least-trustworthy struct
    /// in this file.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    internal struct IbvWc
    {
        internal ulong WrId;
        internal int Status;
        internal int Opcode;
        internal uint VendorError;
        internal uint ByteLen;
        internal uint ImmDataOrInvalidatedRKey;
        internal uint QpNum;
        internal uint SrcQp;
        internal uint WcFlags;
        internal ushort PkeyIndex;
        internal ushort Slid;
        internal byte Sl;
        internal byte DlidPathBits;
        private readonly ushort _padding; // best-effort struct alignment guess -- see banner
    }

    /// <summary>
    /// <c>struct ibv_mr</c> (registered memory region handle) -- only bound to read back the
    /// <see cref="LKey"/> that <c>ibv_reg_mr</c> assigns, needed to fill in every
    /// <see cref="IbvSge"/> posted against that region. A small, long-stable struct (unlike
    /// <c>struct rdma_cm_id</c>), so read via a full <c>Marshal.PtrToStructure</c> rather than a
    /// raw fixed-offset read.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    internal struct IbvMr
    {
        internal IntPtr Context;
        internal IntPtr Pd;
        internal IntPtr Addr;
        internal nuint Length;
        internal uint Handle;
        internal uint LKey;
        internal uint RKey;
    }

    /// <summary><c>struct ibv_qp_cap</c>.</summary>
    [StructLayout(LayoutKind.Sequential)]
    internal struct IbvQpCap
    {
        internal uint MaxSendWr;
        internal uint MaxRecvWr;
        internal uint MaxSendSge;
        internal uint MaxRecvSge;
        internal uint MaxInlineData;
    }

    /// <summary>
    /// <c>struct ibv_qp_init_attr</c> -- passed to <see cref="LibRdmaCmNativeMethods.rdma_create_qp"/>
    /// (this transport creates its queue pair through RDMA CM, not raw <c>ibv_create_qp</c> -- see
    /// the design note on <c>ibv_destroy_qp</c> above).
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    internal struct IbvQpInitAttr
    {
        internal IntPtr QpContext;
        internal IntPtr SendCq;
        internal IntPtr RecvCq;
        internal IntPtr Srq;
        internal IbvQpCap Cap;
        internal int QpType;
        internal int SqSigAll;
    }
}
