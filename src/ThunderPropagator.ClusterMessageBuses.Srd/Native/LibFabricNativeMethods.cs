using System.Runtime.InteropServices;

namespace ThunderPropagator.ClusterMessageBuses.Srd
{
    // ═══════════════════════════════════════════════════════════════════════════════════════════
    // ██  WARNING — UNVERIFIED NATIVE INTEROP — READ BEFORE TOUCHING ANYTHING BELOW  ██
    // ═══════════════════════════════════════════════════════════════════════════════════════════
    //
    // Everything in this file is a best-effort approximation of declarations from the public
    // libfabric headers:
    //
    //     <rdma/fabric.h>       -- fi_info, fi_getinfo, fi_fabric, fi_close, fi_freeinfo, caps/mode bits
    //     <rdma/fi_domain.h>    -- fi_domain, fi_domain_attr, fi_cq_open, fi_cq_attr, fi_cq_data_entry,
    //                              fi_av_open, fi_av_attr, fi_av_insert
    //     <rdma/fi_endpoint.h>  -- fi_endpoint, fi_ep_attr, fi_ep_bind, fi_enable, fi_send, fi_recv, fi_msg
    //     <rdma/fi_cm.h>        -- (address-management declarations; not directly bound here — this
    //                              transport does not use libfabric's connection-manager calls, since
    //                              the RDM endpoint type over EFA is connectionless)
    //
    // This code has NOT been compiled, linked, or run against a real libfabric.so.1 — there is no
    // dotnet SDK, no C compiler, and no EFA hardware available in the environment that wrote it. It
    // was written purely from documentation/memory of the public API surface. Concretely, that means:
    //
    //   1. STRUCT FIELD OFFSETS AND SIZES ARE NOT GUARANTEED CORRECT. The real `fi_info` struct in
    //      particular has had fields added/reordered across libfabric releases — a wrong field order,
    //      a missing field, or a wrong field WIDTH (e.g. `size_t` marshaled as `uint` instead of
    //      `nuint`) shifts every field after it and corrupts memory on the native side. This is a
    //      correctness-critical, not cosmetic, risk: a bad struct layout can crash the process, or
    //      worse, silently corrupt unrelated native memory.
    //   2. Every struct below MUST be checked field-by-field against the actual installed
    //      `libfabric-dev` / `libfabric-devel` headers on the target EC2 instance before this code is
    //      trusted with anything beyond a first smoke test. Prefer generating the layout from the
    //      real headers (e.g. via a small `pahole`/`clang -Xclang -fdump-record-layouts` check, or a
    //      tiny reference C program that prints `sizeof`/`offsetof` for each field) over trusting the
    //      comments in this file.
    //   3. Struct fields this project never reads or writes are deliberately left as opaque `IntPtr`
    //      placeholders (see `fi_domain_attr`/`fi_tx_attr`/`fi_rx_attr`/`fi_nic`, which are represented
    //      only as untyped pointers inside `fi_info`, never dereferenced) rather than being modeled in
    //      full, to shrink the surface that has to be verified. If a future change needs to read one of
    //      those, model it fully first.
    //   4. Enum numeric values (e.g. `FI_EP_RDM`, `FI_AV_TABLE`, `FI_CQ_FORMAT_DATA`) and capability
    //      bitmask values (e.g. `FI_MSG`, `FI_SEND`, `FI_RECV`) are written from memory of the public
    //      headers and are marked individually below where confidence is lowest — cross-check every
    //      one against `<rdma/fabric.h>` / `<rdma/fi_domain.h>` / `<rdma/fi_endpoint.h>` directly.
    //
    // EFA AVAILABILITY: the `efa` provider is only present on AWS EC2 instance types with an Elastic
    // Fabric Adapter physically attached. On every other host — including this sandbox, any non-AWS
    // machine, or an AWS instance without EFA — `fi_getinfo` will simply return no matching results
    // for the `efa` provider (not an error, just an empty/non-matching result). Calling code
    // (`Endpoint/LibFabricSrdClusterEndpoint.cs`) is responsible for detecting that case explicitly and
    // throwing a clear `InvalidOperationException` explaining that EFA/libfabric was not found, rather
    // than letting a cryptic P/Invoke or null-pointer failure surface instead.
    //
    // Scope note: only the functions this transport actually calls are declared below (fi_getinfo,
    // fi_fabric, fi_domain, fi_cq_open, fi_av_open, fi_av_insert, fi_endpoint, fi_ep_bind, fi_enable,
    // fi_send, fi_recv, fi_cq_read, fi_cq_sread, fi_close, fi_freeinfo) — deliberately NOT
    // fi_cq_readfrom/fi_cq_sreadfrom (which would surface a completion's source address) or fi_getname
    // (which would read this endpoint's own local address). See
    // `Wire/SrdClusterRequestEnvelope.cs`'s remarks for how this transport works around not having
    // those two.
    // ═══════════════════════════════════════════════════════════════════════════════════════════

    /// <summary>
    /// Raw <c>DllImport</c> bindings and struct approximations for the subset of libfabric's C API
    /// this transport uses. See the warning banner at the top of this file — nothing here has been
    /// validated against a real compiler, linker, or libfabric installation.
    /// </summary>
    internal static class LibFabricNativeMethods
    {
        private const string LibFabric = "libfabric.so.1";

        /// <summary>
        /// libfabric API version this code was written against, packed via <c>FI_VERSION(major, minor)</c>
        /// (<c>(major &lt;&lt; 16) | minor</c> per <c>&lt;rdma/fabric.h&gt;</c>). 1.18 is a reasonably
        /// recent, AWS-EFA-documented libfabric release as of this writing — verify against whatever
        /// version is actually installed via <c>fi_info -v</c> on the target instance and adjust if
        /// the requested API version is rejected.
        /// </summary>
        internal const uint FI_VERSION_MAJOR = 1;
        internal const uint FI_VERSION_MINOR = 18;
        internal static readonly uint FiVersion = (FI_VERSION_MAJOR << 16) | FI_VERSION_MINOR;

        // ── fi_info.caps / mode bits (<rdma/fabric.h>) ──────────────────────────────────────────
        // LOW CONFIDENCE: exact bit positions from memory, not re-derived from a header. Basic
        // message-queue send/receive capabilities. Re-check against the real enum/#define values.
        internal const ulong FI_MSG = 1UL << 1;
        internal const ulong FI_SEND = 1UL << 8;
        internal const ulong FI_RECV = 1UL << 9;

        // ── enum fi_ep_type (<rdma/fabric.h>) ───────────────────────────────────────────────────
        // { FI_EP_UNSPEC = 0, FI_EP_MSG, FI_EP_DGRAM, FI_EP_RDM, FI_EP_SOCK_STREAM, FI_EP_SOCK_DGRAM }
        internal const int FI_EP_UNSPEC = 0;
        internal const int FI_EP_RDM = 3;

        // ── enum fi_av_type (<rdma/fi_domain.h>) ────────────────────────────────────────────────
        internal const int FI_AV_MAP = 1;
        internal const int FI_AV_TABLE = 2;

        // ── enum fi_cq_format (<rdma/fi_domain.h>) ──────────────────────────────────────────────
        // { FI_CQ_FORMAT_UNSPEC = 0, FI_CQ_FORMAT_CONTEXT, FI_CQ_FORMAT_MSG, FI_CQ_FORMAT_DATA, FI_CQ_FORMAT_TAGGED }
        internal const int FI_CQ_FORMAT_DATA = 3;

        // ── enum fi_wait_obj (<rdma/fabric.h>) ──────────────────────────────────────────────────
        // LOW CONFIDENCE on exact ordering — this transport requests FI_WAIT_NONE / FI_WAIT_UNSPEC
        // (no OS wait object; polled via fi_cq_sread's own timeout instead), so the exact numeric
        // value mostly matters only insofar as it must NOT accidentally select a wait-set/fd mode.
        internal const int FI_WAIT_NONE = 0;
        internal const int FI_WAIT_UNSPEC = 1;

        /// <summary>
        /// Approximation of <c>struct fi_fabric_attr</c> (&lt;rdma/fi_domain.h&gt;). This project only
        /// ever sets/reads <see cref="prov_name"/>; the rest exist purely so the struct's total size
        /// roughly matches the real one when embedded (by pointer) in <see cref="fi_info"/>.
        /// </summary>
        [StructLayout(LayoutKind.Sequential)]
        internal struct fi_fabric_attr
        {
            /// <summary>struct fid_fabric* — populated by libfabric on the fi_getinfo result; irrelevant in hints.</summary>
            public IntPtr fabric;
            /// <summary>char* — human-readable fabric name; irrelevant in hints, left null.</summary>
            public IntPtr name;
            /// <summary>char* — provider name filter, e.g. "efa". Marshaled manually (see <see cref="LibFabricSrdClusterEndpoint"/>).</summary>
            public IntPtr prov_name;
            public uint prov_version;
            public uint api_version;
        }

        /// <summary>
        /// Approximation of <c>struct fi_ep_attr</c> (&lt;rdma/fi_endpoint.h&gt;). This project only
        /// sets <see cref="type"/> (to <see cref="FI_EP_RDM"/>) in hints and reads
        /// <see cref="max_msg_size"/> back from the result, if available.
        /// </summary>
        [StructLayout(LayoutKind.Sequential)]
        internal struct fi_ep_attr
        {
            public int type;
            public uint protocol;
            public uint protocol_version;
            public UIntPtr max_msg_size;
            public UIntPtr msg_prefix_size;
            public UIntPtr max_order_raw_size;
            public UIntPtr max_order_war_size;
            public UIntPtr max_order_waw_size;
            public ulong mem_tag_format;
            public UIntPtr tx_ctx_cnt;
            public UIntPtr rx_ctx_cnt;
            public UIntPtr auth_key_size;
            public IntPtr auth_key;
        }

        /// <summary>
        /// Approximation of <c>struct fi_info</c> (&lt;rdma/fabric.h&gt;) — the most likely struct in
        /// this file to have drifted from whatever libfabric version is actually installed; see the
        /// file banner. Fields this project never reads/writes (<c>tx_attr</c>, <c>rx_attr</c>,
        /// <c>domain_attr</c>, <c>nic</c>) are left as opaque <see cref="IntPtr"/> placeholders rather
        /// than fully modeled sub-structs.
        /// </summary>
        [StructLayout(LayoutKind.Sequential)]
        internal struct fi_info
        {
            /// <summary>struct fi_info* — next entry in the linked list fi_getinfo returns.</summary>
            public IntPtr next;
            public ulong caps;
            public ulong mode;
            public uint addr_format;
            public UIntPtr src_addrlen;
            public UIntPtr dest_addrlen;
            public IntPtr src_addr;
            public IntPtr dest_addr;
            /// <summary>fid_t (struct fid*) — opaque handle; approximated as IntPtr, never dereferenced directly.</summary>
            public IntPtr handle;
            /// <summary>struct fi_tx_attr* — opaque placeholder, not modeled (not read/written by this project).</summary>
            public IntPtr tx_attr;
            /// <summary>struct fi_rx_attr* — opaque placeholder, not modeled (not read/written by this project).</summary>
            public IntPtr rx_attr;
            /// <summary>struct fi_ep_attr* — allocated/populated as <see cref="fi_ep_attr"/> by this project.</summary>
            public IntPtr ep_attr;
            /// <summary>struct fi_domain_attr* — opaque placeholder, not modeled (not read/written by this project).</summary>
            public IntPtr domain_attr;
            /// <summary>struct fi_fabric_attr* — allocated/populated as <see cref="fi_fabric_attr"/> by this project.</summary>
            public IntPtr fabric_attr;
            /// <summary>struct fi_nic* — opaque placeholder, not modeled (not read/written by this project).</summary>
            public IntPtr nic;
        }

        /// <summary>Approximation of <c>struct fi_cq_attr</c> (&lt;rdma/fi_domain.h&gt;).</summary>
        [StructLayout(LayoutKind.Sequential)]
        internal struct fi_cq_attr
        {
            public UIntPtr size;
            public ulong flags;
            public int format;
            public int wait_obj;
            public int signaling_vector;
            public int wait_cond;
            public IntPtr wait_set;
        }

        /// <summary>
        /// Approximation of <c>struct fi_cq_data_entry</c> (&lt;rdma/fi_domain.h&gt;) — the completion
        /// format requested via <see cref="fi_cq_attr.format"/> = <see cref="FI_CQ_FORMAT_DATA"/>.
        /// <see cref="op_context"/> is the same pointer passed as the <c>context</c> argument to
        /// <see cref="fi_send"/>/<see cref="fi_recv"/>, used here to correlate a completion back to the
        /// pending send or posted receive buffer it belongs to (see
        /// <see cref="LibFabricSrdClusterEndpoint"/>).
        /// </summary>
        [StructLayout(LayoutKind.Sequential)]
        internal struct fi_cq_data_entry
        {
            public IntPtr op_context;
            public ulong flags;
            public UIntPtr len;
            public IntPtr buf;
            public ulong data;
        }

        /// <summary>
        /// Approximation of <c>struct fi_msg</c> (&lt;rdma/fi_endpoint.h&gt;). Not currently used by
        /// this transport — <see cref="fi_send"/>/<see cref="fi_recv"/> (the simple, non-"msg" calls)
        /// are used instead, per this transport's "keep it simple, one message type, basic ops, not
        /// tagged messaging" scope. Declared for documentation completeness / a future move to
        /// <c>fi_sendmsg</c>/<c>fi_recvmsg</c> (e.g. to attach per-message flags).
        /// </summary>
        [StructLayout(LayoutKind.Sequential)]
        internal struct fi_msg
        {
            /// <summary>struct iovec* — scatter/gather list; this project would only ever use a single-element list.</summary>
            public IntPtr msg_iov;
            public IntPtr desc;
            public UIntPtr iov_count;
            public ulong addr;
            public IntPtr context;
            public ulong data;
        }

        /// <summary>Approximation of <c>struct fi_av_attr</c> (&lt;rdma/fi_domain.h&gt;).</summary>
        [StructLayout(LayoutKind.Sequential)]
        internal struct fi_av_attr
        {
            public int type;
            public int rx_ctx_bits;
            public UIntPtr count;
            public UIntPtr ep_per_node;
            public IntPtr name;
            public ulong flags;
        }

        // ── fi_getinfo / fi_freeinfo ─────────────────────────────────────────────────────────────

        [DllImport(LibFabric, CallingConvention = CallingConvention.Cdecl)]
        internal static extern int fi_getinfo(
            uint version,
            IntPtr node,
            IntPtr service,
            ulong flags,
            ref fi_info hints,
            out IntPtr info);

        [DllImport(LibFabric, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void fi_freeinfo(IntPtr info);

        // ── fabric / domain / cq / av / endpoint open ────────────────────────────────────────────

        [DllImport(LibFabric, CallingConvention = CallingConvention.Cdecl)]
        internal static extern int fi_fabric(IntPtr fabricAttr, out IntPtr fabric, IntPtr context);

        [DllImport(LibFabric, CallingConvention = CallingConvention.Cdecl)]
        internal static extern int fi_domain(IntPtr fabric, IntPtr info, out IntPtr domain, IntPtr context);

        [DllImport(LibFabric, CallingConvention = CallingConvention.Cdecl)]
        internal static extern int fi_cq_open(IntPtr domain, ref fi_cq_attr attr, out IntPtr cq, IntPtr context);

        [DllImport(LibFabric, CallingConvention = CallingConvention.Cdecl)]
        internal static extern int fi_av_open(IntPtr domain, ref fi_av_attr attr, out IntPtr av, IntPtr context);

        [DllImport(LibFabric, CallingConvention = CallingConvention.Cdecl)]
        internal static extern int fi_endpoint(IntPtr domain, IntPtr info, out IntPtr ep, IntPtr context);

        // ── bind / enable ────────────────────────────────────────────────────────────────────────

        [DllImport(LibFabric, CallingConvention = CallingConvention.Cdecl)]
        internal static extern int fi_ep_bind(IntPtr ep, IntPtr bindFid, ulong flags);

        [DllImport(LibFabric, CallingConvention = CallingConvention.Cdecl)]
        internal static extern int fi_enable(IntPtr ep);

        // ── address vector ───────────────────────────────────────────────────────────────────────

        [DllImport(LibFabric, CallingConvention = CallingConvention.Cdecl)]
        internal static extern int fi_av_insert(IntPtr av, IntPtr addr, UIntPtr count, IntPtr fiAddr, ulong flags, IntPtr context);

        // ── data transfer ────────────────────────────────────────────────────────────────────────

        [DllImport(LibFabric, CallingConvention = CallingConvention.Cdecl)]
        internal static extern nint fi_send(IntPtr ep, IntPtr buf, UIntPtr len, IntPtr desc, ulong destAddr, IntPtr context);

        [DllImport(LibFabric, CallingConvention = CallingConvention.Cdecl)]
        internal static extern nint fi_recv(IntPtr ep, IntPtr buf, UIntPtr len, IntPtr desc, ulong srcAddr, IntPtr context);

        // ── completion queue ─────────────────────────────────────────────────────────────────────

        [DllImport(LibFabric, CallingConvention = CallingConvention.Cdecl)]
        internal static extern nint fi_cq_read(IntPtr cq, IntPtr buf, UIntPtr count);

        [DllImport(LibFabric, CallingConvention = CallingConvention.Cdecl)]
        internal static extern nint fi_cq_sread(IntPtr cq, IntPtr buf, UIntPtr count, IntPtr cond, int timeoutMs);

        // ── teardown ─────────────────────────────────────────────────────────────────────────────

        [DllImport(LibFabric, CallingConvention = CallingConvention.Cdecl)]
        internal static extern int fi_close(IntPtr fid);
    }
}
