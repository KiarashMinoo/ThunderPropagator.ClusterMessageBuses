using System.Net;
using System.Runtime.CompilerServices;

namespace ThunderPropagator.ClusterMessageBuses.Rdma
{
    /// <summary>
    /// Production <see cref="IRdmaQueuePairListener"/>: wraps a real RDMA CM listening
    /// <c>rdma_cm_id</c> bound to every local interface on
    /// <see cref="RdmaClusterMessageBusOptions.Port"/>. Mirrors <c>TcpListenerClusterListener</c>'s
    /// shape. Not named in the task's literal required-file list -- see
    /// <see cref="IRdmaQueuePairListener"/>'s doc comment for why it was added anyway.
    /// </summary>
    /// <remarks>
    /// Every accepted child connection id shares this listener's event channel (this transport
    /// does not use <c>rdma_migrate_id</c> to give each connection its own channel/thread for CM
    /// events -- only the data-path completion channel is per-connection, per the task's
    /// requirement). One consequence: <see cref="RdmaQueuePairConnection.Accept"/> does not itself
    /// wait for that connection's <see cref="RdmaCmEventType.Established"/> event (unlike the
    /// active/connect side, which does) -- on the passive/accepting side, RDMA CM transitions the
    /// local queue pair through the necessary INIT/RTR/RTS states as part of <c>rdma_accept</c>
    /// itself, so the connection is already usable for local sends once <c>rdma_accept</c> returns
    /// successfully; the later <c>ESTABLISHED</c> event for that child id, when it eventually
    /// arrives on this shared channel, is observed by the loop below and simply ignored. THIS IS A
    /// SIMPLIFICATION that needs confirming against real hardware/rdma-core behavior, like
    /// everything else in this transport.
    /// </remarks>
    internal sealed class RdmaQueuePairListener : IRdmaQueuePairListener
    {
        private const short AfInet = 2; // AF_INET on Linux.

        private readonly IntPtr _eventChannel;
        private readonly IntPtr _listenId;
        private readonly RdmaClusterMessageBusOptions _options;
        private bool _disposed;

        internal RdmaQueuePairListener(RdmaClusterMessageBusOptions options)
        {
            _options = options;

            _eventChannel = LibRdmaCmNativeMethods.rdma_create_event_channel();
            if (_eventChannel == IntPtr.Zero)
            {
                throw new InvalidOperationException(
                    "rdma_create_event_channel failed -- confirm librdmacm.so.1 is installed and this host has an RDMA-capable device.");
            }

            var createRc = LibRdmaCmNativeMethods.rdma_create_id(_eventChannel, out _listenId, IntPtr.Zero, LibRdmaCmNativeMethods.RdmaPortSpace.Tcp);
            if (createRc != 0)
            {
                LibRdmaCmNativeMethods.rdma_destroy_event_channel(_eventChannel);
                throw new InvalidOperationException($"rdma_create_id failed (rc={createRc}).");
            }

            var anyAddress = new SockAddrIn
            {
                SinFamily = AfInet,
                SinPort = (ushort)IPAddress.HostToNetworkOrder((short)options.Port),
                SinAddr = 0, // INADDR_ANY -- bind every local interface, mirroring TcpListenerClusterListener's IPAddress.Any.
                SinZero = new byte[8],
            };

            var bindRc = LibRdmaCmNativeMethods.rdma_bind_addr(_listenId, ref anyAddress);
            if (bindRc != 0)
            {
                LibRdmaCmNativeMethods.rdma_destroy_id(_listenId);
                LibRdmaCmNativeMethods.rdma_destroy_event_channel(_eventChannel);
                throw new InvalidOperationException($"rdma_bind_addr failed (rc={bindRc}) for port {options.Port}.");
            }

            const int backlog = 128;
            var listenRc = LibRdmaCmNativeMethods.rdma_listen(_listenId, backlog);
            if (listenRc != 0)
            {
                LibRdmaCmNativeMethods.rdma_destroy_id(_listenId);
                LibRdmaCmNativeMethods.rdma_destroy_event_channel(_eventChannel);
                throw new InvalidOperationException($"rdma_listen failed (rc={listenRc}).");
            }
        }

        public async IAsyncEnumerable<IRdmaQueuePairConnection> AcceptConnectionsAsync([EnumeratorCancellation] CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                RdmaCmEventReader.ParsedEvent parsedEvent;
                try
                {
                    parsedEvent = await RdmaCmEventReader.ReadNextAsync(_eventChannel).ConfigureAwait(false);
                }
                catch (Exception)
                {
                    // Expected once DisposeAsync destroys the event channel to unblock the native
                    // rdma_get_cm_event call this is parked in -- see DisposeAsync's remarks.
                    yield break;
                }

                if (parsedEvent.Type != RdmaCmEventType.ConnectRequest)
                {
                    // Belongs to an already-yielded connection sharing this channel (its own
                    // ESTABLISHED event, a DISCONNECTED notification, etc.) -- this first-pass
                    // design does not correlate those back to a specific RdmaQueuePairConnection
                    // (that would need rdma_migrate_id to give each one an independent channel);
                    // a disconnected peer's connection is instead noticed the ordinary way, through
                    // its own completion thread/queue pair going unusable on the next send.
                    continue;
                }

                IRdmaQueuePairConnection connection;
                try
                {
                    connection = RdmaQueuePairConnection.Accept(parsedEvent.Id, _eventChannel, _options);
                }
                catch (Exception)
                {
                    // One bad inbound connection attempt must not stop the listener -- mirrors
                    // TcpListenerClusterListener's per-iteration exception handling.
                    continue;
                }

                yield return connection;
            }
        }

        public ValueTask DisposeAsync()
        {
            if (_disposed)
                return ValueTask.CompletedTask;
            _disposed = true;

            try { LibRdmaCmNativeMethods.rdma_destroy_id(_listenId); } catch { /* best-effort */ }

            // Destroying the event channel is what unblocks the native rdma_get_cm_event call that
            // AcceptConnectionsAsync's RdmaCmEventReader.ReadNextAsync is parked in -- there is no
            // CancellationToken-based way to interrupt it directly. Must happen after rdma_destroy_id
            // above (a still-live id referencing an already-destroyed channel is undefined behavior).
            try { LibRdmaCmNativeMethods.rdma_destroy_event_channel(_eventChannel); } catch { /* best-effort */ }

            return ValueTask.CompletedTask;
        }
    }
}
