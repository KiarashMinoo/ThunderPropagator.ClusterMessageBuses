using System.Runtime.InteropServices;

namespace ThunderPropagator.ClusterMessageBuses.Rdma
{
    /// <summary>
    /// Shared helper for reading and acknowledging one <c>rdma_cm_event</c> off an RDMA CM event
    /// channel, used both by the active/connect path in <see cref="RdmaQueuePairConnection"/> and
    /// by the persistent accept loop in <see cref="RdmaQueuePairListener"/>. Not named in the
    /// task's literal required-file list -- added because both of those need identical
    /// read-parse-ack logic and duplicating it would drift.
    /// </summary>
    internal static class RdmaCmEventReader
    {
        /// <summary>Parsed, already-acknowledged view of one <c>rdma_cm_event</c>.</summary>
        internal readonly record struct ParsedEvent(RdmaCmEventType Type, IntPtr Id, uint RemoteQpNum);

        /// <summary>
        /// Blocks (on a dedicated thread-pool thread, never the calling async continuation) until
        /// one event is available on <paramref name="eventChannel"/>, then immediately acks it
        /// (<c>rdma_ack_cm_event</c>) before returning -- callers only ever need the parsed values,
        /// never the raw event pointer, so acking eagerly here avoids every caller having to
        /// remember to do it.
        /// </summary>
        /// <remarks>
        /// <c>rdma_get_cm_event</c> is a blocking native call with no way to interrupt it via a
        /// managed <see cref="CancellationToken"/> -- if the caller's <c>cancellationToken</c> is
        /// cancelled (or a timeout elapses via <see cref="WaitForAsync"/>) while this call is
        /// in-flight, the underlying background thread-pool thread stays blocked in native code
        /// until an event actually arrives or the event channel is destroyed. Destroying the event
        /// channel (<c>rdma_destroy_event_channel</c>) is the only reliable way to unblock it, which
        /// is exactly what <see cref="RdmaQueuePairListener.DisposeAsync"/> does on shutdown.
        /// </remarks>
        internal static Task<ParsedEvent> ReadNextAsync(IntPtr eventChannel)
        {
            return Task.Run(() =>
            {
                var rc = LibRdmaCmNativeMethods.rdma_get_cm_event(eventChannel, out var eventPtr);
                if (rc != 0)
                    throw new InvalidOperationException($"rdma_get_cm_event failed (rc={rc}).");

                RdmaCmEvent native;
                try
                {
                    native = Marshal.PtrToStructure<RdmaCmEvent>(eventPtr);
                }
                finally
                {
                    // Ack even if PtrToStructure somehow throws -- an unacked event can wedge
                    // further delivery on this channel per rdma-core's own documentation.
                    LibRdmaCmNativeMethods.rdma_ack_cm_event(eventPtr);
                }

                return new ParsedEvent((RdmaCmEventType)native.Event, native.Id, native.Param.QpNum);
            });
        }

        /// <summary>
        /// Reads events off <paramref name="eventChannel"/> until one matching
        /// <paramref name="expected"/> arrives (returned), an error-shaped event type arrives
        /// (throws), or <paramref name="timeout"/> elapses (throws <see cref="TimeoutException"/>).
        /// For use during the short-lived, sequential handshake in
        /// <see cref="RdmaQueuePairConnection"/>'s active-connect path -- not for the listener's
        /// long-lived multiplexed loop, which needs to see every event, not just one expected kind.
        /// </summary>
        internal static async Task<ParsedEvent> WaitForAsync(IntPtr eventChannel, RdmaCmEventType expected, TimeSpan timeout)
        {
            var readTask = ReadNextAsync(eventChannel);
            var completed = await Task.WhenAny(readTask, Task.Delay(timeout)).ConfigureAwait(false);

            if (completed != readTask)
                throw new TimeoutException($"Timed out after {timeout} waiting for RDMA CM event '{expected}'.");

            var parsedEvent = await readTask.ConfigureAwait(false);
            if (parsedEvent.Type != expected)
            {
                throw new InvalidOperationException(
                    $"RDMA CM handshake failed: expected event '{expected}' but got '{parsedEvent.Type}'.");
            }

            return parsedEvent;
        }
    }
}
