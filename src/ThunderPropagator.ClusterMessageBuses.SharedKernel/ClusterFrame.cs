namespace ThunderPropagator.ClusterMessageBuses.SharedKernel
{
    /// <summary>
    /// The single wire envelope multiplexing every concern (fan-out, subscription-sync,
    /// request/reply, byte fan-out) a single-connection/shared-socket transport (Tcp, WebSocket,
    /// Udp, ZeroMQ, Swim, Rdma, Srd) sends over its one connection or socket. Consolidated from what
    /// used to be per-transport copies (<c>{Name}ClusterFrame</c>) with identical shapes.
    /// <see cref="PayloadJson"/>'s shape depends on <see cref="Kind"/>:
    /// <see cref="ClusterChannelEnvelope{TMessage}"/> for <see cref="ClusterFrameKind.FanOut"/>/
    /// <see cref="ClusterFrameKind.SubscriptionEvent"/>/<see cref="ClusterFrameKind.ByteFanOut"/>,
    /// <see cref="ClusterRequestEnvelope"/> for <see cref="ClusterFrameKind.Request"/>,
    /// <see cref="ClusterResponseEnvelope"/> for <see cref="ClusterFrameKind.Response"/>.
    /// </summary>
    public sealed record ClusterFrame(ClusterFrameKind Kind, string PayloadJson);

    /// <summary>
    /// Discriminates what a <see cref="ClusterFrame"/> carries -- shared across every transport that
    /// multiplexes fan-out, subscription-sync, request/reply, and byte fan-out traffic over one
    /// shared connection or socket (Tcp, WebSocket, Udp, ZeroMQ, Swim, Rdma, Srd), consolidated from
    /// what used to be per-transport copies (<c>{Name}ClusterFrameKind</c>) with identical values.
    /// Broker-based transports (Kafka, RabbitMQ, ...) don't need this -- their topic/queue/subject
    /// naming already discriminates kind, so they never construct a <see cref="ClusterFrame"/>.
    /// </summary>
    public enum ClusterFrameKind
    {
        FanOut,
        SubscriptionEvent,
        Request,
        Response,

        /// <summary>Byte-oriented counterpart to <see cref="FanOut"/> -- see <c>ClusterByteMessage</c>'s own doc comment.</summary>
        ByteFanOut
    }
}
