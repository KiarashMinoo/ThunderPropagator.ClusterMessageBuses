namespace ThunderPropagator.ClusterMessageBuses.ZeroMQ
{
    /// <summary>Options controlling <see cref="ZeroMqClusterMessageBus"/>'s ROUTER/DEALER sockets.</summary>
    public sealed class ClusterZeroMqOptions
    {
        /// <summary>How long a leader/peer-pull request (restore/sync-delta/fetch-subscriptions) waits before timing out.</summary>
        public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(30);

        /// <summary>ZMQ_SNDHWM applied to every outbound DEALER socket — caps how many outstanding outbound messages are buffered per peer before <c>Send</c> starts blocking/dropping.</summary>
        public int? SendHighWatermark { get; set; }

        /// <summary>ZMQ_RCVHWM applied to the ROUTER socket and every outbound DEALER socket.</summary>
        public int? ReceiveHighWatermark { get; set; }

        /// <summary>ZMQ_LINGER applied to every socket — how long a pending send is allowed to drain before a closing socket discards it. Defaults to zero (discard immediately) so shutdown is never blocked by an unreachable peer.</summary>
        public TimeSpan Linger { get; set; } = TimeSpan.Zero;

        /// <summary>ZMQ_HEARTBEAT_IVL applied to every socket, so a peer that silently drops the connection is detected instead of appearing alive indefinitely.</summary>
        public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(30);
    }
}
