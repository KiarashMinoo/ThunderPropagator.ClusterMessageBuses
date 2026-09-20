namespace ThunderPropagator.ClusterMessageBuses.WebApi
{
    /// <summary>Which cluster operation an inbound HTTP request on this node's embedded listener is asking for.</summary>
    internal enum WebApiRouteKind
    {
        FanOut,
        SubscriptionEvent,
        Snapshot,
        SnapshotDelta,
        SubscriptionsSelf,

        /// <summary>Byte-oriented counterpart to <see cref="FanOut"/> -- see <c>ClusterByteMessage</c>'s own doc comment.</summary>
        ByteFanOut,

        /// <summary>Byte-oriented, channel-agnostic counterpart to <see cref="Snapshot"/>, routed by channel key rather than channel name -- backs <c>IClusterMessageBus.PullSnapshotAsync</c>.</summary>
        ByteSnapshot
    }
}
