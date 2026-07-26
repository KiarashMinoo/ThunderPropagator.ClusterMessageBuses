namespace ThunderPropagator.ClusterMessageBuses.WebApi
{
    /// <summary>Which cluster operation an inbound HTTP request on this node's embedded listener is asking for.</summary>
    internal enum WebApiRouteKind
    {
        FanOut,
        SubscriptionEvent,
        Snapshot,
        SnapshotDelta,
        SubscriptionsSelf
    }
}
