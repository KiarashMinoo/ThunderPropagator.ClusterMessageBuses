namespace ThunderPropagator.ClusterMessageBuses.WebSocket
{
    /// <summary>
    /// Discriminates the single kind of message multiplexed over one physical WebSocket connection
    /// between two nodes. Unlike the broker transports in this repo (one topic/channel per concern),
    /// a WebSocket connection is one bidirectional stream, so every concern — fan-out, subscription
    /// events, and the hand-rolled request/reply RPC — shares it, distinguished by this discriminator.
    /// </summary>
    internal enum WebSocketClusterFrameKind
    {
        FanOut,
        SubscriptionEvent,
        Request,
        Response
    }
}
