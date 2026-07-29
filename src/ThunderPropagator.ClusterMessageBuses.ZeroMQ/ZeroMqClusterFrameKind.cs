namespace ThunderPropagator.ClusterMessageBuses.ZeroMQ
{
    /// <summary>Discriminates the single JSON payload frame every ROUTER/DEALER message carries.</summary>
    internal enum ZeroMqClusterFrameKind
    {
        FanOut,
        SubscriptionEvent,
        Request,
        Response,
    }
}
