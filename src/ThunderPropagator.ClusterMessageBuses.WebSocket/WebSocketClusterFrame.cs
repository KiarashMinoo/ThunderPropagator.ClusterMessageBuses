namespace ThunderPropagator.ClusterMessageBuses.WebSocket
{
    /// <summary>
    /// The single wire envelope sent over every WebSocket connection this transport opens or
    /// accepts. <see cref="PayloadJson"/>'s shape depends on <see cref="Kind"/>:
    /// <see cref="WebSocketFanOutPayload"/> for <see cref="WebSocketClusterFrameKind.FanOut"/>,
    /// <see cref="WebSocketSubscriptionEventPayload"/> for <see cref="WebSocketClusterFrameKind.SubscriptionEvent"/>,
    /// <see cref="WebSocketClusterRequestEnvelope"/> for <see cref="WebSocketClusterFrameKind.Request"/>,
    /// <see cref="WebSocketClusterResponseEnvelope"/> for <see cref="WebSocketClusterFrameKind.Response"/>.
    /// </summary>
    internal sealed record WebSocketClusterFrame(WebSocketClusterFrameKind Kind, string PayloadJson);
}
