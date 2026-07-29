namespace ThunderPropagator.ClusterMessageBuses.ZeroMQ
{
    /// <summary>
    /// The single NJson-serialized payload frame carried by every DEALER→ROUTER or ROUTER→DEALER
    /// message in this transport — every one of fan-out, subscription-sync, request, and response
    /// rides over the same physical per-peer connection, so a frame-kind discriminator is required to
    /// tell them apart (mirrors WebSocketClusterFrame's exact reasoning).
    /// </summary>
    internal sealed record ZeroMqClusterFrame(ZeroMqClusterFrameKind Kind, string PayloadJson);
}
