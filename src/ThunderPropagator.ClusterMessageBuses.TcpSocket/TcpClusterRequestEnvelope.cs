namespace ThunderPropagator.ClusterMessageBuses.TcpSocket
{
    /// <summary>
    /// <see cref="TcpClusterFrame.PayloadJson"/> shape for <see cref="TcpClusterFrameKind.Request"/>.
    /// </summary>
    /// <remarks>
    /// Like the WebSocket transport, and unlike every broker transport in this repo, there is no
    /// <c>ReplyToNodeEndpoint</c> field: the answering side writes its
    /// <see cref="TcpClusterResponseEnvelope"/> back over the exact same physical TCP connection the
    /// request arrived on — a TCP connection is inherently a bidirectional stream, so the connection
    /// itself is the reply route.
    /// </remarks>
    internal sealed record TcpClusterRequestEnvelope(
        Guid CorrelationId,
        TcpClusterRequestKind Kind,
        string? ChannelName,
        Guid? ChannelKey,
        long? SinceTicks);
}
