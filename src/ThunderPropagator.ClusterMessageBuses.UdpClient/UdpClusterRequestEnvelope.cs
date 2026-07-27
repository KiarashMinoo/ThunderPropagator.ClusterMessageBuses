namespace ThunderPropagator.ClusterMessageBuses.UdpClient
{
    /// <summary>
    /// <see cref="UdpClusterFrame.PayloadJson"/> shape for <see cref="UdpClusterFrameKind.Request"/>.
    /// </summary>
    /// <remarks>
    /// Like the WebSocket/TcpSocket transports, there is no <c>ReplyToNodeEndpoint</c> field — but
    /// for a different reason than either of them: since UDP is connectionless, there is no
    /// persistent connection object to write a reply back over in the first place. Instead, the
    /// answering side simply sends its <see cref="UdpClusterResponseEnvelope"/> datagram back to
    /// whatever <see cref="System.Net.IPEndPoint"/> this request datagram was actually received
    /// from (captured by the receive loop, not carried in this envelope) — the one shared local UDP
    /// socket every node binds is used for both directions regardless of which peer it's talking to.
    /// </remarks>
    internal sealed record UdpClusterRequestEnvelope(
        Guid CorrelationId,
        UdpClusterRequestKind Kind,
        string? ChannelName,
        Guid? ChannelKey,
        long? SinceTicks);
}
