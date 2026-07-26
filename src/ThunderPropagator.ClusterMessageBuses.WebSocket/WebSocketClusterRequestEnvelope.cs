namespace ThunderPropagator.ClusterMessageBuses.WebSocket
{
    /// <summary>
    /// <see cref="WebSocketClusterFrame.PayloadJson"/> shape for <see cref="WebSocketClusterFrameKind.Request"/>.
    /// </summary>
    /// <param name="CorrelationId">
    /// Matches the eventual <see cref="WebSocketClusterResponseEnvelope.CorrelationId"/>.
    /// </param>
    /// <param name="Kind">Which operation is being requested.</param>
    /// <param name="ChannelName">Identifies the channel by name for <see cref="WebSocketClusterRequestKind.RestoreSnapshot"/> and <see cref="WebSocketClusterRequestKind.SyncDelta"/>.</param>
    /// <param name="ChannelKey">Identifies the channel by key for <see cref="WebSocketClusterRequestKind.FetchSubscriptions"/>.</param>
    /// <param name="SinceTicks"><see cref="DateTimeOffset.UtcTicks"/> cutoff for <see cref="WebSocketClusterRequestKind.SyncDelta"/>.</param>
    /// <remarks>
    /// Unlike every broker transport in this repo, there is no <c>ReplyToNodeEndpoint</c> field:
    /// the answering side writes its <see cref="WebSocketClusterResponseEnvelope"/> back over the
    /// exact same physical <see cref="System.Net.WebSockets.WebSocket"/> connection the request
    /// arrived on (a WebSocket is inherently a bidirectional stream, so the connection itself is
    /// the reply route — there is no need to derive or trust a separate reply address).
    /// </remarks>
    internal sealed record WebSocketClusterRequestEnvelope(
        Guid CorrelationId,
        WebSocketClusterRequestKind Kind,
        string? ChannelName,
        Guid? ChannelKey,
        long? SinceTicks);
}
