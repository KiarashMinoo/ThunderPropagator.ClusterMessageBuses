namespace ThunderPropagator.ClusterMessageBuses.Srd
{
    /// <summary>
    /// The single wire envelope sent as one whole SRD message via <c>fi_send</c>. Like UDP (and
    /// unlike TCP's byte stream), libfabric's RDM endpoint preserves message boundaries -- a single
    /// <c>fi_send</c> call is delivered to the peer's matching <c>fi_recv</c> whole or not at all, so
    /// no additional framing beyond this envelope is needed. <see cref="PayloadJson"/>'s shape depends
    /// on <see cref="Kind"/>: a
    /// <see cref="ThunderPropagator.ClusterMessageBuses.SharedKernel.ClusterChannelEnvelope{TMessage}"/>
    /// (wrapping a <c>ClusterFanOutMessage</c>) for <see cref="SrdClusterFrameKind.FanOut"/>, the same
    /// envelope (wrapping a <c>ClusterSubscriptionEvent</c>) for
    /// <see cref="SrdClusterFrameKind.SubscriptionEvent"/>, a
    /// <see cref="ThunderPropagator.ClusterMessageBuses.SharedKernel.ClusterRequestEnvelope"/> for
    /// <see cref="SrdClusterFrameKind.Request"/>, a
    /// <see cref="ThunderPropagator.ClusterMessageBuses.SharedKernel.ClusterResponseEnvelope"/> for
    /// <see cref="SrdClusterFrameKind.Response"/>, the same channel envelope (wrapping a
    /// <c>ClusterByteMessage</c>) for <see cref="SrdClusterFrameKind.ByteFanOut"/>.
    /// </summary>
    /// <remarks>
    /// Kept as its own local record (unlike every payload type it carries, which have all been
    /// migrated to their <c>ThunderPropagator.ClusterMessageBuses.SharedKernel</c> equivalents):
    /// <see cref="ISrdClusterEndpoint"/>'s <c>SendFrameAsync</c>/<c>ReceiveFramesAsync</c> signatures
    /// reference this type directly, and this transport's native-interop <c>Endpoint/</c> folder is
    /// out of scope for this consolidation.
    /// </remarks>
    internal sealed record SrdClusterFrame(SrdClusterFrameKind Kind, string PayloadJson);
}
