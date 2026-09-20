namespace ThunderPropagator.ClusterMessageBuses.Swim
{
    /// <summary>
    /// <see cref="SwimDatagram.PayloadJson"/> shape for <see cref="SwimMessageKind.Ack"/> -- sent
    /// directly back to whichever <see cref="System.Net.IPEndPoint"/> a <see cref="SwimPingPayload"/>
    /// was received from (the receive loop captures it, exactly like the request/reply envelopes
    /// below), whether that sender was the original prober or a relay acting on a
    /// <see cref="SwimPingReqPayload"/>. A relay that receives this ack for a probe it is relaying
    /// forwards a copy of it on to the original prober's endpoint, preserving <see cref="SourceHost"/>
    /// and <see cref="SequenceId"/> unchanged -- see <c>SwimClusterMessageBus.Membership.cs</c>'s
    /// remarks.
    /// </summary>
    /// <param name="SequenceId">Matches the originating <see cref="SwimPingPayload.SequenceId"/>.</param>
    /// <param name="SourceHost">The acking (i.e. probed) member's own host.</param>
    /// <param name="Updates">Piggybacked membership updates.</param>
    /// <param name="Piggybacked">Piggybacked application-payload gossip items -- see <see cref="SwimPingPayload.Piggybacked"/>.</param>
    internal sealed record SwimAckPayload(
        Guid SequenceId,
        string SourceHost,
        SwimMembershipUpdate[] Updates,
        SwimDatagram[] Piggybacked);
}
