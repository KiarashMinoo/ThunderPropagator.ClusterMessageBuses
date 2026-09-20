namespace ThunderPropagator.ClusterMessageBuses.Swim
{
    /// <summary>
    /// <see cref="SwimDatagram.PayloadJson"/> shape for <see cref="SwimMessageKind.Ping"/> -- a
    /// direct liveness probe, sent either by the original prober (probing one random member each
    /// SWIM probe-loop tick) or by a relay acting on a <see cref="SwimPingReqPayload"/> on someone
    /// else's behalf (see <c>SwimClusterMessageBus.Membership.cs</c>'s remarks for the full
    /// indirect-probe relay flow). The same <see cref="SequenceId"/> is reused across a whole probe
    /// transaction -- the initial direct ping and every indirect <see cref="SwimPingReqPayload"/>
    /// fanned out after it times out -- so any <see cref="SwimAckPayload"/> carrying it, however it
    /// arrives, completes the same pending wait.
    /// </summary>
    /// <param name="SequenceId">Correlates this ping with its eventual <see cref="SwimAckPayload"/>.</param>
    /// <param name="SourceHost">The sender's own host -- who to attribute this probe/gossip traffic to.</param>
    /// <param name="Updates">Piggybacked membership updates, opportunistically disseminated alongside this message.</param>
    /// <param name="Piggybacked">
    /// Piggybacked application-payload gossip items (each independently a
    /// <c>SharedKernel.ClusterChannelEnvelope&lt;TMessage&gt;</c> wrapped in its own <see cref="SwimDatagram"/>) --
    /// infection-style dissemination riding for free on top of the failure-detection traffic that's
    /// already flowing, exactly like HashiCorp's <c>memberlist</c> piggybacks its own
    /// <c>TransmitLimitedQueue</c> broadcasts on every ping/ack/indirect-ping message.
    /// </param>
    internal sealed record SwimPingPayload(
        Guid SequenceId,
        string SourceHost,
        SwimMembershipUpdate[] Updates,
        SwimDatagram[] Piggybacked);
}
