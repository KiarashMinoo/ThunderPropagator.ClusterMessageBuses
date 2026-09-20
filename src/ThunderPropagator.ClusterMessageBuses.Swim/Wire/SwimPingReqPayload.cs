namespace ThunderPropagator.ClusterMessageBuses.Swim
{
    /// <summary>
    /// <see cref="SwimDatagram.PayloadJson"/> shape for <see cref="SwimMessageKind.PingReq"/> --
    /// sent by a prober to <c>IndirectProbeRelayCount</c> other members after a direct
    /// <see cref="SwimPingPayload"/> to <see cref="TargetHost"/> times out, asking each relay to
    /// probe <see cref="TargetHost"/> on the prober's behalf and forward back whatever
    /// <see cref="SwimAckPayload"/> it gets. This is what lets SWIM tell a genuinely dead member
    /// apart from one that's merely unreachable from the prober specifically (e.g. a one-way network
    /// partition, or a transient loss of the direct ping's own datagram -- plain UDP guards against
    /// neither).
    /// </summary>
    /// <param name="SequenceId">
    /// Same correlation id as the direct <see cref="SwimPingPayload"/> that timed out -- the relay
    /// reuses it for its own <see cref="SwimPingPayload"/> sent to <see cref="TargetHost"/>, so that
    /// when <see cref="TargetHost"/> acks, the relay can identify which pending indirect-probe
    /// request to forward that ack back for.
    /// </param>
    /// <param name="TargetHost">The suspect member the relay should probe on the original prober's behalf.</param>
    /// <param name="Updates">Piggybacked membership updates.</param>
    /// <param name="Piggybacked">Piggybacked application-payload gossip items -- see <see cref="SwimPingPayload.Piggybacked"/>.</param>
    internal sealed record SwimPingReqPayload(
        Guid SequenceId,
        string TargetHost,
        SwimMembershipUpdate[] Updates,
        SwimDatagram[] Piggybacked);
}
