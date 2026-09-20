namespace ThunderPropagator.ClusterMessageBuses.Swim
{
    /// <summary>
    /// A single member's known liveness state, as of some incarnation number -- gossiped piggybacked
    /// on every <see cref="SwimPingPayload"/>/<see cref="SwimPingReqPayload"/>/<see cref="SwimAckPayload"/>
    /// so state converges across the cluster without any node needing to talk to every peer directly.
    /// </summary>
    /// <param name="Host">The member's host, matching <see cref="System.Uri.Host"/> of its discovered endpoint.</param>
    /// <param name="State">Alive, Suspect (unconfirmed failure, not yet acted on), or Dead.</param>
    /// <param name="Incarnation">
    /// Monotonically increasing per-member version number, bumped only by the member itself when it
    /// refutes a Suspect/Dead claim about itself by gossiping a higher-incarnation Alive update.
    /// Orders competing claims about the same member: a strictly higher incarnation always wins over
    /// a lower one regardless of state, and at an equal incarnation only a "more dead" state
    /// (Alive -&gt; Suspect -&gt; Dead) is accepted over a "less dead" one -- exactly the precedence
    /// rule used by the original SWIM paper and HashiCorp's <c>memberlist</c>.
    /// </param>
    internal sealed record SwimMembershipUpdate(string Host, SwimMemberState State, long Incarnation);

    /// <summary>Liveness state carried by a <see cref="SwimMembershipUpdate"/>. Ordinal order matters: <see cref="Alive"/> &lt; <see cref="Suspect"/> &lt; <see cref="Dead"/> is used directly for same-incarnation precedence comparisons.</summary>
    internal enum SwimMemberState
    {
        Alive,
        Suspect,
        Dead
    }
}
