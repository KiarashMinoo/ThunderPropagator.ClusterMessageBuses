namespace ThunderPropagator.ClusterMessageBuses.Swim
{
    /// <summary>
    /// Discriminates what a <see cref="SwimDatagram"/> carries -- either one of the three SWIM
    /// failure-detection message types, one of the three application-payload gossip kinds (each
    /// also usable as a piggybacked item inside a Ping/PingReq/Ack), or one of the two point-to-point
    /// request/reply kinds used for the leader/peer-pull operations.
    /// </summary>
    internal enum SwimMessageKind
    {
        Ping,
        PingReq,
        Ack,
        GossipFanOut,
        GossipSubscriptionEvent,
        GossipByteFanOut,
        Request,
        Response
    }
}
