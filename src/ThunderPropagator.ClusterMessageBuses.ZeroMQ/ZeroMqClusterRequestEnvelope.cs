namespace ThunderPropagator.ClusterMessageBuses.ZeroMQ
{
    /// <summary>
    /// One of the three leader/peer-pull requests. No reply-destination field — the answering side
    /// replies over the exact same physical connection the request arrived on (the ROUTER socket
    /// echoes back the identity frame it read the request with), the same non-trusting-the-wire
    /// reasoning documented for every other transport in this repo.
    /// </summary>
    internal sealed record ZeroMqClusterRequestEnvelope(
        Guid CorrelationId,
        ZeroMqClusterRequestKind Kind,
        string? ChannelName,
        Guid? ChannelKey,
        long? SinceTicks);
}
