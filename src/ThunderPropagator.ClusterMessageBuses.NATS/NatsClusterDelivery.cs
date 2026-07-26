namespace ThunderPropagator.ClusterMessageBuses.NATS
{
    /// <summary>
    /// One message delivered from <see cref="INatsClusterTransport.SubscribeAsync"/>.
    /// </summary>
    /// <param name="Payload">The message body.</param>
    /// <param name="ReplyTo">
    /// The NATS reply-to inbox subject, populated only when this delivery originated from a NATS
    /// request (i.e. arrived via <see cref="INatsClusterTransport.RequestAsync"/> on the sender's
    /// side) rather than a plain publish. Only the request-listener loop
    /// (<c>NatsClusterMessageBus.RequestReply.cs</c>) uses this; fan-out and subscription-event
    /// deliveries ignore it.
    /// </param>
    internal readonly record struct NatsClusterDelivery(string Payload, string? ReplyTo);
}
