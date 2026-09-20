namespace ThunderPropagator.ClusterMessageBuses.SharedKernel
{
    /// <summary>
    /// Pairs a channel key with the message being fanned out over it -- the shape every
    /// single-connection/shared-socket transport (Tcp, WebSocket, Udp, ZeroMQ, Swim, Rdma, Srd) uses
    /// to wrap a <c>ClusterFanOutMessage</c>, <c>ClusterSubscriptionEvent</c>, or
    /// <c>ClusterByteMessage</c> before multiplexing it into a <see cref="ClusterFrame"/>. Generic so
    /// one shared type replaces what used to be three near-identical per-transport records per
    /// transport (<c>{Name}FanOutPayload</c>, <c>{Name}SubscriptionEventPayload</c>,
    /// <c>{Name}ByteFanOutPayload</c>).
    /// </summary>
    /// <typeparam name="TMessage">
    /// <c>ClusterFanOutMessage</c>, <c>ClusterSubscriptionEvent</c>, or <c>ClusterByteMessage</c>.
    /// </typeparam>
    /// <param name="ChannelKey">The channel this message is being fanned out over.</param>
    /// <param name="Message">The message itself, already stamped with the sender's self-echo <c>OriginId</c>.</param>
    public sealed record ClusterChannelEnvelope<TMessage>(Guid ChannelKey, TMessage Message);
}
