namespace ThunderPropagator.ClusterMessageBuses.ZeroMQ
{
    /// <summary>Converts a node's <c>NodeEndpoint</c>/a peer's discovered endpoint into ZeroMQ TCP transport addresses.</summary>
    internal static class ZeroMqAddressing
    {
        /// <summary>The address this node's own ROUTER socket binds to — every interface, on the node endpoint's own port.</summary>
        internal static string BindAddress(Uri nodeEndpoint) => $"tcp://*:{nodeEndpoint.Port}";

        /// <summary>The address a peer connection's DEALER socket connects to.</summary>
        internal static string ConnectAddress(Uri peerEndpoint) => $"tcp://{peerEndpoint.Host}:{peerEndpoint.Port}";
    }
}
