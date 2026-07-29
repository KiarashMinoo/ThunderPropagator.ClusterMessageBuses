using Grpc.Net.Client;
using ThunderPropagator.ClusterMessageBuses.Grpc.Protos;

namespace ThunderPropagator.ClusterMessageBuses.Grpc
{
    /// <summary>
    /// The two unary-RPC clients used for the leader/peer-pull operations (restore snapshot/sync
    /// delta, fetch subscriptions), bundled together since both are dialed against the same peer.
    /// Unlike <see cref="GrpcPeerConnection"/>'s long-lived duplex streams, these are created fresh
    /// per call rather than cached — gRPC's own unary call already binds a response to its request
    /// (no correlation-id scheme needed), and these operations are infrequent (bootstrap/leader-pull,
    /// not the fan-out hot path), so there is no persistent connection worth reusing here, mirroring
    /// how the WebApi transport's HTTP request/reply needs no per-peer connection cache either.
    /// <paramref name="Channel"/> is only ever non-null in production — tests construct this
    /// directly from substitute clients with no real channel to dispose.
    /// </summary>
    internal sealed record GrpcUnaryClients(
        ClusterSnapshot.ClusterSnapshotClient Snapshot,
        ClusterSubscriptionFetch.ClusterSubscriptionFetchClient SubscriptionFetch,
        GrpcChannel? Channel);
}
