namespace ThunderPropagator.ClusterMessageBuses.SharedKernel
{
    /// <summary>
    /// Which of <see cref="ThunderPropagator.Application.Channels.Cluster.MessageBus.IClusterMessageBus"/>'s
    /// leader/peer-pull operations a <see cref="ClusterRequestEnvelope"/> is asking for. Shared
    /// across every transport that implements its own request/reply plumbing for these operations
    /// (every transport except WebApi, which dispatches by HTTP route instead of a wire-level
    /// discriminator, and Grpc, which uses its own protobuf-generated service/method dispatch) --
    /// consolidated here from what used to be nineteen near-identical per-transport copies
    /// (<c>{Name}ClusterRequestKind</c>) to remove that duplication. See each transport's own
    /// <c>RequestReply.cs</c>/<c>Snapshots.cs</c>/<c>SubscriptionFetch.cs</c>/<c>BytePayload.cs</c>
    /// for how it's used.
    /// </summary>
    public enum ClusterRequestKind
    {
        /// <summary>Backs <c>IClusterMessageBus.RestoreFromLeaderAsync</c>. Uses <see cref="ClusterRequestEnvelope.ChannelName"/>.</summary>
        RestoreSnapshot,

        /// <summary>Backs <c>IClusterMessageBus.SyncDeltaFromLeaderAsync</c>. Uses <see cref="ClusterRequestEnvelope.ChannelName"/> and <see cref="ClusterRequestEnvelope.SinceTicks"/>.</summary>
        SyncDelta,

        /// <summary>Backs <c>IClusterMessageBus.FetchPeerSubscriptionsAsync</c>. Uses <see cref="ClusterRequestEnvelope.ChannelKey"/>, not <see cref="ClusterRequestEnvelope.ChannelName"/>.</summary>
        FetchSubscriptions,

        /// <summary>
        /// Byte-oriented, channel-agnostic counterpart to <see cref="RestoreSnapshot"/> -- backs
        /// <c>IClusterMessageBus.PullSnapshotAsync</c>. Uses <see cref="ClusterRequestEnvelope.ChannelKey"/>,
        /// not <see cref="ClusterRequestEnvelope.ChannelName"/>.
        /// </summary>
        PullSnapshotBytes
    }
}
