using System.Globalization;
using System.Text;

namespace ThunderPropagator.ClusterMessageBuses.GcpPubSub
{
    /// <summary>
    /// Builds every GCP Pub/Sub topic/subscription ID <see cref="GcpPubSubClusterMessageBus"/> uses,
    /// and the one-way slug transform used to turn an arbitrary
    /// <see cref="ThunderPropagator.Application.Channels.Cluster.ClusterConfiguration.NodeEndpoint"/>
    /// <see cref="Uri"/> into a Pub/Sub-legal ID segment. Mirrors
    /// <c>ThunderPropagator.ClusterMessageBuses.AwsSqs.AwsSqsResourceNaming</c>'s shape (one topic per
    /// channel for fan-out/subscription-sync, with every node's own exclusive subscription on it) —
    /// but unlike SNS/SQS, Pub/Sub has no separate queue primitive at all, so the three leader/
    /// peer-pull operations also go through a topic + single-subscription pair per node rather than a
    /// real queue (see <see cref="RequestTopicId"/>/<see cref="RequestSubscriptionId"/>), matching
    /// CLAUDE.md's "brokers with only topics/subscriptions" pattern. Hyphens throughout are legal in
    /// both topic and subscription IDs (letters, numbers, hyphens, underscores, periods, tildes, plus
    /// signs, percent signs; must start with a letter) — the default <c>"tp-cluster"</c> prefix
    /// satisfies that.
    /// </summary>
    internal static class GcpPubSubResourceNaming
    {
        /// <summary>Topic every node subscribes its own subscription to when it calls <c>SubscribeAsync</c> for this channel's fan-out.</summary>
        internal static string FanOutTopicId(string prefix, Guid channelKey)
            => $"{prefix}-fanout-{channelKey:N}";

        /// <summary>Topic every node subscribes its own subscription to when it calls <c>SubscribeAsync</c> for this channel's subscription-sync.</summary>
        internal static string SubscriptionEventTopicId(string prefix, Guid channelKey)
            => $"{prefix}-subscriptions-{channelKey:N}";

        /// <summary>
        /// This node's own exclusive Pub/Sub subscription on <see cref="FanOutTopicId"/> — deterministic
        /// (not GUID-suffixed) so it survives a restart and is trivially re-attachable, mirroring how
        /// <c>KafkaClusterMessageBus</c> reuses a stable per-node consumer group rather than a
        /// throwaway one.
        /// </summary>
        internal static string FanOutSubscriptionId(string prefix, Uri nodeEndpoint, Guid channelKey)
            => $"{prefix}-fanout-{Slugify(nodeEndpoint)}-{channelKey:N}";

        /// <summary>This node's own exclusive Pub/Sub subscription on <see cref="SubscriptionEventTopicId"/>.</summary>
        internal static string SubscriptionEventSubscriptionId(string prefix, Uri nodeEndpoint, Guid channelKey)
            => $"{prefix}-subscriptions-{Slugify(nodeEndpoint)}-{channelKey:N}";

        /// <summary>
        /// The topic a node listens on for inbound broker-native requests (restore/delta/fetch-
        /// subscriptions) addressed to it. Requesters publish directly to this topic (built from the
        /// target's own <c>NodeEndpoint</c>, mirroring how the HTTP transport addresses
        /// <c>leaderEndpoint</c>/<c>peerEndpoint</c> directly rather than going through discovery);
        /// exactly one subscription (<see cref="RequestSubscriptionId"/>) — this node's own — ever
        /// pulls from it, so it behaves like a point-to-point queue.
        /// </summary>
        internal static string RequestTopicId(string prefix, Uri nodeEndpoint)
            => $"{prefix}-requests-{Slugify(nodeEndpoint)}";

        /// <summary>This node's own single subscription on <see cref="RequestTopicId"/>.</summary>
        internal static string RequestSubscriptionId(string prefix, Uri nodeEndpoint)
            => $"{prefix}-requests-{Slugify(nodeEndpoint)}-sub";

        /// <summary>
        /// The topic a node listens on for replies to requests *it* sent. Named after the requester's
        /// own endpoint so a reply is delivered back to exactly the node that asked — the answering
        /// side derives this name itself from
        /// <see cref="ThunderPropagator.Application.Channels.Cluster.ClusterConfiguration.NodeEndpoint"/>
        /// carried on the request envelope, rather than trusting a destination supplied on the wire,
        /// so a request can never make this node publish to an arbitrary attacker-chosen topic.
        /// </summary>
        internal static string ReplyTopicId(string prefix, Uri nodeEndpoint)
            => $"{prefix}-replies-{Slugify(nodeEndpoint)}";

        /// <summary>This node's own single subscription on <see cref="ReplyTopicId"/>.</summary>
        internal static string ReplySubscriptionId(string prefix, Uri nodeEndpoint)
            => $"{prefix}-replies-{Slugify(nodeEndpoint)}-sub";

        /// <summary>
        /// Turns a <see cref="Uri"/> such as <c>https://follower1:5001/</c> into an ID-safe segment
        /// such as <c>follower1-5001</c>. Deliberately lossy/one-way — only used to build a
        /// deterministic, collision-resistant ID per node, never parsed back into a Uri.
        /// </summary>
        internal static string Slugify(Uri endpoint)
        {
            var host = endpoint.IsDefaultPort ? endpoint.Host : $"{endpoint.Host}-{endpoint.Port.ToString(CultureInfo.InvariantCulture)}";

            var builder = new StringBuilder(host.Length);
            foreach (var ch in host)
            {
                builder.Append(char.IsAsciiLetterOrDigit(ch) ? char.ToLowerInvariant(ch) : '-');
            }

            return builder.ToString();
        }
    }
}
