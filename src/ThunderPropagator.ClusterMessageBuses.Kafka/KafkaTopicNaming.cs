using System.Globalization;
using System.Text;

namespace ThunderPropagator.ClusterMessageBuses.Kafka
{
    /// <summary>
    /// Builds every Kafka topic name <see cref="KafkaClusterMessageBus"/> uses, and the one-way
    /// slug transform used to turn an arbitrary <see cref="ThunderPropagator.Application.Channels.Cluster.ClusterConfiguration.NodeEndpoint"/>
    /// <see cref="Uri"/> into a Kafka-legal topic-name segment (Kafka topic names are restricted
    /// to <c>[a-zA-Z0-9._-]</c>, up to 249 characters).
    /// </summary>
    internal static class KafkaTopicNaming
    {
        /// <summary>Fan-out topic for a specific channel — mirrors the HTTP transport's per-channel POST route.</summary>
        internal static string FanOutTopic(string topicPrefix, Guid channelKey)
            => $"{topicPrefix}.fanout.{channelKey:N}";

        /// <summary>Subscription-event topic for a specific channel.</summary>
        internal static string SubscriptionEventTopic(string topicPrefix, Guid channelKey)
            => $"{topicPrefix}.subscriptions.{channelKey:N}";

        /// <summary>
        /// The topic a node listens on for inbound broker-native requests (restore/delta/fetch-
        /// subscriptions) addressed to it. Requesters publish here using the target's own
        /// <c>NodeEndpoint</c>, mirroring how the HTTP transport addresses <c>leaderEndpoint</c>/
        /// <c>peerEndpoint</c> directly rather than going through discovery.
        /// </summary>
        internal static string RequestTopic(string topicPrefix, Uri nodeEndpoint)
            => $"{topicPrefix}.requests.{Slugify(nodeEndpoint)}";

        /// <summary>
        /// The topic a node listens on for replies to requests *it* sent. Named after the
        /// requester's own endpoint so a reply is delivered back to exactly the node that asked,
        /// regardless of how many other nodes are also waiting on replies from the same peer.
        /// </summary>
        internal static string ReplyTopic(string topicPrefix, Uri nodeEndpoint)
            => $"{topicPrefix}.replies.{Slugify(nodeEndpoint)}";

        /// <summary>
        /// Turns a <see cref="Uri"/> such as <c>https://follower1:5001/</c> into a Kafka-topic-safe
        /// segment such as <c>follower1-5001</c>. Deliberately lossy/one-way — only used to build a
        /// deterministic, collision-resistant topic name per node, never parsed back into a Uri.
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
