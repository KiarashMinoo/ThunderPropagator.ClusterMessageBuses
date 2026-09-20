using System.Globalization;
using System.Text;

namespace ThunderPropagator.ClusterMessageBuses.AwsSqs
{
    /// <summary>
    /// Builds every SNS topic/SQS queue name <see cref="AwsSqsClusterMessageBus"/> uses, and the
    /// one-way slug transform used to turn an arbitrary
    /// <see cref="ThunderPropagator.Application.Channels.Cluster.ClusterConfiguration.NodeEndpoint"/>
    /// <see cref="Uri"/> into an SQS/SNS-legal name segment. Mirrors
    /// <c>ThunderPropagator.ClusterMessageBuses.RabbitMQ.RabbitMqTopicNaming</c>'s shape — one topic
    /// per channel for fan-out/subscription-sync, one queue pair per node for request/reply — but
    /// using hyphens throughout instead of dots, since SNS/SQS resource names only allow
    /// alphanumerics, hyphens, and underscores (AWS rejects dots).
    /// </summary>
    internal static class AwsSqsResourceNaming
    {
        /// <summary>SNS topic every node subscribes a queue to when it calls <c>SubscribeAsync</c> for this channel's fan-out.</summary>
        internal static string FanOutTopic(string prefix, Guid channelKey)
            => $"{prefix}-fanout-{channelKey:N}";

        /// <summary>SNS topic every node subscribes a queue to when it calls <c>SubscribeAsync</c> for this channel's subscription-sync.</summary>
        internal static string SubscriptionEventTopic(string prefix, Guid channelKey)
            => $"{prefix}-subscriptions-{channelKey:N}";

        /// <summary>
        /// SNS topic every node subscribes a queue to when it calls the byte-oriented <c>SubscribeAsync</c>
        /// overload for this channel key -- see <c>ClusterByteMessage</c>'s own doc comment. Kept on
        /// its own resource-name namespace, never sharing <see cref="FanOutTopic"/>, since the two
        /// payload shapes are never interchangeable on the wire.
        /// </summary>
        internal static string ByteFanOutTopic(string prefix, Guid channelKey)
            => $"{prefix}-bytefanout-{channelKey:N}";

        /// <summary>
        /// This node's own exclusive SQS queue for one channel key's byte-oriented fan-out
        /// subscription — same deterministic, restart-survivable naming as <see cref="FanOutQueue"/>,
        /// just on the byte-fanout resource-name namespace so it never collides with it.
        /// </summary>
        internal static string ByteFanOutQueue(string prefix, Uri nodeEndpoint, Guid channelKey)
            => $"{prefix}-bytefanout-{Slugify(nodeEndpoint)}-{channelKey:N}";

        /// <summary>
        /// This node's own exclusive SQS queue for one channel's fan-out subscription — deterministic
        /// (not GUID-suffixed) so it survives a restart and is trivially re-attachable, mirroring how
        /// <c>KafkaClusterMessageBus</c> reuses a stable per-node consumer group rather than a
        /// throwaway one.
        /// </summary>
        internal static string FanOutQueue(string prefix, Uri nodeEndpoint, Guid channelKey)
            => $"{prefix}-fanout-{Slugify(nodeEndpoint)}-{channelKey:N}";

        /// <summary>This node's own exclusive SQS queue for one channel's subscription-sync subscription.</summary>
        internal static string SubscriptionEventQueue(string prefix, Uri nodeEndpoint, Guid channelKey)
            => $"{prefix}-subscriptions-{Slugify(nodeEndpoint)}-{channelKey:N}";

        /// <summary>
        /// The queue a node listens on for inbound broker-native requests (restore/delta/fetch-
        /// subscriptions) addressed to it. Requesters resolve this queue's URL (via
        /// <c>GetQueueUrlAsync</c>) using the target's own <c>NodeEndpoint</c>, mirroring how the
        /// HTTP transport addresses <c>leaderEndpoint</c>/<c>peerEndpoint</c> directly rather than
        /// going through discovery.
        /// </summary>
        internal static string RequestQueue(string prefix, Uri nodeEndpoint)
            => $"{prefix}-requests-{Slugify(nodeEndpoint)}";

        /// <summary>
        /// The queue a node listens on for replies to requests *it* sent. Named after the
        /// requester's own endpoint so a reply is delivered back to exactly the node that asked —
        /// the answering side derives this name itself from
        /// <see cref="ThunderPropagator.Application.Channels.Cluster.ClusterConfiguration.NodeEndpoint"/>
        /// carried on the request envelope, rather than trusting a queue URL supplied on the wire, so
        /// a request can never make this node send to an arbitrary attacker-chosen queue.
        /// </summary>
        internal static string ReplyQueue(string prefix, Uri nodeEndpoint)
            => $"{prefix}-replies-{Slugify(nodeEndpoint)}";

        /// <summary>
        /// Turns a <see cref="Uri"/> such as <c>https://follower1:5001/</c> into a name-safe segment
        /// such as <c>follower1-5001</c>. Deliberately lossy/one-way — only used to build a
        /// deterministic, collision-resistant name per node, never parsed back into a Uri.
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
