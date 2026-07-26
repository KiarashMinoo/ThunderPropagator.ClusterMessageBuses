using System.Globalization;
using System.Text;

namespace ThunderPropagator.ClusterMessageBuses.RabbitMQ
{
    /// <summary>
    /// Builds every exchange/queue name <see cref="RabbitMqClusterMessageBus"/> uses, and the
    /// one-way slug transform used to turn an arbitrary
    /// <see cref="ThunderPropagator.Application.Channels.Cluster.ClusterConfiguration.NodeEndpoint"/>
    /// <see cref="Uri"/> into an AMQP-legal queue-name segment. Mirrors
    /// <c>ThunderPropagator.ClusterMessageBuses.Kafka.KafkaTopicNaming</c> exactly, one topic
    /// becoming one exchange (fan-out/subscription-sync) or one named queue (request/reply).
    /// </summary>
    internal static class RabbitMqTopicNaming
    {
        /// <summary>Fan-out exchange (type "fanout") for a specific channel.</summary>
        internal static string FanOutExchange(string exchangePrefix, Guid channelKey)
            => $"{exchangePrefix}.fanout.{channelKey:N}";

        /// <summary>Subscription-event exchange (type "fanout") for a specific channel.</summary>
        internal static string SubscriptionEventExchange(string exchangePrefix, Guid channelKey)
            => $"{exchangePrefix}.subscriptions.{channelKey:N}";

        /// <summary>
        /// The queue a node listens on for inbound broker-native requests (restore/delta/fetch-
        /// subscriptions) addressed to it. Requesters publish here (via the default exchange, using
        /// the queue name as the routing key) using the target's own <c>NodeEndpoint</c>, mirroring
        /// how the HTTP transport addresses <c>leaderEndpoint</c>/<c>peerEndpoint</c> directly rather
        /// than going through discovery.
        /// </summary>
        internal static string RequestQueue(string exchangePrefix, Uri nodeEndpoint)
            => $"{exchangePrefix}.requests.{Slugify(nodeEndpoint)}";

        /// <summary>
        /// The queue a node listens on for replies to requests *it* sent. Named after the
        /// requester's own endpoint so a reply is delivered back to exactly the node that asked.
        /// </summary>
        internal static string ReplyQueue(string exchangePrefix, Uri nodeEndpoint)
            => $"{exchangePrefix}.replies.{Slugify(nodeEndpoint)}";

        /// <summary>
        /// Turns a <see cref="Uri"/> such as <c>https://follower1:5001/</c> into a queue-name-safe
        /// segment such as <c>follower1-5001</c>. Deliberately lossy/one-way — only used to build a
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
