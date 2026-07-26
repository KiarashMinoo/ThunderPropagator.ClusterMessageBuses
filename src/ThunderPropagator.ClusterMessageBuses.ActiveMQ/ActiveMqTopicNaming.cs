using System.Globalization;
using System.Text;

namespace ThunderPropagator.ClusterMessageBuses.ActiveMQ
{
    /// <summary>
    /// Builds every JMS topic/queue name <see cref="ActiveMqClusterMessageBus"/> uses, and the
    /// one-way slug transform used to turn an arbitrary
    /// <see cref="ThunderPropagator.Application.Channels.Cluster.ClusterConfiguration.NodeEndpoint"/>
    /// <see cref="Uri"/> into a destination-name-safe segment. Mirrors
    /// <c>ThunderPropagator.ClusterMessageBuses.RabbitMQ.RabbitMqTopicNaming</c> exactly — one topic
    /// per channel for fan-out/subscription-sync, one queue per node for request and one for reply
    /// (ActiveMQ/JMS, like Kafka/RabbitMQ/Pulsar/MQTT, has no native request/reply primitive, so this
    /// transport needs both a request *and* reply destination per node).
    /// </summary>
    internal static class ActiveMqTopicNaming
    {
        /// <summary>Fan-out topic for a specific channel.</summary>
        internal static string FanOutTopic(string topicPrefix, Guid channelKey)
            => $"{topicPrefix}.fanout.{channelKey:N}";

        /// <summary>Subscription-event topic for a specific channel.</summary>
        internal static string SubscriptionEventTopic(string topicPrefix, Guid channelKey)
            => $"{topicPrefix}.subscriptions.{channelKey:N}";

        /// <summary>The queue a node listens on for inbound requests addressed to it.</summary>
        internal static string RequestQueue(string topicPrefix, Uri nodeEndpoint)
            => $"{topicPrefix}.requests.{Slugify(nodeEndpoint)}";

        /// <summary>The queue a node listens on for replies to requests *it* sent.</summary>
        internal static string ReplyQueue(string topicPrefix, Uri nodeEndpoint)
            => $"{topicPrefix}.replies.{Slugify(nodeEndpoint)}";

        /// <summary>
        /// Turns a <see cref="Uri"/> such as <c>https://follower1:5001/</c> into a destination-name-safe
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
