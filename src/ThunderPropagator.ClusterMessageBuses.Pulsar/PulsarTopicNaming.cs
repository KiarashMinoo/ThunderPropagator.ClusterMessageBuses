using System.Globalization;
using System.Text;

namespace ThunderPropagator.ClusterMessageBuses.Pulsar
{
    /// <summary>
    /// Builds every Pulsar topic name <see cref="PulsarClusterMessageBus"/> uses, and the one-way
    /// slug transform used to turn an arbitrary
    /// <see cref="ThunderPropagator.Application.Channels.Cluster.ClusterConfiguration.NodeEndpoint"/>
    /// <see cref="Uri"/> into a topic-name-safe segment. Mirrors
    /// <c>ThunderPropagator.ClusterMessageBuses.Kafka.KafkaTopicNaming</c> exactly — Pulsar, like
    /// Kafka, has no native request/reply, so this transport needs its own request *and* reply topic
    /// per node, unlike NATS.
    /// </summary>
    internal static class PulsarTopicNaming
    {
        /// <summary>Fan-out topic for a specific channel.</summary>
        internal static string FanOutTopic(string topicPrefix, Guid channelKey)
            => $"{topicPrefix}-fanout-{channelKey:N}";

        /// <summary>Subscription-event topic for a specific channel.</summary>
        internal static string SubscriptionEventTopic(string topicPrefix, Guid channelKey)
            => $"{topicPrefix}-subscriptions-{channelKey:N}";

        /// <summary>The topic a node listens on for inbound requests addressed to it.</summary>
        internal static string RequestTopic(string topicPrefix, Uri nodeEndpoint)
            => $"{topicPrefix}-requests-{Slugify(nodeEndpoint)}";

        /// <summary>The topic a node listens on for replies to requests *it* sent.</summary>
        internal static string ReplyTopic(string topicPrefix, Uri nodeEndpoint)
            => $"{topicPrefix}-replies-{Slugify(nodeEndpoint)}";

        /// <summary>
        /// Turns a <see cref="Uri"/> such as <c>https://follower1:5001/</c> into a topic-name-safe
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
