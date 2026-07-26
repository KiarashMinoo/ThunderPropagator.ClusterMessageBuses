using System.Globalization;
using System.Text;

namespace ThunderPropagator.ClusterMessageBuses.RedisPubSub
{
    /// <summary>
    /// Builds every Redis pub/sub channel name <see cref="RedisPubSubClusterMessageBus"/> uses, and
    /// the one-way slug transform used to turn an arbitrary
    /// <see cref="ThunderPropagator.Application.Channels.Cluster.ClusterConfiguration.NodeEndpoint"/>
    /// <see cref="Uri"/> into a channel-name-safe segment. Mirrors
    /// <c>ThunderPropagator.ClusterMessageBuses.Pulsar.PulsarTopicNaming</c>'s shape (a request *and*
    /// reply channel per node, since Redis pub/sub — like Kafka/RabbitMQ/Pulsar/MQTT/ActiveMQ, unlike
    /// NATS — has no native request/reply primitive), but uses <c>:</c>-separated segments matching
    /// normal Redis key/channel-naming conventions instead of <c>-</c>/<c>.</c>/<c>/</c>.
    /// </summary>
    internal static class RedisChannelNaming
    {
        /// <summary>Fan-out channel for a specific channel key.</summary>
        internal static string FanOutChannel(string channelPrefix, Guid channelKey)
            => $"{channelPrefix}:fanout:{channelKey:N}";

        /// <summary>Subscription-event channel for a specific channel key.</summary>
        internal static string SubscriptionEventChannel(string channelPrefix, Guid channelKey)
            => $"{channelPrefix}:subscriptions:{channelKey:N}";

        /// <summary>The channel a node listens on for inbound requests addressed to it.</summary>
        internal static string RequestChannel(string channelPrefix, Uri nodeEndpoint)
            => $"{channelPrefix}:requests:{Slugify(nodeEndpoint)}";

        /// <summary>The channel a node listens on for replies to requests *it* sent.</summary>
        internal static string ReplyChannel(string channelPrefix, Uri nodeEndpoint)
            => $"{channelPrefix}:replies:{Slugify(nodeEndpoint)}";

        /// <summary>
        /// Turns a <see cref="Uri"/> such as <c>https://follower1:5001/</c> into a channel-name-safe
        /// segment such as <c>follower1-5001</c>. Deliberately lossy/one-way — only used to build a
        /// deterministic, collision-resistant channel name per node, never parsed back into a Uri.
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
