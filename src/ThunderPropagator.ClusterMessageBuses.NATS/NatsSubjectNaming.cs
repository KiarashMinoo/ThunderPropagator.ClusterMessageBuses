using System.Globalization;
using System.Text;

namespace ThunderPropagator.ClusterMessageBuses.NATS
{
    /// <summary>
    /// Builds every NATS subject <see cref="NatsClusterMessageBus"/> uses, and the one-way slug
    /// transform used to turn an arbitrary
    /// <see cref="ThunderPropagator.Application.Channels.Cluster.ClusterConfiguration.NodeEndpoint"/>
    /// <see cref="Uri"/> into a subject-token-safe segment. Mirrors
    /// <c>ThunderPropagator.ClusterMessageBuses.Kafka.KafkaTopicNaming"/> /
    /// <c>ThunderPropagator.ClusterMessageBuses.RabbitMQ.RabbitMqTopicNaming</c> exactly, except
    /// there is no separate reply-subject builder: NATS's native request/reply delivers replies to a
    /// per-call ephemeral inbox subject the client library manages itself, so this transport never
    /// needs to name or own a reply subject the way the topic/queue-based transports do.
    /// </summary>
    internal static class NatsSubjectNaming
    {
        /// <summary>Fan-out subject for a specific channel.</summary>
        internal static string FanOutSubject(string subjectPrefix, Guid channelKey)
            => $"{subjectPrefix}.fanout.{channelKey:N}";

        /// <summary>Subscription-event subject for a specific channel.</summary>
        internal static string SubscriptionEventSubject(string subjectPrefix, Guid channelKey)
            => $"{subjectPrefix}.subscriptions.{channelKey:N}";

        /// <summary>
        /// The subject a node listens on for inbound requests (restore/delta/fetch-subscriptions)
        /// addressed to it. Requesters send a NATS request to the target's own subject using its
        /// <c>NodeEndpoint</c>, mirroring how the HTTP transport addresses <c>leaderEndpoint</c>/
        /// <c>peerEndpoint</c> directly rather than going through discovery.
        /// </summary>
        internal static string RequestSubject(string subjectPrefix, Uri nodeEndpoint)
            => $"{subjectPrefix}.requests.{Slugify(nodeEndpoint)}";

        /// <summary>
        /// Turns a <see cref="Uri"/> such as <c>https://follower1:5001/</c> into a subject-token-safe
        /// segment such as <c>follower1-5001</c>. Deliberately lossy/one-way — only used to build a
        /// deterministic, collision-resistant subject per node, never parsed back into a Uri.
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
