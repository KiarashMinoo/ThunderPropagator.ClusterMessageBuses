using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace ThunderPropagator.ClusterMessageBuses.AzureServiceBus
{
    /// <summary>
    /// Builds every topic/subscription/queue name <see cref="AzureServiceBusClusterMessageBus"/>
    /// uses, and the one-way slug transform used to turn an arbitrary
    /// <see cref="ThunderPropagator.Application.Channels.Cluster.ClusterConfiguration.NodeEndpoint"/>
    /// <see cref="Uri"/> into a Service-Bus-legal name segment. Mirrors
    /// <c>ThunderPropagator.ClusterMessageBuses.AwsSqs.AwsSqsResourceNaming</c>'s shape — one topic
    /// per channel for fan-out/subscription-sync, one queue pair per node for request/reply — except
    /// a channel's per-node <em>subscription</em> stands in for AwsSqs's separate per-node queue,
    /// since an Azure Service Bus topic subscription is itself a directly receivable entity (no
    /// separate queue, IAM policy, or explicit subscribe call needed the way SNS-to-SQS requires).
    /// </summary>
    internal static class AzureServiceBusResourceNaming
    {
        /// <summary>Topic every node subscribes to when it calls <c>SubscribeAsync</c> for this channel's fan-out.</summary>
        internal static string FanOutTopic(string prefix, Guid channelKey)
            => $"{prefix}-fanout-{channelKey:N}";

        /// <summary>Topic every node subscribes to when it calls <c>SubscribeAsync</c> for this channel's subscription-sync.</summary>
        internal static string SubscriptionEventTopic(string prefix, Guid channelKey)
            => $"{prefix}-subscriptions-{channelKey:N}";

        /// <summary>
        /// This node's own exclusive subscription on the fan-out topic for one channel. Subscription
        /// names are capped at 50 characters by Azure Service Bus — far shorter than the 260-character
        /// cap on topic/queue names — so unlike every other name built here, this one is a short hash
        /// of the node/channel identity rather than their literal slug/hex text, deterministic and
        /// stable across restarts but not human-readable.
        /// </summary>
        internal static string FanOutSubscription(Uri nodeEndpoint, Guid channelKey)
            => $"fo-{ShortHash(nodeEndpoint, channelKey)}";

        /// <summary>This node's own exclusive subscription on the subscription-sync topic for one channel. Same 50-character-limit reasoning as <see cref="FanOutSubscription"/>.</summary>
        internal static string SubscriptionEventSubscription(Uri nodeEndpoint, Guid channelKey)
            => $"se-{ShortHash(nodeEndpoint, channelKey)}";

        /// <summary>
        /// The queue a node listens on for inbound broker-native requests (restore/delta/fetch-
        /// subscriptions) addressed to it. Requesters send directly to this queue by name, using the
        /// target's own <c>NodeEndpoint</c>, mirroring how the HTTP transport addresses
        /// <c>leaderEndpoint</c>/<c>peerEndpoint</c> directly rather than going through discovery.
        /// </summary>
        internal static string RequestQueue(string prefix, Uri nodeEndpoint)
            => $"{prefix}-requests-{Slugify(nodeEndpoint)}";

        /// <summary>
        /// The queue a node listens on for replies to requests *it* sent. Named after the
        /// requester's own endpoint so a reply is delivered back to exactly the node that asked —
        /// the answering side derives this name itself from the request envelope's
        /// <c>ReplyToNodeEndpoint</c>, rather than trusting a queue name supplied on the wire, so a
        /// request can never make this node send to an arbitrary attacker-chosen queue.
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

        /// <summary>
        /// A deterministic, fixed-length (16 hex characters), collision-resistant identifier for one
        /// node/channel pair — short enough that <c>"fo-"</c>/<c>"se-"</c> plus this hash always fits
        /// comfortably under Service Bus's 50-character subscription-name cap, regardless of how long
        /// the node's hostname is.
        /// </summary>
        private static string ShortHash(Uri nodeEndpoint, Guid channelKey)
        {
            var input = $"{Slugify(nodeEndpoint)}|{channelKey:N}";
            var hash = SHA256.HashData(Encoding.UTF8.GetBytes(input));

            // Convert.ToHexStringLower isn't available on net8.0 (added in .NET 9), and this
            // project multi-targets net8.0/net9.0/net10.0, so ToHexString + ToLowerInvariant is
            // used instead for compatibility across all three.
            return Convert.ToHexString(hash)[..16].ToLowerInvariant();
        }
    }
}
