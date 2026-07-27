using Azure.Messaging.ServiceBus;

namespace ThunderPropagator.ClusterMessageBuses.AzureServiceBus
{
    /// <summary>
    /// Configuration for <see cref="AzureServiceBusClusterMessageBus"/>.
    /// </summary>
    public sealed class AzureServiceBusClusterMessageBusOptions
    {
        /// <summary>Azure Service Bus namespace connection string. Required.</summary>
        public string ConnectionString { get; set; } = string.Empty;

        /// <summary>
        /// Prefix applied to every topic/subscription/queue this transport creates, so multiple
        /// ThunderPropagator clusters (or environments) can share one Service Bus namespace without
        /// colliding. Default: <c>"tp-cluster"</c>.
        /// </summary>
        public string ResourcePrefix { get; set; } = "tp-cluster";

        /// <summary>
        /// How long <see cref="AzureServiceBusClusterMessageBus"/> waits for a broker-native
        /// request/reply round trip (<c>RestoreFromLeaderAsync</c>, <c>SyncDeltaFromLeaderAsync</c>,
        /// <c>FetchPeerSubscriptionsAsync</c>) to complete before giving up. Default: 30 seconds.
        /// </summary>
        public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(30);

        /// <summary>
        /// <c>maxWaitTime</c> passed to every <c>ReceiveMessageAsync</c> call — how long a receiver
        /// blocks server-side waiting for a message before returning <see langword="null"/>.
        /// Default: 5 seconds.
        /// </summary>
        public TimeSpan ReceiveMaxWaitTime { get; set; } = TimeSpan.FromSeconds(5);

        /// <summary>
        /// Defensive client-side delay before the next poll whenever a <c>ReceiveMessageAsync</c>
        /// call returns <see langword="null"/> instantly rather than actually waiting
        /// <see cref="ReceiveMaxWaitTime"/> — guards against a client (production misconfiguration,
        /// or a test substitute) that would otherwise turn an idle receiver into a busy-loop.
        /// Default: 250 milliseconds.
        /// </summary>
        public TimeSpan EmptyPollDelay { get; set; } = TimeSpan.FromMilliseconds(250);

        /// <summary>Optional hook to fine-tune the <see cref="ServiceBusClientOptions"/> used to build the default <see cref="ServiceBusClient"/>.</summary>
        public Action<ServiceBusClientOptions>? ConfigureClientOptions { get; set; }
    }
}
