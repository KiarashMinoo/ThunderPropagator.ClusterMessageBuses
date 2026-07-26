using System.Net.Http;

namespace ThunderPropagator.ClusterMessageBuses.WebApi
{
    /// <summary>
    /// Configuration for <see cref="WebApiClusterMessageBus"/>.
    /// </summary>
    public sealed class WebApiClusterMessageBusOptions
    {
        /// <summary>
        /// Path this node's own embedded HTTP listener accepts peer requests on, and that every
        /// outbound call to a peer is addressed against. Default: <c>"/thunderpropagator/cluster/webapi"</c>.
        /// </summary>
        public string ListenPath { get; set; } = "/thunderpropagator/cluster/webapi";

        /// <summary>
        /// How long an outbound HTTP call to a peer (fan-out/subscription-event push, or the
        /// leader/peer-pull GET requests) is allowed to run before it is cancelled. Default: 30 seconds.
        /// </summary>
        public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(30);

        /// <summary>
        /// Optional hook to fine-tune the default <see cref="HttpClient"/> this transport constructs
        /// (e.g. default request headers, a custom <see cref="HttpMessageHandler"/> for TLS options).
        /// Ignored if a custom <c>IWebApiClusterHttpClient</c> is injected instead of using the
        /// default implementation.
        /// </summary>
        public Action<HttpClient>? ConfigureHttpClient { get; set; }
    }
}
