using System.Net.Http;

namespace ThunderPropagator.ClusterMessageBuses.WebApi
{
    /// <summary>
    /// Narrow seam around outbound HTTP calls to peers. <see cref="System.Net.Http.HttpMessageHandler.SendAsync"/>
    /// is <c>protected internal</c> and not directly substitutable with NSubstitute's public API, so —
    /// like the narrow transport-seam interfaces used for NATS.Net/DotPulsar/MQTTnet and the
    /// listener seam used for the WebSocket transport elsewhere in this repo — this interface exists
    /// purely so tests can substitute a fake HTTP sender instead of a real <see cref="HttpClient"/>.
    /// </summary>
    internal interface IWebApiClusterHttpClient
    {
        Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken);
    }
}
