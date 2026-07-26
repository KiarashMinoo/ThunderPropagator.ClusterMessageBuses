using System.Net.Http;

namespace ThunderPropagator.ClusterMessageBuses.WebApi
{
    /// <summary>Production <see cref="IWebApiClusterHttpClient"/>: wraps a real <see cref="HttpClient"/>.</summary>
    internal sealed class HttpClientWebApiClusterHttpClient : IWebApiClusterHttpClient, IAsyncDisposable
    {
        private readonly HttpClient _httpClient;

        internal HttpClientWebApiClusterHttpClient(HttpClient httpClient)
        {
            _httpClient = httpClient;
        }

        public Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
            => _httpClient.SendAsync(request, cancellationToken);

        public ValueTask DisposeAsync()
        {
            _httpClient.Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
