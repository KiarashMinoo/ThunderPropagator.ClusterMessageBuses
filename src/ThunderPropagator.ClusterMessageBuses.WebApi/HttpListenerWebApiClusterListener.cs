using System.Net;
using System.Runtime.CompilerServices;
using System.Text;

namespace ThunderPropagator.ClusterMessageBuses.WebApi
{
    /// <summary>
    /// Production <see cref="IWebApiClusterListener"/>: wraps a real <see cref="HttpListener"/> bound
    /// to this node's own <see cref="WebApiClusterRouting.ListenPrefix"/>, reducing every inbound
    /// <see cref="HttpListenerContext"/> to a plain <see cref="WebApiIncomingRequest"/>.
    /// </summary>
    internal sealed class HttpListenerWebApiClusterListener : IWebApiClusterListener
    {
        private readonly HttpListener _listener;

        internal HttpListenerWebApiClusterListener(string listenPrefix)
        {
            _listener = new HttpListener();
            _listener.Prefixes.Add(listenPrefix.EndsWith('/') ? listenPrefix : $"{listenPrefix}/");
            _listener.Start();
        }

        public async IAsyncEnumerable<WebApiIncomingRequest> AcceptRequestsAsync([EnumeratorCancellation] CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                HttpListenerContext context;
                try
                {
                    var contextTask = _listener.GetContextAsync();
                    var completed = await Task.WhenAny(contextTask, Task.Delay(Timeout.Infinite, cancellationToken)).ConfigureAwait(false);
                    if (completed != contextTask)
                        yield break;

                    context = await contextTask.ConfigureAwait(false);
                }
                catch (Exception) when (cancellationToken.IsCancellationRequested || !_listener.IsListening)
                {
                    yield break;
                }

                string body;
                using (var reader = new StreamReader(context.Request.InputStream, Encoding.UTF8))
                {
                    body = await reader.ReadToEndAsync(cancellationToken).ConfigureAwait(false);
                }

                var response = context.Response;
                yield return new WebApiIncomingRequest
                {
                    Method = context.Request.HttpMethod,
                    Path = context.Request.Url?.AbsolutePath ?? string.Empty,
                    Query = context.Request.Url?.Query,
                    Body = body,
                    RespondAsync = async (statusCode, responseBody) =>
                    {
                        try
                        {
                            response.StatusCode = statusCode;
                            response.ContentType = "application/json";
                            var bytes = Encoding.UTF8.GetBytes(responseBody);
                            await response.OutputStream.WriteAsync(bytes).ConfigureAwait(false);
                        }
                        finally
                        {
                            response.Close();
                        }
                    },
                };
            }
        }

        public ValueTask DisposeAsync()
        {
            try
            {
                _listener.Stop();
            }
            finally
            {
                ((IDisposable)_listener).Dispose();
            }

            return ValueTask.CompletedTask;
        }
    }
}
