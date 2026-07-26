using System.Net;
using System.Net.WebSockets;
using System.Runtime.CompilerServices;

namespace ThunderPropagator.ClusterMessageBuses.WebSocket
{
    /// <summary>
    /// Production <see cref="IWebSocketClusterListener"/>: wraps a real <see cref="HttpListener"/>
    /// bound to this node's own <see cref="WebSocketAddressing.ListenPrefix"/>, accepting the WebSocket
    /// upgrade handshake on every inbound HTTP request it receives.
    /// </summary>
    internal sealed class HttpListenerWebSocketClusterListener : IWebSocketClusterListener
    {
        private readonly HttpListener _listener;

        internal HttpListenerWebSocketClusterListener(string listenPrefix)
        {
            _listener = new HttpListener();
            _listener.Prefixes.Add(listenPrefix.EndsWith('/') ? listenPrefix : $"{listenPrefix}/");
            _listener.Start();
        }

        public async IAsyncEnumerable<System.Net.WebSockets.WebSocket> AcceptConnectionsAsync([EnumeratorCancellation] CancellationToken cancellationToken)
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

                if (!context.Request.IsWebSocketRequest)
                {
                    context.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                    context.Response.Close();
                    continue;
                }

                WebSocketContext webSocketContext;
                try
                {
                    webSocketContext = await context.AcceptWebSocketAsync(subProtocol: null).ConfigureAwait(false);
                }
                catch (Exception)
                {
                    // A single failed upgrade handshake must not take down the whole listener.
                    context.Response.StatusCode = (int)HttpStatusCode.InternalServerError;
                    context.Response.Close();
                    continue;
                }

                yield return webSocketContext.WebSocket;
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
