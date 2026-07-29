using Microsoft.AspNetCore.Builder;

namespace ThunderPropagator.ClusterMessageBuses.Grpc
{
    /// <summary>Real <see cref="IGrpcClusterHost"/>, backed by an embedded <see cref="WebApplication"/>.</summary>
    internal sealed class AspNetCoreGrpcClusterHost : IGrpcClusterHost
    {
        private readonly WebApplication _app;

        internal AspNetCoreGrpcClusterHost(WebApplication app)
        {
            _app = app;
        }

        public Task StartAsync(CancellationToken cancellationToken) => _app.StartAsync(cancellationToken);

        public async ValueTask DisposeAsync()
        {
            await _app.StopAsync().ConfigureAwait(false);
            await _app.DisposeAsync().ConfigureAwait(false);
        }
    }
}
