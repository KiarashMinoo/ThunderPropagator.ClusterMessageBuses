namespace ThunderPropagator.ClusterMessageBuses.Grpc
{
    /// <summary>
    /// Narrow seam around the embedded ASP.NET Core/Kestrel host this bus self-binds for its inbound
    /// gRPC endpoints. <c>WebApplication</c> itself has no interface and cannot be substituted
    /// directly, so — like <c>IWebSocketClusterListener</c>/<c>IWebApiClusterListener</c> elsewhere
    /// in this repo — this interface exists purely as a test seam; production always uses
    /// <see cref="AspNetCoreGrpcClusterHost"/>.
    /// </summary>
    internal interface IGrpcClusterHost : IAsyncDisposable
    {
        Task StartAsync(CancellationToken cancellationToken);
    }
}
