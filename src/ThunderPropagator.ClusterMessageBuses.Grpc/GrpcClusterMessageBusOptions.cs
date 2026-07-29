using Grpc.Net.Client;
using Microsoft.AspNetCore.Server.Kestrel.Core;

namespace ThunderPropagator.ClusterMessageBuses.Grpc
{
    /// <summary>Options controlling <see cref="GrpcClusterMessageBus"/>'s peer-stream lifecycle.</summary>
    public sealed class GrpcClusterMessageBusOptions
    {
        /// <summary>How long to wait before the first reconnect attempt after a peer stream fails.</summary>
        public TimeSpan InitialReconnectDelay { get; set; } = TimeSpan.FromSeconds(1);

        /// <summary>
        /// The ceiling the exponential reconnect back-off is capped at — doubled on every
        /// consecutive failed attempt starting from <see cref="InitialReconnectDelay"/>, reset back
        /// to it after a successful reconnect.
        /// </summary>
        public TimeSpan MaxReconnectDelay { get; set; } = TimeSpan.FromSeconds(30);

        /// <summary>HTTP/2 keepalive ping interval for every outbound peer channel.</summary>
        public TimeSpan KeepAliveInterval { get; set; } = TimeSpan.FromSeconds(30);

        /// <summary>How long a leader/peer-pull request (restore/sync-delta/fetch-subscriptions) waits before timing out.</summary>
        public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(30);

        /// <summary>
        /// Allows dialing peers over plain-text HTTP/2 (h2c) instead of requiring TLS — for local
        /// development/testing only; every peer's <c>NodeEndpoint</c> must already agree on this
        /// (gRPC has no scheme negotiation, unlike the WebSocket transport's https-to-wss mapping).
        /// </summary>
        public bool FallbackToHttp { get; set; }

        /// <summary>Extension point for advanced <see cref="GrpcChannelOptions"/> tuning (compression, message size limits, ...) applied to every outbound peer channel.</summary>
        public Action<GrpcChannelOptions>? ConfigureChannel { get; set; }

        /// <summary>
        /// Extension point for the embedded Kestrel server this bus self-hosts for its inbound gRPC
        /// endpoints — required to configure a TLS certificate unless <see cref="FallbackToHttp"/>
        /// is used instead.
        /// </summary>
        public Action<KestrelServerOptions>? ConfigureKestrel { get; set; }
    }
}
