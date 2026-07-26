namespace ThunderPropagator.ClusterMessageBuses.WebApi
{
    /// <summary>
    /// A single inbound HTTP request accepted by this node's embedded listener, reduced to the plain
    /// data <see cref="WebApiClusterMessageBus"/>'s routing logic needs — deliberately not the real
    /// <see cref="System.Net.HttpListenerContext"/> (a concrete class with no virtual members, so not
    /// directly substitutable), so tests can construct one directly without binding a real socket.
    /// </summary>
    internal sealed class WebApiIncomingRequest
    {
        internal required string Method { get; init; }
        internal required string Path { get; init; }
        internal string? Query { get; init; }
        internal required string Body { get; init; }

        /// <summary>Writes the given status code and UTF8 JSON body back to the requester and closes the response.</summary>
        internal required Func<int, string, Task> RespondAsync { get; init; }
    }
}
