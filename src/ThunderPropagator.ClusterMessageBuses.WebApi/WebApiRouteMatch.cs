namespace ThunderPropagator.ClusterMessageBuses.WebApi
{
    /// <summary>Result of successfully matching an inbound request's method/path/query against a known route.</summary>
    internal sealed record WebApiRouteMatch(WebApiRouteKind Kind, Guid? ChannelKey = null, string? ChannelName = null, long? SinceTicks = null);
}
