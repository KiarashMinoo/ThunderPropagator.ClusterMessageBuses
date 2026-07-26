using Microsoft.Extensions.Logging;

namespace ThunderPropagator.ClusterMessageBuses.WebApi
{
    /// <summary>
    /// Routes every inbound <see cref="WebApiIncomingRequest"/> this node's listener accepts to the
    /// matching handler, and translates the outcome into a plain HTTP status code — this transport's
    /// error-signaling model, distinct from (and simpler than) the JSON success/error envelope every
    /// other transport in this repo needs, since HTTP already has native status codes.
    /// </summary>
    internal sealed partial class WebApiClusterMessageBus
    {
        /// <summary>Internal (rather than private) so tests can drive it directly with a constructed <see cref="WebApiIncomingRequest"/>.</summary>
        internal async Task DispatchRequestAsync(WebApiIncomingRequest request, CancellationToken cancellationToken)
        {
            var match = WebApiClusterRouting.Match(_options.ListenPath, request.Method, request.Path, request.Query);
            if (match is null)
            {
                await request.RespondAsync(404, "{}").ConfigureAwait(false);
                return;
            }

            try
            {
                switch (match.Kind)
                {
                    case WebApiRouteKind.FanOut:
                        await HandleFanOutDeliveryAsync(request.Body, match.ChannelKey!.Value, cancellationToken).ConfigureAwait(false);
                        await request.RespondAsync(204, "{}").ConfigureAwait(false);
                        break;

                    case WebApiRouteKind.SubscriptionEvent:
                        await HandleSubscriptionEventDeliveryAsync(request.Body, match.ChannelKey!.Value, cancellationToken).ConfigureAwait(false);
                        await request.RespondAsync(204, "{}").ConfigureAwait(false);
                        break;

                    case WebApiRouteKind.Snapshot:
                        var snapshotBody = await BuildSnapshotResponseBodyAsync(match.ChannelName!, cancellationToken).ConfigureAwait(false);
                        await request.RespondAsync(200, snapshotBody).ConfigureAwait(false);
                        break;

                    case WebApiRouteKind.SnapshotDelta:
                        var deltaBody = await BuildSnapshotDeltaResponseBodyAsync(match.ChannelName!, match.SinceTicks!.Value, cancellationToken).ConfigureAwait(false);
                        await request.RespondAsync(200, deltaBody).ConfigureAwait(false);
                        break;

                    case WebApiRouteKind.SubscriptionsSelf:
                        var subscriptionsBody = BuildSubscriptionsSelfResponseBody(match.ChannelKey!.Value);
                        await request.RespondAsync(200, subscriptionsBody).ConfigureAwait(false);
                        break;

                    default:
                        await request.RespondAsync(404, "{}").ConfigureAwait(false);
                        break;
                }
            }
            catch (Exception exception)
            {
                // A single faulted request (e.g. an unknown channel name/key) must not crash the
                // listener's accept loop — respond 500 and keep serving other requests.
                Log.RequestHandlingFaulted(_logger, exception, match.Kind.ToString());
                await request.RespondAsync(500, "{}").ConfigureAwait(false);
            }
        }

        private static partial class Log
        {
            [LoggerMessage(EventId = 91310, Level = LogLevel.Error,
                Message = "[Cluster] WebApi request handling faulted for route kind '{Kind}'.")]
            public static partial void RequestHandlingFaulted(ILogger logger, Exception exception, string kind);
        }
    }
}
