using Microsoft.Extensions.Logging;
using ThunderPropagator.BuildingBlocks.Application.Helpers;

namespace ThunderPropagator.ClusterMessageBuses.NATS
{
    /// <summary>
    /// Request/reply plumbing for the three leader/peer-pull operations
    /// (<c>RestoreFromLeaderAsync</c>, <c>SyncDeltaFromLeaderAsync</c>, <c>FetchPeerSubscriptionsAsync</c>).
    /// Unlike <c>KafkaClusterMessageBus</c> / <c>RabbitMqClusterMessageBus</c>, there is no
    /// correlation-id tracking, pending-request dictionary, or separate reply listener here: NATS's
    /// native request/reply (<see cref="INatsClusterTransport.RequestAsync"/>) already performs the
    /// full round trip — publish the request, wait for exactly one reply on a per-call ephemeral
    /// inbox subject, and return it (or <see langword="null"/> on timeout) — as a single call.
    /// <see cref="RunRequestListenerLoopAsync"/> is the answering side: it subscribes to this node's
    /// own request subject and replies to each delivery's <see cref="NatsClusterDelivery.ReplyTo"/>
    /// subject, which is exactly the ephemeral inbox the requester's <c>RequestAsync</c> call is
    /// waiting on.
    /// </summary>
    internal sealed partial class NatsClusterMessageBus
    {
        /// <summary>Internal (rather than private) so tests can exercise the requester side directly.</summary>
        internal async Task<NatsClusterResponseEnvelope> SendRequestAsync(
            Uri targetNodeEndpoint,
            NatsClusterRequestKind kind,
            string? channelName,
            Guid? channelKey,
            long? sinceTicks,
            CancellationToken cancellationToken)
        {
            await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            var request = new NatsClusterRequestEnvelope(kind, channelName, channelKey, sinceTicks);
            var subject = NatsSubjectNaming.RequestSubject(_options.SubjectPrefix, targetNodeEndpoint);

            using var timeoutCts = new CancellationTokenSource(_options.RequestTimeout);
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);

            string? replyJson;
            try
            {
                replyJson = await _transport.RequestAsync(subject, request.ToNJson(), linkedCts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (OperationCanceledException)
            {
                throw new TimeoutException($"NATS cluster request '{kind}' to '{targetNodeEndpoint.Host}' timed out after {_options.RequestTimeout}.");
            }

            if (replyJson is null)
                throw new TimeoutException($"NATS cluster request '{kind}' to '{targetNodeEndpoint.Host}' received no reply within {_options.RequestTimeout}.");

            var response = replyJson.FromNJson<NatsClusterResponseEnvelope>()
                ?? throw new InvalidOperationException($"NATS cluster request '{kind}' to '{targetNodeEndpoint.Host}' returned an unparseable response.");

            if (!response.Success)
                throw new InvalidOperationException($"NATS cluster request '{kind}' to '{targetNodeEndpoint.Host}' was rejected: {response.ErrorMessage}");

            return response;
        }

        internal async Task RunRequestListenerLoopAsync(CancellationToken cancellationToken)
        {
            var subject = NatsSubjectNaming.RequestSubject(_options.SubjectPrefix, _nodeEndpoint);

            try
            {
                await foreach (var delivery in _transport.SubscribeAsync(subject, cancellationToken).ConfigureAwait(false))
                {
                    await HandleRequestDeliveryAsync(delivery, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
                // Expected on shutdown.
            }
        }

        /// <summary>Internal (rather than private) so tests can drive it directly with a raw delivery.</summary>
        internal async Task HandleRequestDeliveryAsync(NatsClusterDelivery delivery, CancellationToken cancellationToken)
        {
            NatsClusterRequestEnvelope? request;
            try
            {
                request = delivery.Payload.FromNJson<NatsClusterRequestEnvelope>();
            }
            catch (Exception exception)
            {
                Log.RequestUnparseable(_logger, exception);
                return;
            }

            if (request is null)
                return;

            var response = await BuildResponseAsync(request, cancellationToken).ConfigureAwait(false);

            if (delivery.ReplyTo is null)
            {
                // Should never happen for a delivery that arrived via a real NATS request, but a
                // plain publish to this subject (misconfiguration, or a test) has nowhere to reply to.
                Log.RequestHadNoReplyTo(_logger);
                return;
            }

            try
            {
                await _transport.PublishAsync(delivery.ReplyTo, response.ToNJson(), cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Log.ReplyPublishFailed(_logger, exception, delivery.ReplyTo);
            }
        }

        internal async Task<NatsClusterResponseEnvelope> BuildResponseAsync(NatsClusterRequestEnvelope request, CancellationToken cancellationToken)
        {
            try
            {
                return request.Kind switch
                {
                    NatsClusterRequestKind.RestoreSnapshot => await BuildRestoreSnapshotResponseAsync(request, cancellationToken).ConfigureAwait(false),
                    NatsClusterRequestKind.SyncDelta => await BuildSyncDeltaResponseAsync(request, cancellationToken).ConfigureAwait(false),
                    NatsClusterRequestKind.FetchSubscriptions => BuildFetchSubscriptionsResponse(request),
                    _ => new NatsClusterResponseEnvelope(false, $"Unknown request kind '{request.Kind}'.", null)
                };
            }
            catch (Exception exception)
            {
                Log.RequestHandlingFaulted(_logger, exception, request.Kind.ToString());
                return new NatsClusterResponseEnvelope(false, exception.Message, null);
            }
        }

        private static partial class Log
        {
            [LoggerMessage(EventId = 9730, Level = LogLevel.Warning,
                Message = "[Cluster] NATS cluster request could not be parsed; skipping it.")]
            public static partial void RequestUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 9731, Level = LogLevel.Warning,
                Message = "[Cluster] NATS cluster request delivery had no reply-to subject; cannot answer it.")]
            public static partial void RequestHadNoReplyTo(ILogger logger);

            [LoggerMessage(EventId = 9732, Level = LogLevel.Warning,
                Message = "[Cluster] NATS reply publish to subject '{Subject}' failed.")]
            public static partial void ReplyPublishFailed(ILogger logger, Exception exception, string subject);

            [LoggerMessage(EventId = 9733, Level = LogLevel.Error,
                Message = "[Cluster] NATS request handling faulted for kind '{Kind}'.")]
            public static partial void RequestHandlingFaulted(ILogger logger, Exception exception, string kind);
        }
    }
}
