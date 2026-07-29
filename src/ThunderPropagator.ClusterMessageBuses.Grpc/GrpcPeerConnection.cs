using Grpc.Core;
using Grpc.Net.Client;
using ThunderPropagator.ClusterMessageBuses.Grpc.Protos;

namespace ThunderPropagator.ClusterMessageBuses.Grpc
{
    /// <summary>
    /// One physical outbound connection to a peer: a pair of long-lived duplex-streaming calls (one
    /// for fan-out, one for subscription-sync), opened once and reused across every publish to that
    /// peer until the connection is invalidated (see <c>ClusterConnectionCache.InvalidateAsync</c>).
    /// <c>ClusterFanOutClient</c>/<c>ClusterSubscriptionSyncClient</c> are <c>ClientBase&lt;T&gt;</c>-
    /// derived, generated with a protected parameterless constructor and virtual RPC methods
    /// specifically to support direct test substitution (the same reasoning as GcpPubSub's GAPIC
    /// clients elsewhere in this repo), so the <c>channel</c> constructor parameter is only ever
    /// non-null in production — tests construct this directly from substitute clients with no real
    /// channel to dispose.
    /// </summary>
    internal sealed class GrpcPeerConnection : IAsyncDisposable
    {
        private readonly GrpcChannel? _channel;

        // A gRPC stream writer is not safe for concurrent callers, mirroring the same constraint
        // (and the same SemaphoreSlim(1,1)-serialized fix) WebSocketPeerConnection.SendFrameAsync
        // applies to System.Net.WebSockets.WebSocket.SendAsync.
        private readonly SemaphoreSlim _fanOutWriteLock = new(1, 1);
        private readonly SemaphoreSlim _subscriptionSyncWriteLock = new(1, 1);

        internal AsyncDuplexStreamingCall<PushedMessageBatch, PushedMessageBatch> FanOutCall { get; }
        internal AsyncDuplexStreamingCall<SubscriptionEvent, SubscriptionAck> SubscriptionSyncCall { get; }

        internal GrpcPeerConnection(
            ClusterFanOut.ClusterFanOutClient fanOutClient,
            ClusterSubscriptionSync.ClusterSubscriptionSyncClient subscriptionSyncClient,
            CancellationToken cancellationToken,
            GrpcChannel? channel = null)
        {
            _channel = channel;
            FanOutCall = fanOutClient.Stream(cancellationToken: cancellationToken);
            SubscriptionSyncCall = subscriptionSyncClient.Stream(cancellationToken: cancellationToken);
        }

        internal async Task SendFanOutAsync(PushedMessageBatch message, CancellationToken cancellationToken)
        {
            await _fanOutWriteLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                await FanOutCall.RequestStream.WriteAsync(message).ConfigureAwait(false);
            }
            finally
            {
                _fanOutWriteLock.Release();
            }
        }

        internal async Task SendSubscriptionEventAsync(SubscriptionEvent subscriptionEvent, CancellationToken cancellationToken)
        {
            await _subscriptionSyncWriteLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                await SubscriptionSyncCall.RequestStream.WriteAsync(subscriptionEvent).ConfigureAwait(false);
            }
            finally
            {
                _subscriptionSyncWriteLock.Release();
            }
        }

        public async ValueTask DisposeAsync()
        {
            try
            {
                await FanOutCall.RequestStream.CompleteAsync().ConfigureAwait(false);
            }
            catch
            {
                // Best-effort — the stream may already be broken, which is exactly why we're disposing it.
            }

            try
            {
                await SubscriptionSyncCall.RequestStream.CompleteAsync().ConfigureAwait(false);
            }
            catch
            {
                // Best-effort, same reasoning as above.
            }

            FanOutCall.Dispose();
            SubscriptionSyncCall.Dispose();

            if (_channel is not null)
            {
                await _channel.ShutdownAsync().ConfigureAwait(false);
                _channel.Dispose();
            }
        }
    }
}
