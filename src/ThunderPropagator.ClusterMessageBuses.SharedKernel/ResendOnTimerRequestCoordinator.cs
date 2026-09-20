namespace ThunderPropagator.ClusterMessageBuses.SharedKernel
{
    /// <summary>
    /// Shared "resend the request itself on a timer until a reply arrives or the overall timeout
    /// elapses" loop, for transports whose underlying delivery has no guarantee at all (plain
    /// UDP-based transports: Udp, Swim) -- unlike <see cref="ClusterResiliencePipelineFactory"/>,
    /// whose retry/circuit-breaker policy protects a single send call and assumes the underlying
    /// transport otherwise guarantees delivery of whatever it did manage to send. Consolidates what
    /// used to be an identical hand-rolled loop duplicated between <c>UdpClusterMessageBus</c> and
    /// <c>SwimClusterMessageBus</c>.
    /// </summary>
    public static class ResendOnTimerRequestCoordinator
    {
        /// <summary>
        /// Calls <paramref name="sendOnceAsync"/> immediately, then again every
        /// <paramref name="resendInterval"/> for as long as <paramref name="overallTimeout"/> has not
        /// yet elapsed and <paramref name="pendingResponse"/> hasn't completed, racing each resend
        /// against the same <paramref name="pendingResponse"/> rather than creating a new wait per
        /// attempt (a matching reply completes whichever attempt is currently waiting, regardless of
        /// which resend it answers).
        /// </summary>
        /// <typeparam name="TResponse">The response type a matching reply completes <paramref name="pendingResponse"/> with.</typeparam>
        /// <param name="pendingResponse">
        /// The pending wait to race against -- typically obtained from
        /// <see cref="PendingRequestTracker{TResponse}.Register"/>, completed by the caller's own
        /// receive loop when a matching reply arrives.
        /// </param>
        /// <param name="sendOnceAsync">Sends one copy of the request datagram/frame.</param>
        /// <param name="resendInterval">How long to wait for a reply before resending.</param>
        /// <param name="overallTimeout">Total time budget across every resend before giving up.</param>
        /// <param name="timeoutMessage">Exception message used if <paramref name="overallTimeout"/> elapses with no reply.</param>
        /// <param name="cancellationToken">Propagated immediately (without resending further) if cancelled.</param>
        /// <exception cref="TimeoutException"><paramref name="overallTimeout"/> elapsed with no matching reply.</exception>
        public static async Task<TResponse> SendWithResendAsync<TResponse>(
            TaskCompletionSource<TResponse> pendingResponse,
            Func<CancellationToken, Task> sendOnceAsync,
            TimeSpan resendInterval,
            TimeSpan overallTimeout,
            string timeoutMessage,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(pendingResponse);
            ArgumentNullException.ThrowIfNull(sendOnceAsync);

            var deadline = DateTime.UtcNow + overallTimeout;

            while (true)
            {
                await sendOnceAsync(cancellationToken).ConfigureAwait(false);

                var remaining = deadline - DateTime.UtcNow;
                if (remaining <= TimeSpan.Zero)
                    throw new TimeoutException(timeoutMessage);

                var waitTime = remaining < resendInterval ? remaining : resendInterval;
                using var waitCts = new CancellationTokenSource(waitTime);
                using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, waitCts.Token);

                var delayTask = Task.Delay(Timeout.InfiniteTimeSpan, linkedCts.Token);
                var completed = await Task.WhenAny(pendingResponse.Task, delayTask).ConfigureAwait(false);

                if (completed == pendingResponse.Task)
                    return await pendingResponse.Task.ConfigureAwait(false);

                // The delay "won" the race -- either this attempt's resend interval elapsed (loop
                // around and resend) or the caller's own cancellationToken fired (propagate
                // immediately rather than resending forever).
                cancellationToken.ThrowIfCancellationRequested();
            }
        }
    }
}
