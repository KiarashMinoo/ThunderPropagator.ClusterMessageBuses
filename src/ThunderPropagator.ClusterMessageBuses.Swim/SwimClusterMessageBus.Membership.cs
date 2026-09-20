using System.Net;
using Microsoft.Extensions.Logging;
using ThunderPropagator.BuildingBlocks.Application.Helpers;

namespace ThunderPropagator.ClusterMessageBuses.Swim
{
    /// <summary>
    /// The SWIM protocol itself: the gossip-round loop (periodic epidemic dissemination of the
    /// application broadcast queue) and the probe loop (direct/indirect liveness probing,
    /// suspicion-timeout expiry, self-refutation), plus the wire-level Ping/PingReq/Ack handlers that
    /// back them. See <see cref="SwimClusterMessageBus"/>'s own remarks for the overall design.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Indirect-probe relay flow.</b> A probe transaction reuses one <c>SequenceId</c>
    /// end-to-end. The original prober sends a direct <see cref="SwimPingPayload"/> to the target
    /// using a fresh <c>SequenceId</c> and registers a pending wait for it in
    /// <see cref="_pendingAcks"/>. If that times out, the prober sends a <see cref="SwimPingReqPayload"/>
    /// carrying the <i>same</i> <c>SequenceId</c> to each of <see cref="SwimClusterMessageBusOptions.IndirectProbeRelayCount"/>
    /// other members. Each relay, on receiving that <see cref="SwimPingReqPayload"/>, remembers
    /// "forward whatever ack matches this <c>SequenceId</c> back to whoever just asked me" in
    /// <see cref="_relayForwardTargets"/>, then sends its own direct <see cref="SwimPingPayload"/> to
    /// the real target, reusing that same <c>SequenceId</c> again. When the target acks (it has no
    /// way to tell a relayed probe from a direct one, nor does it need to), whichever node receives
    /// that <see cref="SwimAckPayload"/> either completes its own pending wait (if it's the original
    /// prober), forwards the ack on to whoever it's relaying for (if it's a relay), or both are
    /// possible reactions to the very same inbound ack and both are attempted independently in
    /// <see cref="HandleAckAsync"/>.
    /// </para>
    /// <para>
    /// <b>Suspicion/incarnation state machine.</b> <see cref="MarkSuspect"/> and
    /// <see cref="ExpireSuspects"/> only ever gossip a state transition when
    /// <see cref="SwimMembershipTable.Merge"/> reports it actually changed something -- redundant
    /// re-suspicion of an already-Suspect host at the same incarnation is a no-op, exactly matching
    /// the precedence rule documented on <see cref="SwimMembershipUpdate.Incarnation"/>. Self-refutation
    /// in <see cref="MergeAndMaybeRefute"/> bumps this node's own incarnation to
    /// <c>max(current, claimedIncarnation) + 1</c> -- strictly higher than whatever claim triggered
    /// it, not just "+1" from this node's own last-known value, since a claim can itself already carry
    /// a higher incarnation than this node remembers issuing (e.g. after a restart).
    /// </para>
    /// </remarks>
    internal sealed partial class SwimClusterMessageBus
    {
        // Per-message-type piggyback batch sizes. These are deliberately small, fixed constants
        // rather than a public option: with MaxDatagramSize's default of 60000 bytes and this
        // cluster's payload shapes, a handful of items per packet stays comfortably within budget
        // without needing a byte-accounting packer. A deployment pushing unusually large messages
        // through this transport should lower these alongside raising MaxDatagramSize if datagrams
        // start being rejected as oversized -- see SendDatagramAsync's own size check.
        private const int PiggybackAppItemBatchSize = 3;
        private const int PiggybackMembershipBatchSize = 5;
        private const int StandaloneGossipBatchSize = 5;

        /// <summary>
        /// Periodically reconciles membership against discovery and disseminates a batch of queued
        /// application broadcast items to <see cref="SwimClusterMessageBusOptions.GossipFanout"/>
        /// random alive peers. Internal (rather than private) so tests can drive one round directly.
        /// </summary>
        internal async Task RunGossipRoundLoopAsync(CancellationToken cancellationToken)
        {
            try
            {
                using var timer = new PeriodicTimer(_options.GossipInterval);
                while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
                {
                    await RunGossipRoundOnceAsync(cancellationToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Expected on shutdown.
            }
            catch (Exception exception)
            {
                Log.GossipRoundLoopFaulted(_logger, exception);
            }
        }

        internal async Task RunGossipRoundOnceAsync(CancellationToken cancellationToken)
        {
            var peers = await _discovery.GetPeersAsync(cancellationToken).ConfigureAwait(false);
            var hosts = peers.Select(p => p.Endpoint.Host).ToList();
            _membershipTable.ReconcileWithDiscovery(hosts);

            var targets = _membershipTable.GetRandomAlivePeers(_options.GossipFanout);

            foreach (var target in targets)
            {
                var items = _appBroadcastQueue.TakeBatch(StandaloneGossipBatchSize, _membershipTable.KnownMemberCount, _options.RetransmitMult);
                if (items.Count == 0)
                    break; // Queue drained -- nothing left to disseminate to the remaining targets this round.

                IPEndPoint remoteEndpoint;
                try
                {
                    remoteEndpoint = await _peerEndpointResolver(target, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    Log.GossipSendToPeerFailed(_logger, exception, target);
                    continue;
                }

                foreach (var item in items)
                {
                    try
                    {
                        await SendDatagramAsync(item, remoteEndpoint, cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception exception)
                    {
                        Log.GossipSendToPeerFailed(_logger, exception, target);
                    }
                }
            }
        }

        /// <summary>
        /// SWIM's own failure-detection cycle: expires stale suspicions, then probes one random
        /// alive member. Internal (rather than private) so tests can drive one round directly.
        /// </summary>
        internal async Task RunProbeLoopAsync(CancellationToken cancellationToken)
        {
            try
            {
                using var timer = new PeriodicTimer(_options.GossipInterval);
                while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
                {
                    await RunProbeRoundOnceAsync(cancellationToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Expected on shutdown.
            }
            catch (Exception exception)
            {
                Log.ProbeLoopFaulted(_logger, exception);
            }
        }

        internal async Task RunProbeRoundOnceAsync(CancellationToken cancellationToken)
        {
            ExpireSuspects();

            var candidates = _membershipTable.GetRandomAlivePeers(1);
            if (candidates.Count == 0)
                return;

            await ProbeMemberAsync(candidates[0], cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Runs one full SWIM probe transaction against <paramref name="targetHost"/>: a direct ping,
        /// then (if that times out) an indirect ping-req fan-out, then (if that also times out)
        /// marking the target Suspect. Internal (rather than private) so tests can drive it directly.
        /// </summary>
        internal async Task ProbeMemberAsync(string targetHost, CancellationToken cancellationToken)
        {
            var sequenceId = Guid.NewGuid();
            var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            if (!_pendingAcks.TryAdd(sequenceId, tcs))
                return;

            try
            {
                IPEndPoint targetEndpoint;
                try
                {
                    targetEndpoint = await _peerEndpointResolver(targetHost, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    Log.ProbeAddressResolutionFailed(_logger, exception, targetHost);
                    return;
                }

                await SendPingAsync(sequenceId, targetEndpoint, cancellationToken).ConfigureAwait(false);

                if (await WaitForAckAsync(tcs, _options.ProbeTimeout, cancellationToken).ConfigureAwait(false))
                {
                    Log.ProbeSucceededDirect(_logger, targetHost);
                    return;
                }

                var relays = _membershipTable.GetRandomAlivePeers(_options.IndirectProbeRelayCount, exclude: targetHost);
                foreach (var relay in relays)
                {
                    try
                    {
                        var relayEndpoint = await _peerEndpointResolver(relay, cancellationToken).ConfigureAwait(false);
                        await SendPingReqAsync(sequenceId, targetHost, relayEndpoint, cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception exception)
                    {
                        Log.IndirectProbeSendFailed(_logger, exception, relay, targetHost);
                    }
                }

                if (relays.Count > 0 && await WaitForAckAsync(tcs, _options.ProbeTimeout, cancellationToken).ConfigureAwait(false))
                {
                    Log.ProbeSucceededIndirect(_logger, targetHost);
                    return;
                }

                Log.ProbeFailed(_logger, targetHost);
                MarkSuspect(targetHost);
            }
            finally
            {
                _pendingAcks.TryRemove(sequenceId, out _);
            }
        }

        private static async Task<bool> WaitForAckAsync(TaskCompletionSource<bool> tcs, TimeSpan timeout, CancellationToken cancellationToken)
        {
            using var timeoutCts = new CancellationTokenSource(timeout);
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);

            var delayTask = Task.Delay(Timeout.InfiniteTimeSpan, linkedCts.Token);
            var completed = await Task.WhenAny(tcs.Task, delayTask).ConfigureAwait(false);

            if (completed != tcs.Task)
            {
                cancellationToken.ThrowIfCancellationRequested();
                return false;
            }

            return await tcs.Task.ConfigureAwait(false);
        }

        private void MarkSuspect(string targetHost)
        {
            var incarnation = _membershipTable.GetIncarnation(targetHost);
            var update = new SwimMembershipUpdate(targetHost, SwimMemberState.Suspect, incarnation);
            if (_membershipTable.Merge(update))
            {
                _membershipBroadcastQueue.Enqueue(update);
                Log.MemberSuspected(_logger, targetHost, incarnation);
            }
        }

        private void ExpireSuspects()
        {
            var expired = _membershipTable.GetExpiredSuspects(_options.SuspicionTimeout);
            foreach (var host in expired)
            {
                var incarnation = _membershipTable.GetIncarnation(host);
                var update = new SwimMembershipUpdate(host, SwimMemberState.Dead, incarnation);
                if (_membershipTable.Merge(update))
                {
                    _membershipBroadcastQueue.Enqueue(update);
                    Log.MemberDeclaredDead(_logger, host, incarnation);
                }
            }
        }

        private async Task SendPingAsync(Guid sequenceId, IPEndPoint targetEndpoint, CancellationToken cancellationToken)
        {
            var payload = new SwimPingPayload(sequenceId, _selfHost, TakeMembershipUpdates(), TakePiggybackedItems());
            await SendDatagramAsync(new SwimDatagram(SwimMessageKind.Ping, payload.ToNJson()), targetEndpoint, cancellationToken).ConfigureAwait(false);
        }

        private async Task SendPingReqAsync(Guid sequenceId, string targetHost, IPEndPoint relayEndpoint, CancellationToken cancellationToken)
        {
            var payload = new SwimPingReqPayload(sequenceId, targetHost, TakeMembershipUpdates(), TakePiggybackedItems());
            await SendDatagramAsync(new SwimDatagram(SwimMessageKind.PingReq, payload.ToNJson()), relayEndpoint, cancellationToken).ConfigureAwait(false);
        }

        private async Task SendAckAsync(Guid sequenceId, IPEndPoint destinationEndpoint, CancellationToken cancellationToken)
        {
            var payload = new SwimAckPayload(sequenceId, _selfHost, TakeMembershipUpdates(), TakePiggybackedItems());
            await SendDatagramAsync(new SwimDatagram(SwimMessageKind.Ack, payload.ToNJson()), destinationEndpoint, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Forwards <paramref name="originalAck"/> on to <paramref name="destinationEndpoint"/> for
        /// an indirect-probe relay -- preserves the original acking member's identity
        /// (<see cref="SwimAckPayload.SourceHost"/>) and correlation id unchanged (this relay is not
        /// the one who was probed), but refreshes the piggyback batch opportunistically since this
        /// relay's own queues may have new items to spread since the ack was first received.
        /// </summary>
        private async Task ForwardAckAsync(SwimAckPayload originalAck, IPEndPoint destinationEndpoint, CancellationToken cancellationToken)
        {
            var payload = new SwimAckPayload(originalAck.SequenceId, originalAck.SourceHost, TakeMembershipUpdates(), TakePiggybackedItems());
            await SendDatagramAsync(new SwimDatagram(SwimMessageKind.Ack, payload.ToNJson()), destinationEndpoint, cancellationToken).ConfigureAwait(false);
        }

        private SwimMembershipUpdate[] TakeMembershipUpdates()
            => _membershipBroadcastQueue.TakeBatch(PiggybackMembershipBatchSize, _membershipTable.KnownMemberCount, _options.RetransmitMult).ToArray();

        private SwimDatagram[] TakePiggybackedItems()
            => _appBroadcastQueue.TakeBatch(PiggybackAppItemBatchSize, _membershipTable.KnownMemberCount, _options.RetransmitMult).ToArray();

        /// <summary>Internal (rather than private) so tests can drive it directly with a raw payload.</summary>
        internal async Task HandlePingAsync(string payloadJson, IPEndPoint remoteEndpoint, CancellationToken cancellationToken)
        {
            SwimPingPayload? ping;
            try
            {
                ping = payloadJson.FromNJson<SwimPingPayload>();
            }
            catch (Exception exception)
            {
                Log.PingUnparseable(_logger, exception);
                return;
            }

            if (ping is null)
                return;

            await ApplyIncomingAsync(ping.Updates, ping.Piggybacked, cancellationToken).ConfigureAwait(false);

            try
            {
                await SendAckAsync(ping.SequenceId, remoteEndpoint, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Log.AckSendFailed(_logger, exception);
            }
        }

        /// <summary>Internal (rather than private) so tests can drive it directly with a raw payload.</summary>
        internal async Task HandlePingReqAsync(string payloadJson, IPEndPoint remoteEndpoint, CancellationToken cancellationToken)
        {
            SwimPingReqPayload? pingReq;
            try
            {
                pingReq = payloadJson.FromNJson<SwimPingReqPayload>();
            }
            catch (Exception exception)
            {
                Log.PingReqUnparseable(_logger, exception);
                return;
            }

            if (pingReq is null)
                return;

            await ApplyIncomingAsync(pingReq.Updates, pingReq.Piggybacked, cancellationToken).ConfigureAwait(false);

            // Remember who to forward the eventual ack back to, then probe the real target ourselves,
            // reusing the same SequenceId -- see this class's own remarks for the full relay flow.
            _relayForwardTargets[pingReq.SequenceId] = remoteEndpoint;

            try
            {
                var targetEndpoint = await _peerEndpointResolver(pingReq.TargetHost, cancellationToken).ConfigureAwait(false);
                await SendPingAsync(pingReq.SequenceId, targetEndpoint, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Log.IndirectProbeRelayFailed(_logger, exception, pingReq.TargetHost);
                _relayForwardTargets.TryRemove(pingReq.SequenceId, out _);
            }
        }

        /// <summary>Internal (rather than private) so tests can drive it directly with a raw payload.</summary>
        internal async Task HandleAckAsync(string payloadJson, CancellationToken cancellationToken)
        {
            SwimAckPayload? ack;
            try
            {
                ack = payloadJson.FromNJson<SwimAckPayload>();
            }
            catch (Exception exception)
            {
                Log.AckUnparseable(_logger, exception);
                return;
            }

            if (ack is null)
                return;

            await ApplyIncomingAsync(ack.Updates, ack.Piggybacked, cancellationToken).ConfigureAwait(false);

            // Complete our own pending probe wait, if this ack answers one we're directly waiting on.
            if (_pendingAcks.TryGetValue(ack.SequenceId, out var tcs))
                tcs.TrySetResult(true);

            // If we relayed an indirect probe for this SequenceId on someone else's behalf, forward
            // this ack on to whoever originally asked us to relay it. Both reactions can legitimately
            // apply to the very same inbound ack -- a node can simultaneously be the original prober
            // for one probe and a relay for a different, unrelated one that happens to share no state,
            // but even for the very same SequenceId there is no conflict: at most one of the two
            // dictionaries will actually contain an entry for it on any given node.
            if (_relayForwardTargets.TryRemove(ack.SequenceId, out var forwardTo))
            {
                try
                {
                    await ForwardAckAsync(ack, forwardTo, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    Log.AckForwardFailed(_logger, exception);
                }
            }
        }

        private async Task ApplyIncomingAsync(SwimMembershipUpdate[] updates, SwimDatagram[] piggybacked, CancellationToken cancellationToken)
        {
            foreach (var update in updates)
            {
                MergeAndMaybeRefute(update);
            }

            foreach (var item in piggybacked)
            {
                await DeliverPiggybackedItemAsync(item, cancellationToken).ConfigureAwait(false);
            }
        }

        private void MergeAndMaybeRefute(SwimMembershipUpdate update)
        {
            if (string.Equals(update.Host, _selfHost, StringComparison.OrdinalIgnoreCase))
            {
                // Someone else is reporting suspicion or death about this very node -- refute it by
                // gossiping a fresh Alive update at a strictly higher incarnation than whatever claim
                // triggered this, exactly like memberlist's own self-refutation.
                lock (_incarnationLock)
                {
                    if (update.State != SwimMemberState.Alive && update.Incarnation >= _selfIncarnation)
                    {
                        _selfIncarnation = Math.Max(_selfIncarnation, update.Incarnation) + 1;
                        _membershipBroadcastQueue.Enqueue(new SwimMembershipUpdate(_selfHost, SwimMemberState.Alive, _selfIncarnation));
                        Log.SelfRefuted(_logger, _selfIncarnation);
                    }
                }

                return;
            }

            if (_membershipTable.Merge(update))
            {
                // Genuinely new information about a peer -- keep spreading it (infection-style).
                _membershipBroadcastQueue.Enqueue(update);
            }
        }

        private async Task DeliverPiggybackedItemAsync(SwimDatagram item, CancellationToken cancellationToken)
        {
            switch (item.Kind)
            {
                case SwimMessageKind.GossipFanOut:
                    await HandleFanOutDeliveryAsync(item.PayloadJson, cancellationToken).ConfigureAwait(false);
                    break;
                case SwimMessageKind.GossipSubscriptionEvent:
                    await HandleSubscriptionEventDeliveryAsync(item.PayloadJson, cancellationToken).ConfigureAwait(false);
                    break;
                case SwimMessageKind.GossipByteFanOut:
                    await HandleByteFanOutDeliveryAsync(item.PayloadJson, cancellationToken).ConfigureAwait(false);
                    break;
                default:
                    Log.UnexpectedPiggybackKind(_logger, item.Kind.ToString());
                    break;
            }
        }

        private static partial class Log
        {
            [LoggerMessage(EventId = 91670, Level = LogLevel.Error,
                Message = "[Cluster] SWIM gossip-round loop faulted; no further gossip rounds will run.")]
            public static partial void GossipRoundLoopFaulted(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91671, Level = LogLevel.Warning,
                Message = "[Cluster] SWIM gossip send to peer '{Host}' failed.")]
            public static partial void GossipSendToPeerFailed(ILogger logger, Exception exception, string host);

            [LoggerMessage(EventId = 91672, Level = LogLevel.Error,
                Message = "[Cluster] SWIM probe loop faulted; no further failure-detection rounds will run.")]
            public static partial void ProbeLoopFaulted(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91673, Level = LogLevel.Warning,
                Message = "[Cluster] SWIM could not resolve an address for probe target '{Host}'.")]
            public static partial void ProbeAddressResolutionFailed(ILogger logger, Exception exception, string host);

            [LoggerMessage(EventId = 91674, Level = LogLevel.Debug,
                Message = "[Cluster] SWIM direct probe of '{Host}' succeeded.")]
            public static partial void ProbeSucceededDirect(ILogger logger, string host);

            [LoggerMessage(EventId = 91675, Level = LogLevel.Debug,
                Message = "[Cluster] SWIM indirect probe of '{Host}' succeeded.")]
            public static partial void ProbeSucceededIndirect(ILogger logger, string host);

            [LoggerMessage(EventId = 91676, Level = LogLevel.Warning,
                Message = "[Cluster] SWIM probe of '{Host}' failed (direct and indirect); marking it Suspect.")]
            public static partial void ProbeFailed(ILogger logger, string host);

            [LoggerMessage(EventId = 91677, Level = LogLevel.Warning,
                Message = "[Cluster] SWIM indirect probe request to relay '{Relay}' for target '{Target}' failed to send.")]
            public static partial void IndirectProbeSendFailed(ILogger logger, Exception exception, string relay, string target);

            [LoggerMessage(EventId = 91678, Level = LogLevel.Warning,
                Message = "[Cluster] SWIM relay could not probe target '{Host}' on the requester's behalf.")]
            public static partial void IndirectProbeRelayFailed(ILogger logger, Exception exception, string host);

            [LoggerMessage(EventId = 91679, Level = LogLevel.Information,
                Message = "[Cluster] SWIM member '{Host}' (incarnation {Incarnation}) marked Suspect.")]
            public static partial void MemberSuspected(ILogger logger, string host, long incarnation);

            [LoggerMessage(EventId = 91680, Level = LogLevel.Warning,
                Message = "[Cluster] SWIM member '{Host}' (incarnation {Incarnation}) declared Dead after an unrefuted suspicion timeout.")]
            public static partial void MemberDeclaredDead(ILogger logger, string host, long incarnation);

            [LoggerMessage(EventId = 91681, Level = LogLevel.Information,
                Message = "[Cluster] SWIM self-refuted a Suspect/Dead claim about this node; new incarnation is {Incarnation}.")]
            public static partial void SelfRefuted(ILogger logger, long incarnation);

            [LoggerMessage(EventId = 91682, Level = LogLevel.Warning,
                Message = "[Cluster] SWIM ping could not be parsed; skipping it.")]
            public static partial void PingUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91683, Level = LogLevel.Warning,
                Message = "[Cluster] SWIM ping-req could not be parsed; skipping it.")]
            public static partial void PingReqUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91684, Level = LogLevel.Warning,
                Message = "[Cluster] SWIM ack could not be parsed; skipping it.")]
            public static partial void AckUnparseable(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91685, Level = LogLevel.Warning,
                Message = "[Cluster] Sending a SWIM ack failed.")]
            public static partial void AckSendFailed(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91686, Level = LogLevel.Warning,
                Message = "[Cluster] Forwarding a relayed SWIM ack back to the original prober failed.")]
            public static partial void AckForwardFailed(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 91687, Level = LogLevel.Warning,
                Message = "[Cluster] SWIM received a piggybacked item with unexpected kind '{Kind}'; skipping it.")]
            public static partial void UnexpectedPiggybackKind(ILogger logger, string kind);
        }
    }
}
