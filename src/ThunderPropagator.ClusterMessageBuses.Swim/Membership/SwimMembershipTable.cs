using System.Collections.Concurrent;

namespace ThunderPropagator.ClusterMessageBuses.Swim
{
    /// <summary>
    /// Thread-safe, <see cref="ConcurrentDictionary{TKey,TValue}"/>-backed tracker of every known
    /// peer's SWIM liveness state (<see cref="SwimMemberState"/>, incarnation, and last state-change
    /// timestamp used for suspicion-timeout tracking). Distinct from
    /// <see cref="ThunderPropagator.Application.Channels.Cluster.Discovery.IClusterNodeDiscovery"/>,
    /// which this table is periodically reconciled against via <see cref="ReconcileWithDiscovery"/>:
    /// discovery is the source of truth for cluster <i>membership intent</i> (which hosts are
    /// supposed to be part of the cluster at all), while this table layers SWIM's own
    /// eventually-consistent <i>liveness</i> tracking on top of that -- a host discovery still lists
    /// can be locally believed <see cref="SwimMemberState.Suspect"/> or even
    /// <see cref="SwimMemberState.Dead"/> here well before (or even without) discovery itself ever
    /// learning that. Gossip fan-out (<see cref="GetRandomAlivePeers"/>) only ever targets peers this
    /// table currently believes <see cref="SwimMemberState.Alive"/>.
    /// </summary>
    internal sealed class SwimMembershipTable
    {
        private readonly string _selfHost;
        private readonly ConcurrentDictionary<string, SwimMemberInfo> _members = new(StringComparer.OrdinalIgnoreCase);

        internal SwimMembershipTable(string selfHost)
        {
            _selfHost = selfHost;
        }

        /// <summary>
        /// Number of members known to this table, including this node itself -- the "N" in the
        /// <c>ceil(RetransmitMult * log10(N+1))</c> retransmit-limit formula every broadcast queue
        /// in this transport uses.
        /// </summary>
        internal int KnownMemberCount => _members.Count + 1;

        /// <summary>
        /// Adds any host in <paramref name="discoveredHosts"/> that this table doesn't know about yet
        /// (seeded as <see cref="SwimMemberState.Alive"/> at incarnation 0), and removes any host
        /// this table has that <paramref name="discoveredHosts"/> no longer lists -- discovery is the
        /// source of truth for whether a host is still part of the cluster's intended membership at
        /// all, regardless of what this table's own gossip-driven liveness state currently says about
        /// it.
        /// </summary>
        internal void ReconcileWithDiscovery(IReadOnlyCollection<string> discoveredHosts)
        {
            foreach (var host in discoveredHosts)
            {
                if (string.Equals(host, _selfHost, StringComparison.OrdinalIgnoreCase))
                    continue;

                _members.TryAdd(host, new SwimMemberInfo(host, SwimMemberState.Alive, 0, DateTime.UtcNow));
            }

            var discoveredSet = new HashSet<string>(discoveredHosts, StringComparer.OrdinalIgnoreCase);
            foreach (var host in _members.Keys)
            {
                if (!discoveredSet.Contains(host))
                    _members.TryRemove(host, out _);
            }
        }

        /// <summary>Current incarnation this table knows for <paramref name="host"/>, or 0 if unknown.</summary>
        internal long GetIncarnation(string host) => _members.TryGetValue(host, out var info) ? info.Incarnation : 0;

        /// <summary>Current state this table knows for <paramref name="host"/>, or <see langword="null"/> if unknown.</summary>
        internal SwimMemberState? GetState(string host) => _members.TryGetValue(host, out var info) ? info.State : null;

        /// <summary>
        /// Applies <paramref name="update"/> using SWIM's standard precedence rule: a strictly higher
        /// incarnation always wins outright; at an equal incarnation only a "more dead" state
        /// (Alive -&gt; Suspect -&gt; Dead) is accepted over the currently-known one; a lower
        /// incarnation, or an equal-incarnation "less dead" state, is stale and ignored. Returns
        /// <see langword="true"/> if the update actually changed this table's recorded state for
        /// that host (i.e. it carries genuinely new information that should keep being gossiped --
        /// infection-style dissemination only needs to keep spreading what's actually new).
        /// </summary>
        internal bool Merge(SwimMembershipUpdate update)
        {
            var changed = false;

            _members.AddOrUpdate(
                update.Host,
                addValueFactory: _ =>
                {
                    changed = true;
                    return new SwimMemberInfo(update.Host, update.State, update.Incarnation, DateTime.UtcNow);
                },
                updateValueFactory: (_, existing) =>
                {
                    if (update.Incarnation > existing.Incarnation ||
                        (update.Incarnation == existing.Incarnation && update.State > existing.State))
                    {
                        changed = true;
                        return new SwimMemberInfo(update.Host, update.State, update.Incarnation, DateTime.UtcNow);
                    }

                    return existing;
                });

            return changed;
        }

        /// <summary>
        /// Returns up to <paramref name="count"/> distinct, currently-<see cref="SwimMemberState.Alive"/>
        /// hosts, chosen at random, excluding <paramref name="exclude"/> if given (e.g. the host
        /// currently being probed, which shouldn't be asked to indirectly probe itself).
        /// </summary>
        internal IReadOnlyList<string> GetRandomAlivePeers(int count, string? exclude = null)
        {
            var alive = _members.Values
                .Where(m => m.State == SwimMemberState.Alive && !string.Equals(m.Host, exclude, StringComparison.OrdinalIgnoreCase))
                .Select(m => m.Host)
                .ToList();

            if (alive.Count <= count)
                return alive;

            // Partial Fisher-Yates shuffle -- avoids both the bias and the extra allocations of an
            // OrderBy(_ => Random) shuffle for what is typically a very small candidate list.
            for (var i = 0; i < count; i++)
            {
                var swapIndex = i + Random.Shared.Next(alive.Count - i);
                (alive[i], alive[swapIndex]) = (alive[swapIndex], alive[i]);
            }

            return alive.Take(count).ToList();
        }

        /// <summary>
        /// Every <see cref="SwimMemberState.Suspect"/> host whose last state change is older than
        /// <paramref name="suspicionTimeout"/> -- the caller (the SWIM probe loop) transitions each
        /// to <see cref="SwimMemberState.Dead"/> and gossips that decision, exactly like
        /// <c>memberlist</c>'s own suspicion-timeout handling.
        /// </summary>
        internal IReadOnlyList<string> GetExpiredSuspects(TimeSpan suspicionTimeout)
        {
            var now = DateTime.UtcNow;
            return _members.Values
                .Where(m => m.State == SwimMemberState.Suspect && now - m.LastStateChangeUtc >= suspicionTimeout)
                .Select(m => m.Host)
                .ToList();
        }
    }

    /// <summary>One member's tracked liveness state inside a <see cref="SwimMembershipTable"/>.</summary>
    internal sealed record SwimMemberInfo(string Host, SwimMemberState State, long Incarnation, DateTime LastStateChangeUtc);
}
