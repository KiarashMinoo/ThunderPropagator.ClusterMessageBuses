using System.Net;
using System.Net.Sockets;

namespace ThunderPropagator.ClusterMessageBuses.Swim
{
    /// <summary>
    /// Resolves a peer's host -- a discovered peer's <see cref="Uri.Host"/>, or (far more often for
    /// this transport) a <see cref="SwimMembershipTable"/> entry's own host string, since gossip and
    /// SWIM probing both operate purely on host strings once a peer has been seeded into that table
    /// -- down to the <see cref="IPEndPoint"/> this transport actually sends UDP datagrams to.
    /// Not called out explicitly in this project's required file layout, but needed because this
    /// transport's gossip/probe paths only ever have a bare host string on hand, unlike the
    /// leader/peer-pull request/reply path, which still receives a full <see cref="Uri"/> from its
    /// caller. Adapted from the sibling UDP transport project's <c>UdpAddressing</c>, which resolves
    /// from a <see cref="Uri"/> directly since every one of its send paths already has one.
    /// </summary>
    internal static class SwimAddressing
    {
        /// <summary>
        /// Resolves <paramref name="host"/> to an <see cref="IPAddress"/> (parsing it directly if
        /// it's already a literal IP, otherwise via DNS) and pairs it with <paramref name="port"/> --
        /// every node in the cluster is assumed to listen on the same UDP port, matching every other
        /// transport's "port is fixed, host varies per peer" design.
        /// </summary>
        internal static async Task<IPEndPoint> ResolveEndpointAsync(string host, int port, CancellationToken cancellationToken)
        {
            if (IPAddress.TryParse(host, out var literalAddress))
                return new IPEndPoint(literalAddress, port);

            var addresses = await Dns.GetHostAddressesAsync(host, cancellationToken).ConfigureAwait(false);
            var address = addresses.FirstOrDefault(a => a.AddressFamily is AddressFamily.InterNetwork or AddressFamily.InterNetworkV6)
                ?? throw new InvalidOperationException($"Could not resolve host '{host}' to an IP address for SWIM cluster messaging.");

            return new IPEndPoint(address, port);
        }
    }
}
