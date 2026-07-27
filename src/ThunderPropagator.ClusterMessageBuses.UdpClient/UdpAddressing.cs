using System.Net;
using System.Net.Sockets;

namespace ThunderPropagator.ClusterMessageBuses.UdpClient
{
    /// <summary>
    /// Resolves a peer's discovered <see cref="Uri"/> (an http/https identity, same as every other
    /// transport in this repo reuses from <see cref="ThunderPropagator.Application.Channels.Cluster.Discovery.IClusterNodeDiscovery"/>)
    /// down to the <see cref="IPEndPoint"/> this transport actually sends UDP datagrams to.
    /// </summary>
    internal static class UdpAddressing
    {
        /// <summary>
        /// Resolves <paramref name="peerEndpoint"/>'s host to an <see cref="IPAddress"/> (parsing it
        /// directly if it's already a literal IP, otherwise via DNS) and pairs it with
        /// <paramref name="port"/> — every node in the cluster is assumed to listen on the same UDP
        /// port, matching the TCP/WebSocket transports' "port is fixed, host varies per peer" design.
        /// </summary>
        internal static async Task<IPEndPoint> ResolveEndpointAsync(Uri peerEndpoint, int port, CancellationToken cancellationToken)
        {
            if (IPAddress.TryParse(peerEndpoint.Host, out var literalAddress))
                return new IPEndPoint(literalAddress, port);

            var addresses = await Dns.GetHostAddressesAsync(peerEndpoint.Host, cancellationToken).ConfigureAwait(false);
            var address = addresses.FirstOrDefault(a => a.AddressFamily is AddressFamily.InterNetwork or AddressFamily.InterNetworkV6)
                ?? throw new InvalidOperationException($"Could not resolve host '{peerEndpoint.Host}' to an IP address for UDP cluster messaging.");

            return new IPEndPoint(address, port);
        }
    }
}
