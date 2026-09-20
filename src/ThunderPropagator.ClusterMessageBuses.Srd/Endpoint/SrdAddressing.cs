using System.Net;
using System.Net.Sockets;

namespace ThunderPropagator.ClusterMessageBuses.Srd
{
    /// <summary>
    /// Produces the raw address bytes handed to <c>fi_av_insert</c> for a peer discovered via
    /// <see cref="ThunderPropagator.Application.Channels.Cluster.Discovery.IClusterNodeDiscovery"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>This is a placeholder, and is almost certainly NOT a valid raw EFA address.</b> Every other
    /// transport in this repo can turn a peer's discovered <see cref="Uri"/> straight into the address
    /// it actually sends to (an <see cref="IPEndPoint"/>, a connection string, a topic name — see
    /// <c>UdpAddressing.ResolveEndpointAsync</c> for the closest analogue) because those transports'
    /// addressing schemes are all ultimately IP/DNS-based. EFA is not: a real EFA/libfabric raw address
    /// is an opaque, provider-defined blob (informally, a GID plus queue-pair/QKey-style routing
    /// information) that can only be obtained by calling <c>fi_getname</c> on a <em>live, already-
    /// enabled</em> EFA endpoint — it cannot be derived from a hostname or port number at all. AWS's own
    /// EFA documentation is explicit that EFA has no name service: every real EFA application (MPI,
    /// NCCL's <c>aws-ofi-nccl</c> plugin, libfabric's own <c>fi_pingpong</c> example) exchanges each
    /// peer's <c>fi_getname</c> output through some existing out-of-band channel (MPI ranks, a
    /// rendezvous server, sockets) before any <c>fi_av_insert</c> call can succeed.
    /// </para>
    /// <para>
    /// This project's native-interop scope deliberately does not bind <c>fi_getname</c> (see the
    /// banner atop <c>Native/LibFabricNativeMethods.cs</c>), and
    /// <see cref="ThunderPropagator.Application.Channels.Cluster.Discovery.IClusterNodeDiscovery"/>'s
    /// contract only exposes a peer's <see cref="Uri"/> — there is currently no channel in this repo
    /// for a node to publish/consume the other, EFA-specific raw address bytes a real deployment would
    /// need. Rather than leave <c>fi_av_insert</c> entirely unimplemented, this method fabricates a
    /// structurally-plausible fixed-size buffer (a serialized <see cref="IPEndPoint"/>, sized like a
    /// generic <c>sockaddr</c>) purely so the surrounding send/receive/AV-caching plumbing has
    /// something concrete to exercise end-to-end in code review and structural tests. <b>Treat this as
    /// the single biggest gap in this transport</b>: on real EFA hardware, replace this method with a
    /// real out-of-band exchange of each peer's actual <c>fi_getname</c> bytes before trusting
    /// <c>fi_av_insert</c>/<c>fi_send</c> to do anything meaningful.
    /// </para>
    /// </remarks>
    internal static class SrdAddressing
    {
        /// <summary>
        /// Resolves <paramref name="peerEndpoint"/>'s host to an <see cref="IPAddress"/> (parsing it
        /// directly if it's already a literal IP, otherwise via DNS) and packs it with
        /// <paramref name="port"/> into a placeholder raw-address buffer. See this type's remarks —
        /// the result is NOT a real EFA address.
        /// </summary>
        internal static async Task<byte[]> ResolvePlaceholderRawAddressAsync(Uri peerEndpoint, int port, CancellationToken cancellationToken)
        {
            IPAddress address;
            if (IPAddress.TryParse(peerEndpoint.Host, out var literalAddress))
            {
                address = literalAddress;
            }
            else
            {
                var addresses = await Dns.GetHostAddressesAsync(peerEndpoint.Host, cancellationToken).ConfigureAwait(false);
                address = addresses.FirstOrDefault(a => a.AddressFamily is AddressFamily.InterNetwork or AddressFamily.InterNetworkV6)
                    ?? throw new InvalidOperationException($"Could not resolve host '{peerEndpoint.Host}' for SRD cluster addressing.");
            }

            var endpoint = new IPEndPoint(address, port);
            var socketAddress = endpoint.Serialize();
            var buffer = new byte[socketAddress.Size];
            for (var i = 0; i < socketAddress.Size; i++)
            {
                buffer[i] = socketAddress[i];
            }

            return buffer;
        }
    }
}
