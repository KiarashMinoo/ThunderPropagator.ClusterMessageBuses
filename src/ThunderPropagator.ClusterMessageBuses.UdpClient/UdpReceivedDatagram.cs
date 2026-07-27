using System.Net;

namespace ThunderPropagator.ClusterMessageBuses.UdpClient
{
    /// <summary>
    /// One datagram received off this node's shared UDP socket, paired with the
    /// <see cref="System.Net.IPEndPoint"/> it actually arrived from — captured so the answering side
    /// of a request can send its response straight back to the real sender, without any
    /// self-reported "reply to" address in the payload itself (see
    /// <see cref="UdpClusterRequestEnvelope"/>'s remarks).
    /// </summary>
    internal sealed record UdpReceivedDatagram(byte[] Payload, IPEndPoint RemoteEndPoint);
}
