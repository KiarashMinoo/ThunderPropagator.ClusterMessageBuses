using System.Net;

namespace ThunderPropagator.ClusterMessageBuses.Swim
{
    /// <summary>
    /// One datagram received off this node's shared UDP socket, paired with the
    /// <see cref="System.Net.IPEndPoint"/> it actually arrived from -- captured so the answering side
    /// of a Ping/PingReq/request can send its Ack/response straight back to the real sender, without
    /// any self-reported "reply to" address in the payload itself (see
    /// <c>SharedKernel.ClusterRequestEnvelope</c>'s remarks). Mirrors the sibling UDP transport
    /// project's <c>UdpReceivedDatagram</c> exactly.
    /// </summary>
    internal sealed record SwimReceivedDatagram(byte[] Payload, IPEndPoint RemoteEndPoint);
}
