using NetMQ;

namespace ThunderPropagator.ClusterMessageBuses.ZeroMQ
{
    /// <summary>Applies <see cref="ClusterZeroMqOptions"/> uniformly to every ROUTER/DEALER socket this transport creates.</summary>
    internal static class NetMQSocketConfigurator
    {
        internal static void Apply(NetMQSocket socket, ClusterZeroMqOptions options)
        {
            socket.Options.Linger = options.Linger;
            socket.Options.HeartbeatInterval = options.HeartbeatInterval;

            if (options.SendHighWatermark is { } sendHighWatermark)
                socket.Options.SendHighWatermark = sendHighWatermark;

            if (options.ReceiveHighWatermark is { } receiveHighWatermark)
                socket.Options.ReceiveHighWatermark = receiveHighWatermark;
        }
    }
}
