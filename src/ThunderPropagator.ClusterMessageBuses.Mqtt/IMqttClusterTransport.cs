namespace ThunderPropagator.ClusterMessageBuses.Mqtt
{
    /// <summary>
    /// Narrow seam over the real <c>MQTTnet</c> client, exposing only the two primitives
    /// <see cref="MqttClusterMessageBus"/> needs: publish a string payload to a topic, and consume a
    /// topic as an async stream of string payloads. <see cref="MqttClusterTransport"/> is the single
    /// production implementation wrapping <c>MQTTnet</c>'s <c>IMqttClient</c>; every unit test
    /// substitutes this interface directly with NSubstitute instead of touching <c>MQTTnet</c>'s own
    /// concrete/event-based API, mirroring the same defensive isolation used for
    /// <c>INatsClusterTransport</c> and <c>IPulsarClusterTransport</c>.
    /// </summary>
    internal interface IMqttClusterTransport : IAsyncDisposable
    {
        ValueTask PublishAsync(string topic, string payload, CancellationToken cancellationToken);

        IAsyncEnumerable<string> SubscribeAsync(string topic, CancellationToken cancellationToken);
    }
}
