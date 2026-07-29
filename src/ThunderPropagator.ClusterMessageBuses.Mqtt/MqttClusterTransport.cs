using System.Buffers;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Channels;
using MQTTnet;

namespace ThunderPropagator.ClusterMessageBuses.Mqtt
{
    /// <summary>
    /// Real <see cref="IMqttClusterTransport"/> implementation wrapping a single <c>MQTTnet</c>
    /// <c>IMqttClient</c> connection.
    /// </summary>
    /// <remarks>
    /// <c>MQTTnet</c>'s client is event-based (<c>ApplicationMessageReceivedAsync</c> fires for every
    /// topic the client is subscribed to, not just one), so this class bridges that single shared
    /// event stream into per-topic <see cref="IAsyncEnumerable{T}"/>s by routing each received message
    /// to a <see cref="Channel{T}"/> keyed by topic — the one MQTTnet-specific plumbing detail that
    /// isolates the event-callback shape from <see cref="MqttClusterMessageBus"/>, which only ever
    /// sees the narrow <see cref="IMqttClusterTransport"/> seam.
    /// </remarks>
    internal sealed class MqttClusterTransport : IMqttClusterTransport
    {
        private readonly IMqttClient _client;
        private readonly MqttClientOptions _clientOptions;
        private readonly ConcurrentDictionary<string, Channel<string>> _topicChannels = new(StringComparer.Ordinal);
        private readonly SemaphoreSlim _connectLock = new(1, 1);
        private volatile bool _connected;

        internal MqttClusterTransport(string host, int port, string? clientId)
        {
            var factory = new MqttClientFactory();
            _client = factory.CreateMqttClient();
            _client.ApplicationMessageReceivedAsync += OnApplicationMessageReceivedAsync;

            _clientOptions = new MqttClientOptionsBuilder()
                .WithTcpServer(host, port)
                .WithClientId(clientId ?? $"thunderpropagator-cluster-{Guid.NewGuid():N}")
                .Build();
        }

        private Task OnApplicationMessageReceivedAsync(MqttApplicationMessageReceivedEventArgs args)
        {
            if (_topicChannels.TryGetValue(args.ApplicationMessage.Topic, out var channel))
            {
                var payload = Encoding.UTF8.GetString(args.ApplicationMessage.Payload.ToArray());
                channel.Writer.TryWrite(payload);
            }

            return Task.CompletedTask;
        }

        private async Task EnsureConnectedAsync(CancellationToken cancellationToken)
        {
            if (_connected)
                return;

            await _connectLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                if (_connected)
                    return;

                await _client.ConnectAsync(_clientOptions, cancellationToken).ConfigureAwait(false);
                _connected = true;
            }
            finally
            {
                _connectLock.Release();
            }
        }

        public async ValueTask PublishAsync(string topic, string payload, CancellationToken cancellationToken)
        {
            await EnsureConnectedAsync(cancellationToken).ConfigureAwait(false);

            var message = new MqttApplicationMessageBuilder()
                .WithTopic(topic)
                .WithPayload(payload)
                .Build();

            await _client.PublishAsync(message, cancellationToken).ConfigureAwait(false);
        }

        public async IAsyncEnumerable<string> SubscribeAsync(string topic, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await EnsureConnectedAsync(cancellationToken).ConfigureAwait(false);

            var channel = Channel.CreateUnbounded<string>(new UnboundedChannelOptions { SingleWriter = true, SingleReader = true });
            _topicChannels[topic] = channel;

            try
            {
                var factory = new MqttClientFactory();
                var subscribeOptions = factory.CreateSubscribeOptionsBuilder().WithTopicFilter(topic).Build();
                await _client.SubscribeAsync(subscribeOptions, cancellationToken).ConfigureAwait(false);

                await foreach (var payload in channel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
                {
                    yield return payload;
                }
            }
            finally
            {
                _topicChannels.TryRemove(topic, out _);
            }
        }

        public async ValueTask DisposeAsync()
        {
            foreach (var channel in _topicChannels.Values)
            {
                channel.Writer.TryComplete();
            }

            try
            {
                if (_connected)
                {
                    await _client.DisconnectAsync().ConfigureAwait(false);
                }
            }
            catch
            {
                // Best-effort disconnect during shutdown — the client is disposed regardless.
            }

            _client.Dispose();
            _connectLock.Dispose();
        }
    }
}
