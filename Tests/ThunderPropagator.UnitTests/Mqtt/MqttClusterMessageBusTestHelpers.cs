using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using ThunderPropagator.Application.Channels.Cluster;
using ThunderPropagator.ClusterMessageBuses.Mqtt;

using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.UnitTests.Mqtt;

/// <summary>
/// Shared construction helpers for <see cref="MqttClusterMessageBus"/> tests. Because all real
/// <c>MQTTnet</c> client usage is isolated behind <see cref="IMqttClusterTransport"/> (see
/// <see cref="MqttClusterTransport"/>), every test substitutes that narrow interface directly with
/// NSubstitute — there is no need to fake any concrete/event-based <c>MQTTnet</c> type.
/// </summary>
internal static class MqttClusterMessageBusTestHelpers
{
    internal static readonly Uri DefaultNodeEndpoint = new("https://node1:5000/");

    /// <summary>An async-enumerable that completes immediately without yielding anything.</summary>
    internal static async IAsyncEnumerable<string> EmptyPayloads([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await Task.CompletedTask;
        yield break;
    }

    /// <summary>
    /// An async-enumerable that yields exactly one payload, then blocks (like a real subscription
    /// would) until <paramref name="cancellationToken"/> is cancelled, at which point it throws
    /// <see cref="OperationCanceledException"/> — mirroring how a real MQTT topic subscription's
    /// message stream behaves when it is torn down.
    /// </summary>
    internal static async IAsyncEnumerable<string> SinglePayloadThenBlock(string payload, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        yield return payload;
        await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Builds a substitute <see cref="IMqttClusterTransport"/> whose <c>SubscribeAsync</c> returns an
    /// empty, immediately-completing stream for every topic by default — safe for the request/reply
    /// listener loops <see cref="MqttClusterMessageBus.EnsureInitializedAsync"/> starts eagerly, since
    /// tests that need to control that stream's contents override the setup themselves afterward.
    /// </summary>
    internal static IMqttClusterTransport CreateSubstituteTransport()
    {
        var transport = Substitute.For<IMqttClusterTransport>();
        transport.SubscribeAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(callInfo => EmptyPayloads((CancellationToken)callInfo[1]));

        return transport;
    }

    internal static async Task<MqttClusterMessageBus> CreateBusAsync(
        IMqttClusterTransport? transport = null,
        IClusterChannelResolver? channelResolver = null,
        Uri? nodeEndpoint = null,
        TimeSpan? requestTimeout = null)
    {
        transport ??= CreateSubstituteTransport();
        channelResolver ??= Substitute.For<IClusterChannelResolver>();

        var options = Options.Create(new MqttClusterMessageBusOptions
        {
            Host = "localhost",
            Port = 1883,
            RequestTimeout = requestTimeout ?? TimeSpan.FromSeconds(30),
        });

        var clusterConfiguration = new ClusterConfiguration { NodeEndpoint = nodeEndpoint ?? DefaultNodeEndpoint };

        var bus = new MqttClusterMessageBus(options, clusterConfiguration, channelResolver, NullLoggerFactory.Instance, transport);

        await bus.EnsureInitializedAsync(CancellationToken.None);

        return bus;
    }
}
