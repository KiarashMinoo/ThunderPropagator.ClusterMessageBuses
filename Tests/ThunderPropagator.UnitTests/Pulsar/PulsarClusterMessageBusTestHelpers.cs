using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using ThunderPropagator.Application.Channels.Cluster;
using ThunderPropagator.ClusterMessageBuses.Pulsar;

using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.UnitTests.Pulsar;

/// <summary>
/// Shared construction helpers for <see cref="PulsarClusterMessageBus"/> tests. Because all real
/// <c>DotPulsar</c> client usage is isolated behind <see cref="IPulsarClusterTransport"/> (see
/// <see cref="PulsarClusterTransport"/>), every test substitutes that narrow interface directly with
/// NSubstitute — there is no need to fake any concrete <c>DotPulsar</c> type.
/// </summary>
internal static class PulsarClusterMessageBusTestHelpers
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
    /// <see cref="OperationCanceledException"/> — mirroring how a real Pulsar consumer's message
    /// stream behaves when its consumer is torn down.
    /// </summary>
    internal static async IAsyncEnumerable<string> SinglePayloadThenBlock(string payload, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        yield return payload;
        await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Builds a substitute <see cref="IPulsarClusterTransport"/> whose <c>SubscribeAsync</c> returns
    /// an empty, immediately-completing stream for every topic/subscription by default — safe for
    /// the request/reply listener loops <see cref="PulsarClusterMessageBus.EnsureInitializedAsync"/>
    /// starts eagerly, since tests that need to control that stream's contents override the setup
    /// themselves afterward.
    /// </summary>
    internal static IPulsarClusterTransport CreateSubstituteTransport()
    {
        var transport = Substitute.For<IPulsarClusterTransport>();
        transport.SubscribeAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(callInfo => EmptyPayloads((CancellationToken)callInfo[2]));

        return transport;
    }

    internal static async Task<PulsarClusterMessageBus> CreateBusAsync(
        IPulsarClusterTransport? transport = null,
        IClusterChannelResolver? channelResolver = null,
        Uri? nodeEndpoint = null,
        TimeSpan? requestTimeout = null)
    {
        transport ??= CreateSubstituteTransport();
        channelResolver ??= Substitute.For<IClusterChannelResolver>();

        var options = Options.Create(new PulsarClusterMessageBusOptions
        {
            ServiceUrl = "pulsar://localhost:6650",
            RequestTimeout = requestTimeout ?? TimeSpan.FromSeconds(30),
        });

        var clusterConfiguration = new ClusterConfiguration { NodeEndpoint = nodeEndpoint ?? DefaultNodeEndpoint };

        var bus = new PulsarClusterMessageBus(options, clusterConfiguration, channelResolver, NullLoggerFactory.Instance, transport);

        await bus.EnsureInitializedAsync(CancellationToken.None);

        return bus;
    }
}
