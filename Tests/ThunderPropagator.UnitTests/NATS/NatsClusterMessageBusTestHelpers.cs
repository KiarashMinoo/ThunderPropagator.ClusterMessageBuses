using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using ThunderPropagator.Application.Channels.Cluster;
using ThunderPropagator.ClusterMessageBuses.NATS;

using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.UnitTests.NATS;

/// <summary>
/// Shared construction helpers for <see cref="NatsClusterMessageBus"/> tests. Because all real
/// <c>NATS.Net</c> client usage is isolated behind <see cref="INatsClusterTransport"/> (see
/// <see cref="NatsClusterTransport"/>), every test substitutes that narrow interface directly with
/// NSubstitute — there is no need to fake any concrete <c>NATS.Net</c> type at all, unlike the
/// Kafka/RabbitMQ transports which substitute the real client library's own interfaces.
/// </summary>
internal static class NatsClusterMessageBusTestHelpers
{
    internal static readonly Uri DefaultNodeEndpoint = new("https://node1:5000/");

    /// <summary>An async-enumerable that completes immediately without yielding anything.</summary>
    internal static async IAsyncEnumerable<NatsClusterDelivery> EmptyDeliveries([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await Task.CompletedTask;
        yield break;
    }

    /// <summary>
    /// An async-enumerable that yields exactly one delivery, then blocks (like a real subscription
    /// would) until <paramref name="cancellationToken"/> is cancelled, at which point it throws
    /// <see cref="OperationCanceledException"/> — mirroring how a real NATS subscription's enumerator
    /// behaves when its subscription is torn down.
    /// </summary>
    internal static async IAsyncEnumerable<NatsClusterDelivery> SingleDeliveryThenBlock(NatsClusterDelivery delivery, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        yield return delivery;
        await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Builds a substitute <see cref="INatsClusterTransport"/> whose <c>SubscribeAsync</c> returns an
    /// empty, immediately-completing stream for every subject by default — safe for the request
    /// listener loop <see cref="NatsClusterMessageBus.EnsureInitializedAsync"/> starts eagerly, since
    /// tests that need to control that stream's contents override the setup themselves afterward.
    /// </summary>
    internal static INatsClusterTransport CreateSubstituteTransport()
    {
        var transport = Substitute.For<INatsClusterTransport>();
        transport.SubscribeAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(callInfo => EmptyDeliveries((CancellationToken)callInfo[1]));

        return transport;
    }

    internal static async Task<NatsClusterMessageBus> CreateBusAsync(
        INatsClusterTransport? transport = null,
        IClusterChannelResolver? channelResolver = null,
        Uri? nodeEndpoint = null,
        TimeSpan? requestTimeout = null)
    {
        transport ??= CreateSubstituteTransport();
        channelResolver ??= Substitute.For<IClusterChannelResolver>();

        var options = Options.Create(new NatsClusterMessageBusOptions
        {
            Url = "nats://localhost:4222",
            RequestTimeout = requestTimeout ?? TimeSpan.FromSeconds(30),
        });

        var clusterConfiguration = new ClusterConfiguration { NodeEndpoint = nodeEndpoint ?? DefaultNodeEndpoint };

        var bus = new NatsClusterMessageBus(options, clusterConfiguration, channelResolver, NullLoggerFactory.Instance, transport);

        await bus.EnsureInitializedAsync(CancellationToken.None);

        return bus;
    }
}
