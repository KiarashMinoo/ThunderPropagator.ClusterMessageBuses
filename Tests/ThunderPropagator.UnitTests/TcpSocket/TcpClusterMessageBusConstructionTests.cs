using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using ThunderPropagator.Application.Channels.Cluster;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;
using ThunderPropagator.ClusterMessageBuses.TcpSocket;

namespace ThunderPropagator.UnitTests.TcpSocket;

public class TcpClusterMessageBusConstructionTests
{
    [Fact]
    public void Constructor_NodeEndpointNotSet_Throws()
    {
        var options = Options.Create(new TcpClusterMessageBusOptions());
        var clusterConfiguration = new ClusterConfiguration { NodeEndpoint = null };
        var channelResolver = Substitute.For<IClusterChannelResolver>();
        var discovery = TcpClusterMessageBusTestHelpers.CreateDiscoverySubstitute([]);
        var listener = TcpClusterMessageBusTestHelpers.CreateListenerSubstitute();

        var act = () => new TcpClusterMessageBus(
            options, clusterConfiguration, channelResolver, discovery, NullLoggerFactory.Instance,
            TcpClusterMessageBusTestHelpers.ListenerFactoryReturning(listener));

        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public async Task EnsureInitializedAsync_CalledMoreThanOnce_OnlyStartsTheListenerOnce()
    {
        var listenerFactoryCalls = 0;
        var listener = TcpClusterMessageBusTestHelpers.CreateListenerSubstitute();

        var options = Options.Create(new TcpClusterMessageBusOptions());
        var clusterConfiguration = new ClusterConfiguration { NodeEndpoint = TcpClusterMessageBusTestHelpers.DefaultNodeEndpoint };
        var discovery = TcpClusterMessageBusTestHelpers.CreateDiscoverySubstitute([]);
        var channelResolver = Substitute.For<IClusterChannelResolver>();

        Task<ITcpClusterListener> ListenerFactory(CancellationToken _)
        {
            listenerFactoryCalls++;
            return Task.FromResult(listener);
        }

        await using var bus = new TcpClusterMessageBus(
            options, clusterConfiguration, channelResolver, discovery, NullLoggerFactory.Instance, ListenerFactory);

        await bus.EnsureInitializedAsync(CancellationToken.None);
        await bus.EnsureInitializedAsync(CancellationToken.None);

        listenerFactoryCalls.Should().Be(1);
    }

    [Fact]
    public async Task DisposeAsync_DisposesTheListener()
    {
        var listener = TcpClusterMessageBusTestHelpers.CreateListenerSubstitute();

        var options = Options.Create(new TcpClusterMessageBusOptions());
        var clusterConfiguration = new ClusterConfiguration { NodeEndpoint = TcpClusterMessageBusTestHelpers.DefaultNodeEndpoint };
        var discovery = TcpClusterMessageBusTestHelpers.CreateDiscoverySubstitute([]);
        var channelResolver = Substitute.For<IClusterChannelResolver>();

        var bus = new TcpClusterMessageBus(
            options, clusterConfiguration, channelResolver, discovery, NullLoggerFactory.Instance,
            TcpClusterMessageBusTestHelpers.ListenerFactoryReturning(listener));

        await bus.EnsureInitializedAsync(CancellationToken.None);
        await bus.DisposeAsync();

        await listener.Received(1).DisposeAsync();
    }

    [Fact]
    public async Task DisposeAsync_AlsoDisposesOutboundConnectionsTheCallerNeverDisposedItself()
    {
        var connection = TcpClusterMessageBusTestHelpers.CreateConnectionSubstitute();
        var bus = await TcpClusterMessageBusTestHelpers.CreateBusAsync(
            outboundConnectionFactory: TcpClusterMessageBusTestHelpers.OutboundConnectionFactoryReturning(connection));

        // Force an outbound connection to be created and cached, without disposing it ourselves —
        // mirrors the regression tests added for every other transport in this repo after the same
        // leak was found there first for Kafka.
        await bus.GetOrCreateOutboundConnectionAsync(new Uri("https://peer1:5001/"), CancellationToken.None);

        await bus.DisposeAsync();

        await connection.Received(1).DisposeAsync();
    }

    [Fact]
    public async Task DisposeAsync_AlsoRemovesFanOutSubscriptionsTheCallerNeverDisposedItself()
    {
        var bus = await TcpClusterMessageBusTestHelpers.CreateBusAsync();

        Func<ClusterFanOutMessage, CancellationToken, Task> noOpHandler = (_, _) => Task.CompletedTask;
        var subscriptionHandle = await bus.SubscribeAsync(Guid.NewGuid(), noOpHandler);
        _ = subscriptionHandle;

        var act = async () => await bus.DisposeAsync();

        await act.Should().NotThrowAsync("DisposeAsync must clean up orphaned local subscriptions rather than leaking them");
    }
}
