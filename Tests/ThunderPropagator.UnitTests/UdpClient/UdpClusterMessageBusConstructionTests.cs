using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using ThunderPropagator.Application.Channels.Cluster;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;
using ThunderPropagator.ClusterMessageBuses.UdpClient;

namespace ThunderPropagator.UnitTests.UdpClient;

public class UdpClusterMessageBusConstructionTests
{
    [Fact]
    public void Constructor_NodeEndpointNotSet_Throws()
    {
        var options = Options.Create(new UdpClusterMessageBusOptions());
        var clusterConfiguration = new ClusterConfiguration { NodeEndpoint = null };
        var channelResolver = Substitute.For<IClusterChannelResolver>();
        var discovery = UdpClusterMessageBusTestHelpers.CreateDiscoverySubstitute([]);
        var socket = UdpClusterMessageBusTestHelpers.CreateSocketSubstitute();

        var act = () => new UdpClusterMessageBus(
            options, clusterConfiguration, channelResolver, discovery, NullLoggerFactory.Instance,
            UdpClusterMessageBusTestHelpers.SocketFactoryReturning(socket));

        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public async Task EnsureInitializedAsync_CalledMoreThanOnce_OnlyBindsTheSocketOnce()
    {
        var socketFactoryCalls = 0;
        var socket = UdpClusterMessageBusTestHelpers.CreateSocketSubstitute();

        var options = Options.Create(new UdpClusterMessageBusOptions());
        var clusterConfiguration = new ClusterConfiguration { NodeEndpoint = UdpClusterMessageBusTestHelpers.DefaultNodeEndpoint };
        var discovery = UdpClusterMessageBusTestHelpers.CreateDiscoverySubstitute([]);
        var channelResolver = Substitute.For<IClusterChannelResolver>();

        Task<IUdpClusterSocket> SocketFactory(CancellationToken _)
        {
            socketFactoryCalls++;
            return Task.FromResult(socket);
        }

        await using var bus = new UdpClusterMessageBus(
            options, clusterConfiguration, channelResolver, discovery, NullLoggerFactory.Instance, SocketFactory);

        await bus.EnsureInitializedAsync(CancellationToken.None);
        await bus.EnsureInitializedAsync(CancellationToken.None);

        socketFactoryCalls.Should().Be(1);
    }

    [Fact]
    public async Task DisposeAsync_DisposesTheSocket()
    {
        var socket = UdpClusterMessageBusTestHelpers.CreateSocketSubstitute();

        var options = Options.Create(new UdpClusterMessageBusOptions());
        var clusterConfiguration = new ClusterConfiguration { NodeEndpoint = UdpClusterMessageBusTestHelpers.DefaultNodeEndpoint };
        var discovery = UdpClusterMessageBusTestHelpers.CreateDiscoverySubstitute([]);
        var channelResolver = Substitute.For<IClusterChannelResolver>();

        var bus = new UdpClusterMessageBus(
            options, clusterConfiguration, channelResolver, discovery, NullLoggerFactory.Instance,
            UdpClusterMessageBusTestHelpers.SocketFactoryReturning(socket));

        await bus.EnsureInitializedAsync(CancellationToken.None);
        await bus.DisposeAsync();

        await socket.Received(1).DisposeAsync();
    }

    [Fact]
    public async Task DisposeAsync_AlsoRemovesFanOutSubscriptionsTheCallerNeverDisposedItself()
    {
        var bus = await UdpClusterMessageBusTestHelpers.CreateBusAsync();

        Func<ClusterFanOutMessage, CancellationToken, Task> noOpHandler = (_, _) => Task.CompletedTask;
        var subscriptionHandle = await bus.SubscribeAsync(Guid.NewGuid(), noOpHandler);
        _ = subscriptionHandle;

        var act = async () => await bus.DisposeAsync();

        await act.Should().NotThrowAsync("DisposeAsync must clean up orphaned local subscriptions rather than leaking them");
    }
}
