using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using ThunderPropagator.Application.Channels.Cluster;
using ThunderPropagator.Application.Channels.Cluster.Discovery;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;
using ThunderPropagator.ClusterMessageBuses.ZeroMQ;

namespace ThunderPropagator.UnitTests.ZeroMQ;

public class ZeroMqClusterMessageBusConstructionTests
{
    [Fact]
    public void Constructor_NodeEndpointNotSet_Throws()
    {
        var options = Options.Create(new ClusterZeroMqOptions());
        var clusterConfiguration = new ClusterConfiguration { NodeEndpoint = null };
        var channelResolver = Substitute.For<IClusterChannelResolver>();
        var discovery = Substitute.For<IClusterNodeDiscovery>();

        var act = () => new ZeroMqClusterMessageBus(options, clusterConfiguration, channelResolver, discovery, NullLoggerFactory.Instance);

        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public async Task EnsureInitializedAsync_StartsTheEmbeddedHost()
    {
        var host = ZeroMqClusterMessageBusTestHelpers.CreateHostSubstitute();
        await using var bus = await ZeroMqClusterMessageBusTestHelpers.CreateBusAsync(hostFactory: _ => Task.FromResult(host));

        await host.Received(1).StartAsync(Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task EnsureInitializedAsync_CalledMoreThanOnce_OnlyStartsTheHostOnce()
    {
        var host = ZeroMqClusterMessageBusTestHelpers.CreateHostSubstitute();
        var bus = await ZeroMqClusterMessageBusTestHelpers.CreateBusAsync(hostFactory: _ => Task.FromResult(host));

        await bus.EnsureInitializedAsync(CancellationToken.None);
        await bus.EnsureInitializedAsync(CancellationToken.None);

        await host.Received(1).StartAsync(Arg.Any<CancellationToken>());
        await bus.DisposeAsync();
    }

    [Fact]
    public async Task DisposeAsync_DisposesTheHost()
    {
        var host = ZeroMqClusterMessageBusTestHelpers.CreateHostSubstitute();
        var bus = await ZeroMqClusterMessageBusTestHelpers.CreateBusAsync(hostFactory: _ => Task.FromResult(host));

        await bus.DisposeAsync();

        await host.Received(1).DisposeAsync();
    }

    [Fact]
    public async Task DisposeAsync_AlsoRemovesFanOutSubscriptionsTheCallerNeverDisposedItself()
    {
        var bus = await ZeroMqClusterMessageBusTestHelpers.CreateBusAsync();

        Func<ClusterFanOutMessage, CancellationToken, Task> noOpHandler = (_, _) => Task.CompletedTask;

        // Deliberately NOT disposing this subscription — simulating a host shutdown where the bus
        // is disposed before every individual channel subscription is (mirrors the regression tests
        // added for every other transport in this repo after the same leak was found there first
        // for Kafka).
        _ = await bus.SubscribeAsync(Guid.NewGuid(), noOpHandler);

        var act = async () => await bus.DisposeAsync();

        await act.Should().NotThrowAsync("DisposeAsync must clean up orphaned local subscriptions rather than leaking them");
    }
}
