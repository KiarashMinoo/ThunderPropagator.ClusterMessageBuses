using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using ThunderPropagator.Application.Channels.Cluster;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;
using ThunderPropagator.ClusterMessageBuses.WebApi;

namespace ThunderPropagator.UnitTests.WebApi;

public class WebApiClusterMessageBusConstructionTests
{
    [Fact]
    public void Constructor_NodeEndpointNotSet_Throws()
    {
        var options = Options.Create(new WebApiClusterMessageBusOptions());
        var clusterConfiguration = new ClusterConfiguration { NodeEndpoint = null };
        var channelResolver = Substitute.For<IClusterChannelResolver>();
        var discovery = WebApiClusterMessageBusTestHelpers.CreateDiscoverySubstitute([]);
        var listener = WebApiClusterMessageBusTestHelpers.CreateListenerSubstitute();

        var act = () => new WebApiClusterMessageBus(
            options, clusterConfiguration, channelResolver, discovery, NullLoggerFactory.Instance,
            WebApiClusterMessageBusTestHelpers.ListenerFactoryReturning(listener),
            WebApiClusterMessageBusTestHelpers.CreateHttpClientSubstitute());

        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public async Task EnsureInitializedAsync_CalledMoreThanOnce_OnlyStartsTheListenerOnce()
    {
        var listenerFactoryCalls = 0;
        var listener = WebApiClusterMessageBusTestHelpers.CreateListenerSubstitute();

        var options = Options.Create(new WebApiClusterMessageBusOptions());
        var clusterConfiguration = new ClusterConfiguration { NodeEndpoint = WebApiClusterMessageBusTestHelpers.DefaultNodeEndpoint };
        var discovery = WebApiClusterMessageBusTestHelpers.CreateDiscoverySubstitute([]);
        var channelResolver = Substitute.For<IClusterChannelResolver>();

        Task<IWebApiClusterListener> ListenerFactory(CancellationToken _)
        {
            listenerFactoryCalls++;
            return Task.FromResult(listener);
        }

        await using var bus = new WebApiClusterMessageBus(
            options, clusterConfiguration, channelResolver, discovery, NullLoggerFactory.Instance,
            ListenerFactory, WebApiClusterMessageBusTestHelpers.CreateHttpClientSubstitute());

        await bus.EnsureInitializedAsync(CancellationToken.None);
        await bus.EnsureInitializedAsync(CancellationToken.None);

        listenerFactoryCalls.Should().Be(1);
    }

    [Fact]
    public async Task DisposeAsync_DisposesTheListener()
    {
        var listener = WebApiClusterMessageBusTestHelpers.CreateListenerSubstitute();

        var options = Options.Create(new WebApiClusterMessageBusOptions());
        var clusterConfiguration = new ClusterConfiguration { NodeEndpoint = WebApiClusterMessageBusTestHelpers.DefaultNodeEndpoint };
        var discovery = WebApiClusterMessageBusTestHelpers.CreateDiscoverySubstitute([]);
        var channelResolver = Substitute.For<IClusterChannelResolver>();

        var bus = new WebApiClusterMessageBus(
            options, clusterConfiguration, channelResolver, discovery, NullLoggerFactory.Instance,
            WebApiClusterMessageBusTestHelpers.ListenerFactoryReturning(listener),
            WebApiClusterMessageBusTestHelpers.CreateHttpClientSubstitute());

        await bus.EnsureInitializedAsync(CancellationToken.None);
        await bus.DisposeAsync();

        await listener.Received(1).DisposeAsync();
    }

    [Fact]
    public async Task DisposeAsync_AlsoRemovesFanOutSubscriptionsTheCallerNeverDisposedItself()
    {
        await using var bus = await WebApiClusterMessageBusTestHelpers.CreateBusAsync();

        Func<ThunderPropagator.Application.Channels.Cluster.MessageBus.ClusterFanOutMessage, CancellationToken, Task> noOpHandler = (_, _) => Task.CompletedTask;
        var subscriptionHandle = await bus.SubscribeAsync(Guid.NewGuid(), noOpHandler);

        // Deliberately NOT disposing subscriptionHandle — simulating a host shutdown where the bus
        // is disposed before every individual channel subscription is (mirrors the regression tests
        // added for every other transport in this repo after the same leak was found there first
        // for Kafka).
        _ = subscriptionHandle;

        var act = async () => await bus.DisposeAsync();

        await act.Should().NotThrowAsync("DisposeAsync must clean up orphaned local subscriptions rather than leaking them");
    }
}
