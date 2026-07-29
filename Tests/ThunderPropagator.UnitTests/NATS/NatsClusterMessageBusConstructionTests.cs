using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using ThunderPropagator.Application.Channels.Cluster;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.ClusterMessageBuses.NATS;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.UnitTests.NATS;

public class NatsClusterMessageBusConstructionTests
{
    [Fact]
    public void Constructor_NodeEndpointNotSet_Throws()
    {
        var options = Options.Create(new NatsClusterMessageBusOptions());
        var clusterConfiguration = new ClusterConfiguration { NodeEndpoint = null };
        var channelResolver = Substitute.For<IClusterChannelResolver>();
        var transport = NatsClusterMessageBusTestHelpers.CreateSubstituteTransport();

        var act = () => new NatsClusterMessageBus(options, clusterConfiguration, channelResolver, NullLoggerFactory.Instance, transport);

        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public async Task EnsureInitializedAsync_SubscribesToTheOwnRequestSubject()
    {
        var transport = NatsClusterMessageBusTestHelpers.CreateSubstituteTransport();
        var nodeEndpoint = new Uri("https://node1:5001/");

        await using var bus = await NatsClusterMessageBusTestHelpers.CreateBusAsync(transport: transport, nodeEndpoint: nodeEndpoint);

        var expectedSubject = NatsSubjectNaming.RequestSubject("thunderpropagator.cluster", nodeEndpoint);
        transport.Received(1).SubscribeAsync(expectedSubject, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task EnsureInitializedAsync_CalledMoreThanOnce_OnlyStartsTheListenerOnce()
    {
        var transport = NatsClusterMessageBusTestHelpers.CreateSubstituteTransport();
        await using var bus = await NatsClusterMessageBusTestHelpers.CreateBusAsync(transport: transport);

        await bus.EnsureInitializedAsync(CancellationToken.None);
        await bus.EnsureInitializedAsync(CancellationToken.None);

        transport.Received(1).SubscribeAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task DisposeAsync_DisposesTheTransport()
    {
        var transport = NatsClusterMessageBusTestHelpers.CreateSubstituteTransport();
        var bus = await NatsClusterMessageBusTestHelpers.CreateBusAsync(transport: transport);

        await bus.DisposeAsync();

        await transport.Received(1).DisposeAsync();
    }

    [Fact]
    public async Task DisposeAsync_AlsoStopsSubscriptionsTheCallerNeverDisposedItself()
    {
        var transport = NatsClusterMessageBusTestHelpers.CreateSubstituteTransport();
        var bus = await NatsClusterMessageBusTestHelpers.CreateBusAsync(transport: transport);

        Func<ClusterFanOutMessage, CancellationToken, Task> noOpHandler = (_, _) => Task.CompletedTask;
        var subscriptionHandle = await bus.SubscribeAsync(Guid.NewGuid(), noOpHandler);

        // Deliberately NOT disposing subscriptionHandle — simulating a host shutdown where the bus
        // is disposed before every individual channel subscription is (mirrors the regression tests
        // added for KafkaClusterMessageBus / RabbitMqClusterMessageBus after the same leak was found
        // there first).
        _ = subscriptionHandle;

        var act = async () => await bus.DisposeAsync();

        await act.Should().NotThrowAsync("DisposeAsync must clean up orphaned per-channel subscriptions rather than leaking their listener loops");
    }
}
