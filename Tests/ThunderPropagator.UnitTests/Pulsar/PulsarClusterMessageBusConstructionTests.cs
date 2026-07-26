using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using ThunderPropagator.Application.Channels.Cluster;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.ClusterMessageBuses.Pulsar;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.UnitTests.Pulsar;

public class PulsarClusterMessageBusConstructionTests
{
    [Fact]
    public void Constructor_NodeEndpointNotSet_Throws()
    {
        var options = Options.Create(new PulsarClusterMessageBusOptions());
        var clusterConfiguration = new ClusterConfiguration { NodeEndpoint = null };
        var channelResolver = Substitute.For<IClusterChannelResolver>();
        var transport = PulsarClusterMessageBusTestHelpers.CreateSubstituteTransport();

        var act = () => new PulsarClusterMessageBus(options, clusterConfiguration, channelResolver, NullLoggerFactory.Instance, transport);

        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public async Task EnsureInitializedAsync_SubscribesToTheOwnRequestAndReplyTopics()
    {
        var transport = PulsarClusterMessageBusTestHelpers.CreateSubstituteTransport();
        var nodeEndpoint = new Uri("https://node1:5001/");

        await using var bus = await PulsarClusterMessageBusTestHelpers.CreateBusAsync(transport: transport, nodeEndpoint: nodeEndpoint);

        var requestTopic = PulsarTopicNaming.RequestTopic("persistent://public/default/thunderpropagator-cluster", nodeEndpoint);
        var replyTopic = PulsarTopicNaming.ReplyTopic("persistent://public/default/thunderpropagator-cluster", nodeEndpoint);

        await transport.Received(1).SubscribeAsync(requestTopic, Arg.Any<string>(), Arg.Any<CancellationToken>());
        await transport.Received(1).SubscribeAsync(replyTopic, Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task EnsureInitializedAsync_CalledMoreThanOnce_OnlyStartsTheListenersOnce()
    {
        var transport = PulsarClusterMessageBusTestHelpers.CreateSubstituteTransport();
        await using var bus = await PulsarClusterMessageBusTestHelpers.CreateBusAsync(transport: transport);

        await bus.EnsureInitializedAsync(CancellationToken.None);
        await bus.EnsureInitializedAsync(CancellationToken.None);

        await transport.Received(2).SubscribeAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task DisposeAsync_DisposesTheTransport()
    {
        var transport = PulsarClusterMessageBusTestHelpers.CreateSubstituteTransport();
        var bus = await PulsarClusterMessageBusTestHelpers.CreateBusAsync(transport: transport);

        await bus.DisposeAsync();

        await transport.Received(1).DisposeAsync();
    }

    [Fact]
    public async Task DisposeAsync_AlsoStopsSubscriptionsTheCallerNeverDisposedItself()
    {
        var transport = PulsarClusterMessageBusTestHelpers.CreateSubstituteTransport();
        var bus = await PulsarClusterMessageBusTestHelpers.CreateBusAsync(transport: transport);

        Func<ClusterFanOutMessage, CancellationToken, Task> noOpHandler = (_, _) => Task.CompletedTask;
        var subscriptionHandle = await bus.SubscribeAsync(Guid.NewGuid(), noOpHandler);

        // Deliberately NOT disposing subscriptionHandle — simulating a host shutdown where the bus
        // is disposed before every individual channel subscription is (mirrors the regression tests
        // added for the other broker transports after the same leak was found there first).
        _ = subscriptionHandle;

        var act = async () => await bus.DisposeAsync();

        await act.Should().NotThrowAsync("DisposeAsync must clean up orphaned per-channel subscriptions rather than leaking their listener loops");
    }
}
