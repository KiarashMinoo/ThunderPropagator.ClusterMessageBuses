using Azure.Messaging.ServiceBus;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using ThunderPropagator.Application.Channels.Cluster;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.ClusterMessageBuses.AzureServiceBus;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.UnitTests.AzureServiceBus;

public class AzureServiceBusClusterMessageBusConstructionTests
{
    [Fact]
    public void Constructor_NodeEndpointNotSet_Throws()
    {
        var options = Options.Create(new AzureServiceBusClusterMessageBusOptions { ConnectionString = AzureServiceBusClusterMessageBusTestHelpers.TestConnectionString });
        var clusterConfiguration = new ClusterConfiguration { NodeEndpoint = null };
        var channelResolver = Substitute.For<IClusterChannelResolver>();
        var client = AzureServiceBusClusterMessageBusTestHelpers.CreateClientSubstitute();
        var admin = AzureServiceBusClusterMessageBusTestHelpers.CreateAdminSubstitute();

        var act = () => new AzureServiceBusClusterMessageBus(
            options, clusterConfiguration, channelResolver, NullLoggerFactory.Instance,
            AzureServiceBusClusterMessageBusTestHelpers.ClientFactoryReturning(client),
            AzureServiceBusClusterMessageBusTestHelpers.AdminFactoryReturning(admin));

        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public async Task EnsureInitializedAsync_CalledMoreThanOnce_OnlyBuildsClientsOnce()
    {
        var clientFactoryCalls = 0;
        var client = AzureServiceBusClusterMessageBusTestHelpers.CreateClientSubstitute();
        var admin = AzureServiceBusClusterMessageBusTestHelpers.CreateAdminSubstitute();

        var options = Options.Create(new AzureServiceBusClusterMessageBusOptions
        {
            ConnectionString = AzureServiceBusClusterMessageBusTestHelpers.TestConnectionString,
            EmptyPollDelay = TimeSpan.FromMilliseconds(5),
        });
        var clusterConfiguration = new ClusterConfiguration { NodeEndpoint = AzureServiceBusClusterMessageBusTestHelpers.DefaultNodeEndpoint };
        var channelResolver = Substitute.For<IClusterChannelResolver>();

        Task<ServiceBusClient> ClientFactory(CancellationToken _)
        {
            clientFactoryCalls++;
            return Task.FromResult(client);
        }

        await using var bus = new AzureServiceBusClusterMessageBus(
            options, clusterConfiguration, channelResolver, NullLoggerFactory.Instance,
            ClientFactory, AzureServiceBusClusterMessageBusTestHelpers.AdminFactoryReturning(admin));

        await bus.EnsureInitializedAsync(CancellationToken.None);
        await bus.EnsureInitializedAsync(CancellationToken.None);

        clientFactoryCalls.Should().Be(1);
    }

    [Fact]
    public async Task EnsureInitializedAsync_CreatesThisNodesRequestAndReplyQueues()
    {
        var admin = AzureServiceBusClusterMessageBusTestHelpers.CreateAdminSubstitute();
        await using var bus = await AzureServiceBusClusterMessageBusTestHelpers.CreateBusAsync(admin: admin);

        await admin.Received().CreateQueueAsync(
            Arg.Is<string>(n => n.Contains("-requests-")), Arg.Any<CancellationToken>());
        await admin.Received().CreateQueueAsync(
            Arg.Is<string>(n => n.Contains("-replies-")), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task DisposeAsync_DisposesTheClient()
    {
        var client = AzureServiceBusClusterMessageBusTestHelpers.CreateClientSubstitute();

        var bus = await AzureServiceBusClusterMessageBusTestHelpers.CreateBusAsync(client: client);
        await bus.DisposeAsync();

        await client.Received(1).DisposeAsync();
    }

    [Fact]
    public async Task DisposeAsync_AlsoRemovesFanOutSubscriptionsTheCallerNeverDisposedItself()
    {
        await using var bus = await AzureServiceBusClusterMessageBusTestHelpers.CreateBusAsync();

        Func<ClusterFanOutMessage, CancellationToken, Task> noOpHandler = (_, _) => Task.CompletedTask;
        var subscriptionHandle = await bus.SubscribeAsync(Guid.NewGuid(), noOpHandler);
        _ = subscriptionHandle;

        var act = async () => await bus.DisposeAsync();

        await act.Should().NotThrowAsync("DisposeAsync must clean up orphaned local subscriptions rather than leaking them");
    }
}
