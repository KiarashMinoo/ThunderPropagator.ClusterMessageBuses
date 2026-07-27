using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.ClusterMessageBuses.AzureServiceBus;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.UnitTests.AzureServiceBus;

public class AzureServiceBusClusterMessageBusExtensionsTests
{
    [Fact]
    public void AddClusterAzureServiceBusMessageBus_RegistersIClusterMessageBusAsASingleton()
    {
        var services = new ServiceCollection();

        services.AddClusterAzureServiceBusMessageBus(options => options.ResourcePrefix = "tp-test");

        var descriptor = services.FirstOrDefault(d => d.ServiceType == typeof(IClusterMessageBus));
        descriptor.Should().NotBeNull();
        descriptor!.Lifetime.Should().Be(ServiceLifetime.Singleton);
    }

    [Fact]
    public void AddClusterAzureServiceBusMessageBus_AppliesTheConfigureDelegate()
    {
        var services = new ServiceCollection();

        services.AddClusterAzureServiceBusMessageBus(options => options.ResourcePrefix = "tp-test");

        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<Microsoft.Extensions.Options.IOptions<AzureServiceBusClusterMessageBusOptions>>();

        options.Value.ResourcePrefix.Should().Be("tp-test");
    }

    [Fact]
    public void AddClusterAzureServiceBusMessageBus_DoesNotOverrideAnAlreadyRegisteredIClusterMessageBus()
    {
        var services = new ServiceCollection();
        var placeholder = NSubstitute.Substitute.For<IClusterMessageBus>();
        services.AddSingleton(placeholder);

        services.AddClusterAzureServiceBusMessageBus(options => options.ResourcePrefix = "tp-test");

        using var provider = services.BuildServiceProvider();
        var resolved = provider.GetRequiredService<IClusterMessageBus>();

        resolved.Should().BeSameAs(placeholder);
    }

    [Fact]
    public void AddClusterAzureServiceBusMessageBus_RegistersAChannelResolver_WhenNoneIsAlreadyRegistered()
    {
        var services = new ServiceCollection();

        services.AddClusterAzureServiceBusMessageBus(options => options.ResourcePrefix = "tp-test");

        var descriptor = services.FirstOrDefault(d => d.ServiceType == typeof(IClusterChannelResolver));
        descriptor.Should().NotBeNull();
    }
}
