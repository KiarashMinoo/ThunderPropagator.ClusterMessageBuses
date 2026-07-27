using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;
using ThunderPropagator.ClusterMessageBuses.UdpClient;

namespace ThunderPropagator.UnitTests.UdpClient;

public class UdpClusterMessageBusExtensionsTests
{
    [Fact]
    public void AddClusterUdpMessageBus_RegistersIClusterMessageBusAsASingleton()
    {
        var services = new ServiceCollection();

        services.AddClusterUdpMessageBus(options => options.Port = 6400);

        var descriptor = services.FirstOrDefault(d => d.ServiceType == typeof(IClusterMessageBus));
        descriptor.Should().NotBeNull();
        descriptor!.Lifetime.Should().Be(ServiceLifetime.Singleton);
    }

    [Fact]
    public void AddClusterUdpMessageBus_AppliesTheConfigureDelegate()
    {
        var services = new ServiceCollection();

        services.AddClusterUdpMessageBus(options => options.Port = 6400);

        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<Microsoft.Extensions.Options.IOptions<UdpClusterMessageBusOptions>>();

        options.Value.Port.Should().Be(6400);
    }

    [Fact]
    public void AddClusterUdpMessageBus_DoesNotOverrideAnAlreadyRegisteredIClusterMessageBus()
    {
        var services = new ServiceCollection();
        var placeholder = NSubstitute.Substitute.For<IClusterMessageBus>();
        services.AddSingleton(placeholder);

        services.AddClusterUdpMessageBus(options => options.Port = 6400);

        using var provider = services.BuildServiceProvider();
        var resolved = provider.GetRequiredService<IClusterMessageBus>();

        resolved.Should().BeSameAs(placeholder);
    }

    [Fact]
    public void AddClusterUdpMessageBus_RegistersAChannelResolver_WhenNoneIsAlreadyRegistered()
    {
        var services = new ServiceCollection();

        services.AddClusterUdpMessageBus(options => options.Port = 6400);

        var descriptor = services.FirstOrDefault(d => d.ServiceType == typeof(IClusterChannelResolver));
        descriptor.Should().NotBeNull();
    }
}
