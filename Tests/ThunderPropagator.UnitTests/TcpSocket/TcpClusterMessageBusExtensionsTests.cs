using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;
using ThunderPropagator.ClusterMessageBuses.TcpSocket;

namespace ThunderPropagator.UnitTests.TcpSocket;

public class TcpClusterMessageBusExtensionsTests
{
    [Fact]
    public void AddClusterTcpMessageBus_RegistersIClusterMessageBusAsASingleton()
    {
        var services = new ServiceCollection();

        services.AddClusterTcpMessageBus(options => options.Port = 6200);

        var descriptor = services.FirstOrDefault(d => d.ServiceType == typeof(IClusterMessageBus));
        descriptor.Should().NotBeNull();
        descriptor!.Lifetime.Should().Be(ServiceLifetime.Singleton);
    }

    [Fact]
    public void AddClusterTcpMessageBus_AppliesTheConfigureDelegate()
    {
        var services = new ServiceCollection();

        services.AddClusterTcpMessageBus(options => options.Port = 6200);

        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<Microsoft.Extensions.Options.IOptions<TcpClusterMessageBusOptions>>();

        options.Value.Port.Should().Be(6200);
    }

    [Fact]
    public void AddClusterTcpMessageBus_DoesNotOverrideAnAlreadyRegisteredIClusterMessageBus()
    {
        var services = new ServiceCollection();
        var placeholder = NSubstitute.Substitute.For<IClusterMessageBus>();
        services.AddSingleton(placeholder);

        services.AddClusterTcpMessageBus(options => options.Port = 6200);

        using var provider = services.BuildServiceProvider();
        var resolved = provider.GetRequiredService<IClusterMessageBus>();

        resolved.Should().BeSameAs(placeholder);
    }

    [Fact]
    public void AddClusterTcpMessageBus_RegistersAChannelResolver_WhenNoneIsAlreadyRegistered()
    {
        var services = new ServiceCollection();

        services.AddClusterTcpMessageBus(options => options.Port = 6200);

        var descriptor = services.FirstOrDefault(d => d.ServiceType == typeof(IClusterChannelResolver));
        descriptor.Should().NotBeNull();
    }
}
