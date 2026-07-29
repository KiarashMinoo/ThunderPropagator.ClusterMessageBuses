using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.ClusterMessageBuses.GcpPubSub;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.UnitTests.GcpPubSub;

public class GcpPubSubClusterMessageBusExtensionsTests
{
    [Fact]
    public void AddClusterGcpPubSubMessageBus_RegistersIClusterMessageBusAsASingleton()
    {
        var services = new ServiceCollection();

        services.AddClusterGcpPubSubMessageBus(options => options.ProjectId = "tp-test-project");

        var descriptor = services.FirstOrDefault(d => d.ServiceType == typeof(IClusterMessageBus));
        descriptor.Should().NotBeNull();
        descriptor!.Lifetime.Should().Be(ServiceLifetime.Singleton);
    }

    [Fact]
    public void AddClusterGcpPubSubMessageBus_AppliesTheConfigureDelegate()
    {
        var services = new ServiceCollection();

        services.AddClusterGcpPubSubMessageBus(options => options.ProjectId = "tp-test-project");

        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<Microsoft.Extensions.Options.IOptions<GcpPubSubClusterMessageBusOptions>>();

        options.Value.ProjectId.Should().Be("tp-test-project");
    }

    [Fact]
    public void AddClusterGcpPubSubMessageBus_DoesNotOverrideAnAlreadyRegisteredIClusterMessageBus()
    {
        var services = new ServiceCollection();
        var placeholder = NSubstitute.Substitute.For<IClusterMessageBus>();
        services.AddSingleton(placeholder);

        services.AddClusterGcpPubSubMessageBus(options => options.ProjectId = "tp-test-project");

        using var provider = services.BuildServiceProvider();
        var resolved = provider.GetRequiredService<IClusterMessageBus>();

        resolved.Should().BeSameAs(placeholder);
    }

    [Fact]
    public void AddClusterGcpPubSubMessageBus_RegistersAChannelResolver_WhenNoneIsAlreadyRegistered()
    {
        var services = new ServiceCollection();

        services.AddClusterGcpPubSubMessageBus(options => options.ProjectId = "tp-test-project");

        var descriptor = services.FirstOrDefault(d => d.ServiceType == typeof(IClusterChannelResolver));
        descriptor.Should().NotBeNull();
    }
}
