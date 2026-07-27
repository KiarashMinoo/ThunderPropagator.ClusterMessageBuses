using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.ClusterMessageBuses.AwsSqs;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.UnitTests.AwsSqs;

public class AwsSqsClusterMessageBusExtensionsTests
{
    [Fact]
    public void AddClusterAwsSqsMessageBus_RegistersIClusterMessageBusAsASingleton()
    {
        var services = new ServiceCollection();

        services.AddClusterAwsSqsMessageBus(options => options.ResourcePrefix = "tp-test");

        var descriptor = services.FirstOrDefault(d => d.ServiceType == typeof(IClusterMessageBus));
        descriptor.Should().NotBeNull();
        descriptor!.Lifetime.Should().Be(ServiceLifetime.Singleton);
    }

    [Fact]
    public void AddClusterAwsSqsMessageBus_AppliesTheConfigureDelegate()
    {
        var services = new ServiceCollection();

        services.AddClusterAwsSqsMessageBus(options => options.ResourcePrefix = "tp-test");

        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<Microsoft.Extensions.Options.IOptions<AwsSqsClusterMessageBusOptions>>();

        options.Value.ResourcePrefix.Should().Be("tp-test");
    }

    [Fact]
    public void AddClusterAwsSqsMessageBus_DoesNotOverrideAnAlreadyRegisteredIClusterMessageBus()
    {
        var services = new ServiceCollection();
        var placeholder = NSubstitute.Substitute.For<IClusterMessageBus>();
        services.AddSingleton(placeholder);

        services.AddClusterAwsSqsMessageBus(options => options.ResourcePrefix = "tp-test");

        using var provider = services.BuildServiceProvider();
        var resolved = provider.GetRequiredService<IClusterMessageBus>();

        resolved.Should().BeSameAs(placeholder);
    }

    [Fact]
    public void AddClusterAwsSqsMessageBus_RegistersAChannelResolver_WhenNoneIsAlreadyRegistered()
    {
        var services = new ServiceCollection();

        services.AddClusterAwsSqsMessageBus(options => options.ResourcePrefix = "tp-test");

        var descriptor = services.FirstOrDefault(d => d.ServiceType == typeof(IClusterChannelResolver));
        descriptor.Should().NotBeNull();
    }
}
