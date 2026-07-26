using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.ClusterMessageBuses.Pulsar;

using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.UnitTests.Pulsar;

public class PulsarClusterMessageBusExtensionsTests
{
    [Fact]
    public void AddClusterPulsarMessageBus_RegistersIClusterMessageBusAndItsChannelResolverDependency()
    {
        var services = new ServiceCollection();

        services.AddClusterPulsarMessageBus(options => options.ServiceUrl = "pulsar://broker:6650");

        services.Should().Contain(d => d.ServiceType == typeof(IClusterMessageBus));
        services.Should().Contain(d => d.ServiceType == typeof(IClusterChannelResolver));
    }

    [Fact]
    public void AddClusterPulsarMessageBus_TryAddSingleton_DoesNotOverrideAnExistingIClusterMessageBusRegistration()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IClusterMessageBus>(_ => throw new InvalidOperationException("should never be constructed"));

        services.AddClusterPulsarMessageBus(options => options.ServiceUrl = "pulsar://broker:6650");

        services.Count(d => d.ServiceType == typeof(IClusterMessageBus)).Should().Be(1,
            "TryAddSingleton must not add a second registration once one already exists — the first registration wins, matching HttpClusterTransportExtensions");
    }

    [Fact]
    public void AddClusterPulsarMessageBus_ConfiguresOptionsFromTheProvidedDelegate()
    {
        var services = new ServiceCollection();

        services.AddClusterPulsarMessageBus(options =>
        {
            options.ServiceUrl = "pulsar://broker1:6650";
            options.TopicPrefix = "persistent://public/default/custom-prefix";
        });

        using var provider = services.BuildServiceProvider();
        var resolved = provider.GetRequiredService<IOptions<PulsarClusterMessageBusOptions>>().Value;

        resolved.ServiceUrl.Should().Be("pulsar://broker1:6650");
        resolved.TopicPrefix.Should().Be("persistent://public/default/custom-prefix");
    }
}
