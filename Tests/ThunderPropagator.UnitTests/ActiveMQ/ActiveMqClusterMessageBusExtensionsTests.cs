using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.ClusterMessageBuses.ActiveMQ;

using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.UnitTests.ActiveMQ;

public class ActiveMqClusterMessageBusExtensionsTests
{
    [Fact]
    public void AddClusterActiveMqMessageBus_RegistersIClusterMessageBusAndItsChannelResolverDependency()
    {
        var services = new ServiceCollection();

        services.AddClusterActiveMqMessageBus(options => options.BrokerUri = "activemq:tcp://broker1:61616");

        services.Should().Contain(d => d.ServiceType == typeof(IClusterMessageBus));
        services.Should().Contain(d => d.ServiceType == typeof(IClusterChannelResolver));
    }

    [Fact]
    public void AddClusterActiveMqMessageBus_TryAddSingleton_DoesNotOverrideAnExistingIClusterMessageBusRegistration()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IClusterMessageBus>(_ => throw new InvalidOperationException("should never be constructed"));

        services.AddClusterActiveMqMessageBus(options => options.BrokerUri = "activemq:tcp://broker1:61616");

        services.Count(d => d.ServiceType == typeof(IClusterMessageBus)).Should().Be(1,
            "TryAddSingleton must not add a second registration once one already exists — the first registration wins, matching HttpClusterTransportExtensions");
    }

    [Fact]
    public void AddClusterActiveMqMessageBus_ConfiguresOptionsFromTheProvidedDelegate()
    {
        var services = new ServiceCollection();

        services.AddClusterActiveMqMessageBus(options =>
        {
            options.BrokerUri = "activemq:tcp://broker1:61616";
            options.TopicPrefix = "custom.prefix";
        });

        using var provider = services.BuildServiceProvider();
        var resolved = provider.GetRequiredService<IOptions<ActiveMqClusterMessageBusOptions>>().Value;

        resolved.BrokerUri.Should().Be("activemq:tcp://broker1:61616");
        resolved.TopicPrefix.Should().Be("custom.prefix");
    }
}
