using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.ClusterMessageBuses.NATS;

using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.UnitTests.NATS;

public class NatsClusterMessageBusExtensionsTests
{
    [Fact]
    public void AddClusterNatsMessageBus_RegistersIClusterMessageBusAndItsChannelResolverDependency()
    {
        var services = new ServiceCollection();

        services.AddClusterNatsMessageBus(options => options.Url = "nats://broker:4222");

        services.Should().Contain(d => d.ServiceType == typeof(IClusterMessageBus));
        services.Should().Contain(d => d.ServiceType == typeof(IClusterChannelResolver));
    }

    [Fact]
    public void AddClusterNatsMessageBus_TryAddSingleton_DoesNotOverrideAnExistingIClusterMessageBusRegistration()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IClusterMessageBus>(_ => throw new InvalidOperationException("should never be constructed"));

        services.AddClusterNatsMessageBus(options => options.Url = "nats://broker:4222");

        services.Count(d => d.ServiceType == typeof(IClusterMessageBus)).Should().Be(1,
            "TryAddSingleton must not add a second registration once one already exists — the first registration wins, matching HttpClusterTransportExtensions");
    }

    [Fact]
    public void AddClusterNatsMessageBus_ConfiguresOptionsFromTheProvidedDelegate()
    {
        var services = new ServiceCollection();

        services.AddClusterNatsMessageBus(options =>
        {
            options.Url = "nats://broker1:4222";
            options.SubjectPrefix = "custom.prefix";
        });

        using var provider = services.BuildServiceProvider();
        var resolved = provider.GetRequiredService<IOptions<NatsClusterMessageBusOptions>>().Value;

        resolved.Url.Should().Be("nats://broker1:4222");
        resolved.SubjectPrefix.Should().Be("custom.prefix");
    }
}
