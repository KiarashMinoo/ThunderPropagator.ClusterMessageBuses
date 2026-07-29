using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;
using ThunderPropagator.ClusterMessageBuses.ZeroMQ;

namespace ThunderPropagator.UnitTests.ZeroMQ;

public class ZeroMqClusterMessageBusExtensionsTests
{
    [Fact]
    public void AddClusterZeroMqMessageBus_RegistersIClusterMessageBusAndItsChannelResolverDependency()
    {
        var services = new ServiceCollection();

        services.AddClusterZeroMqMessageBus(options => options.RequestTimeout = TimeSpan.FromSeconds(10));

        services.Should().Contain(d => d.ServiceType == typeof(IClusterMessageBus));
        services.Should().Contain(d => d.ServiceType == typeof(IClusterChannelResolver));
    }

    [Fact]
    public void AddClusterZeroMqMessageBus_TryAddSingleton_DoesNotOverrideAnExistingIClusterMessageBusRegistration()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IClusterMessageBus>(_ => throw new InvalidOperationException("should never be constructed"));

        services.AddClusterZeroMqMessageBus(options => options.RequestTimeout = TimeSpan.FromSeconds(10));

        services.Count(d => d.ServiceType == typeof(IClusterMessageBus)).Should().Be(1,
            "TryAddSingleton must not add a second registration once one already exists — the first registration wins, matching HttpClusterTransportExtensions");
    }

    [Fact]
    public void AddClusterZeroMqMessageBus_ConfiguresOptionsFromTheProvidedDelegate()
    {
        var services = new ServiceCollection();

        services.AddClusterZeroMqMessageBus(options =>
        {
            options.RequestTimeout = TimeSpan.FromSeconds(45);
            options.SendHighWatermark = 500;
        });

        using var provider = services.BuildServiceProvider();
        var resolved = provider.GetRequiredService<IOptions<ClusterZeroMqOptions>>().Value;

        resolved.RequestTimeout.Should().Be(TimeSpan.FromSeconds(45));
        resolved.SendHighWatermark.Should().Be(500);
    }
}
