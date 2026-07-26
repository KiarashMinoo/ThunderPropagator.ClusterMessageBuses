using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.ClusterMessageBuses.RedisPubSub;

using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.UnitTests.RedisPubSub;

public class RedisPubSubClusterMessageBusExtensionsTests
{
    [Fact]
    public void AddClusterRedisPubSubMessageBus_RegistersIClusterMessageBusAndItsChannelResolverDependency()
    {
        var services = new ServiceCollection();

        services.AddClusterRedisPubSubMessageBus(options => options.ConnectionString = "redis1:6379");

        services.Should().Contain(d => d.ServiceType == typeof(IClusterMessageBus));
        services.Should().Contain(d => d.ServiceType == typeof(IClusterChannelResolver));
    }

    [Fact]
    public void AddClusterRedisPubSubMessageBus_TryAddSingleton_DoesNotOverrideAnExistingIClusterMessageBusRegistration()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IClusterMessageBus>(_ => throw new InvalidOperationException("should never be constructed"));

        services.AddClusterRedisPubSubMessageBus(options => options.ConnectionString = "redis1:6379");

        services.Count(d => d.ServiceType == typeof(IClusterMessageBus)).Should().Be(1,
            "TryAddSingleton must not add a second registration once one already exists — the first registration wins, matching HttpClusterTransportExtensions");
    }

    [Fact]
    public void AddClusterRedisPubSubMessageBus_ConfiguresOptionsFromTheProvidedDelegate()
    {
        var services = new ServiceCollection();

        services.AddClusterRedisPubSubMessageBus(options =>
        {
            options.ConnectionString = "redis1:6379";
            options.ChannelPrefix = "custom:prefix";
        });

        using var provider = services.BuildServiceProvider();
        var resolved = provider.GetRequiredService<IOptions<RedisPubSubClusterMessageBusOptions>>().Value;

        resolved.ConnectionString.Should().Be("redis1:6379");
        resolved.ChannelPrefix.Should().Be("custom:prefix");
    }
}
