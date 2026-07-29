using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.ClusterMessageBuses.Grpc;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.UnitTests.Grpc;

public class GrpcClusterMessageBusExtensionsTests
{
    [Fact]
    public void AddClusterGrpcMessageBus_RegistersIClusterMessageBusAndItsChannelResolverDependency()
    {
        var services = new ServiceCollection();

        services.AddClusterGrpcMessageBus(options => options.RequestTimeout = TimeSpan.FromSeconds(10));

        services.Should().Contain(d => d.ServiceType == typeof(IClusterMessageBus));
        services.Should().Contain(d => d.ServiceType == typeof(IClusterChannelResolver));
    }

    [Fact]
    public void AddClusterGrpcMessageBus_TryAddSingleton_DoesNotOverrideAnExistingIClusterMessageBusRegistration()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IClusterMessageBus>(_ => throw new InvalidOperationException("should never be constructed"));

        services.AddClusterGrpcMessageBus(options => options.RequestTimeout = TimeSpan.FromSeconds(10));

        services.Count(d => d.ServiceType == typeof(IClusterMessageBus)).Should().Be(1,
            "TryAddSingleton must not add a second registration once one already exists — the first registration wins, matching HttpClusterTransportExtensions");
    }

    [Fact]
    public void AddClusterGrpcMessageBus_ConfiguresOptionsFromTheProvidedDelegate()
    {
        var services = new ServiceCollection();

        services.AddClusterGrpcMessageBus(options =>
        {
            options.RequestTimeout = TimeSpan.FromSeconds(45);
            options.FallbackToHttp = true;
        });

        using var provider = services.BuildServiceProvider();
        var resolved = provider.GetRequiredService<IOptions<GrpcClusterMessageBusOptions>>().Value;

        resolved.RequestTimeout.Should().Be(TimeSpan.FromSeconds(45));
        resolved.FallbackToHttp.Should().BeTrue();
    }
}
