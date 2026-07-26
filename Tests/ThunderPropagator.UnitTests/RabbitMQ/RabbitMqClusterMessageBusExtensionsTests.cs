using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.ClusterMessageBuses.RabbitMQ;

using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.UnitTests.RabbitMQ;

public class RabbitMqClusterMessageBusExtensionsTests
{
    [Fact]
    public void AddClusterRabbitMqMessageBus_RegistersIClusterMessageBusAndItsChannelResolverDependency()
    {
        var services = new ServiceCollection();

        services.AddClusterRabbitMqMessageBus(options => options.ConnectionString = "amqp://guest:guest@broker:5672/");

        services.Should().Contain(d => d.ServiceType == typeof(IClusterMessageBus));
        services.Should().Contain(d => d.ServiceType == typeof(IClusterChannelResolver));
    }

    [Fact]
    public void AddClusterRabbitMqMessageBus_TryAddSingleton_DoesNotOverrideAnExistingIClusterMessageBusRegistration()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IClusterMessageBus>(_ => throw new InvalidOperationException("should never be constructed"));

        services.AddClusterRabbitMqMessageBus(options => options.ConnectionString = "amqp://guest:guest@broker:5672/");

        services.Count(d => d.ServiceType == typeof(IClusterMessageBus)).Should().Be(1,
            "TryAddSingleton must not add a second registration once one already exists — the first registration wins, matching HttpClusterTransportExtensions");
    }

    [Fact]
    public void AddClusterRabbitMqMessageBus_ConfiguresOptionsFromTheProvidedDelegate()
    {
        var services = new ServiceCollection();

        services.AddClusterRabbitMqMessageBus(options =>
        {
            options.ConnectionString = "amqp://guest:guest@broker1:5672/";
            options.ExchangePrefix = "custom.prefix";
        });

        using var provider = services.BuildServiceProvider();
        var resolved = provider.GetRequiredService<IOptions<RabbitMqClusterMessageBusOptions>>().Value;

        resolved.ConnectionString.Should().Be("amqp://guest:guest@broker1:5672/");
        resolved.ExchangePrefix.Should().Be("custom.prefix");
    }
}
