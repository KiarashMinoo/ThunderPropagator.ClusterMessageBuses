using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using RabbitMQ.Client;
using ThunderPropagator.Application.Channels.Cluster;
using ThunderPropagator.ClusterMessageBuses.RabbitMQ;

using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.UnitTests.RabbitMQ;

/// <summary>
/// Shared construction helpers for <see cref="RabbitMqClusterMessageBus"/> tests. Every test
/// constructs the bus through the same seams the production DI extension uses
/// (<c>IOptions&lt;RabbitMqClusterMessageBusOptions&gt;</c>, <c>ClusterConfiguration</c>,
/// <c>IClusterChannelResolver</c>, a connection-factory delegate) so no test ever needs a live
/// RabbitMQ broker: <see cref="IConnection"/> and <see cref="IChannel"/>
/// are both plain interfaces, substituted directly with NSubstitute. <see cref="CreateBusAsync"/>
/// also awaits <c>EnsureInitializedAsync</c> before returning, since — unlike Kafka's
/// synchronous-constructor design — RabbitMQ.Client v7's connection/channel/topology setup is fully
/// asynchronous and cannot happen inside the bus's constructor.
/// </summary>
internal static class RabbitMqClusterMessageBusTestHelpers
{
    internal static readonly Uri DefaultNodeEndpoint = new("https://node1:5000/");

    /// <summary>
    /// Builds a substitute <see cref="IConnection"/> whose <c>CreateChannelAsync()</c> returns a
    /// fresh substitute <see cref="IChannel"/> on every call (three are created eagerly during
    /// initialization — publish, request-listener, reply-listener — plus one more per
    /// <c>SubscribeAsync</c> call), and whose every substitute channel's <c>QueueDeclareAsync</c>
    /// returns a generated queue name so <c>SubscribeAsync</c>'s anonymous-queue flow has something
    /// real to bind and consume from.
    /// </summary>
    internal static IConnection CreateSubstituteConnection()
    {
        var connection = Substitute.For<IConnection>();
        connection.CreateChannelAsync().Returns(_ =>
        {
            var channel = Substitute.For<IChannel>();
            channel.QueueDeclareAsync(Arg.Any<string>(), Arg.Any<bool>(), Arg.Any<bool>(), Arg.Any<bool>(), Arg.Any<IDictionary<string, object?>?>())
                .Returns(_ => new QueueDeclareOk($"generated-queue-{Guid.NewGuid():N}", 0, 0));
            channel.BasicConsumeAsync(Arg.Any<string>(), Arg.Any<bool>(), Arg.Any<IAsyncBasicConsumer>())
                .Returns(_ => $"consumer-tag-{Guid.NewGuid():N}");
            return Task.FromResult(channel);
        });

        return connection;
    }

    /// <summary>
    /// Same as <see cref="CreateSubstituteConnection"/>, but also returns every substitute
    /// <see cref="IChannel"/> created, in creation order. <see cref="RabbitMqClusterMessageBus.EnsureInitializedAsync"/>
    /// creates exactly three, in this order: the shared publish channel (index 0), the request-
    /// listener channel (index 1), and the reply-listener channel (index 2); each subsequent
    /// <c>SubscribeAsync</c> call appends one more.
    /// </summary>
    internal static IConnection CreateTrackedConnection(out List<IChannel> channels)
    {
        var createdChannels = new List<IChannel>();
        var connection = Substitute.For<IConnection>();
        connection.CreateChannelAsync().Returns(_ =>
        {
            var channel = Substitute.For<IChannel>();
            channel.QueueDeclareAsync(Arg.Any<string>(), Arg.Any<bool>(), Arg.Any<bool>(), Arg.Any<bool>(), Arg.Any<IDictionary<string, object?>?>())
                .Returns(_ => new QueueDeclareOk($"generated-queue-{Guid.NewGuid():N}", 0, 0));
            channel.BasicConsumeAsync(Arg.Any<string>(), Arg.Any<bool>(), Arg.Any<IAsyncBasicConsumer>())
                .Returns(_ => $"consumer-tag-{Guid.NewGuid():N}");
            createdChannels.Add(channel);
            return Task.FromResult(channel);
        });

        channels = createdChannels;
        return connection;
    }

    internal static Func<CancellationToken, Task<IConnection>> ConnectionFactoryReturning(IConnection connection)
        => _ => Task.FromResult(connection);

    internal static async Task<RabbitMqClusterMessageBus> CreateBusAsync(
        IConnection? connection = null,
        IClusterChannelResolver? channelResolver = null,
        Uri? nodeEndpoint = null,
        TimeSpan? requestTimeout = null)
    {
        connection ??= CreateSubstituteConnection();
        channelResolver ??= Substitute.For<IClusterChannelResolver>();

        var options = Options.Create(new RabbitMqClusterMessageBusOptions
        {
            ConnectionString = "amqp://guest:guest@localhost:5672/",
            RequestTimeout = requestTimeout ?? TimeSpan.FromSeconds(30),
        });

        var clusterConfiguration = new ClusterConfiguration { NodeEndpoint = nodeEndpoint ?? DefaultNodeEndpoint };

        var bus = new RabbitMqClusterMessageBus(
            options,
            clusterConfiguration,
            channelResolver,
            NullLoggerFactory.Instance,
            ConnectionFactoryReturning(connection));

        await bus.EnsureInitializedAsync(CancellationToken.None);

        return bus;
    }
}
