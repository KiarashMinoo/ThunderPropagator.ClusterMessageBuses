using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using StackExchange.Redis;
using ThunderPropagator.Application.Channels.Cluster;
using ThunderPropagator.ClusterMessageBuses.RedisPubSub;

using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.UnitTests.RedisPubSub;

/// <summary>
/// Shared construction helpers for <see cref="RedisPubSubClusterMessageBus"/> tests. Every test
/// constructs the bus through the same seams the production DI extension uses
/// (<c>IOptions&lt;RedisPubSubClusterMessageBusOptions&gt;</c>, <c>ClusterConfiguration</c>,
/// <c>IClusterChannelResolver</c>, a connection-factory delegate) so no test ever needs a live Redis
/// server: <c>StackExchange.Redis</c>'s <see cref="IConnectionMultiplexer"/> and <see cref="ISubscriber"/>
/// are both plain interfaces, substituted directly with NSubstitute — mirroring
/// <c>ActiveMqClusterMessageBusTestHelpers</c>'s approach rather than the narrow transport-seam
/// interfaces used for NATS.Net/DotPulsar/MQTTnet.
/// </summary>
internal static class RedisPubSubClusterMessageBusTestHelpers
{
    internal static readonly Uri DefaultNodeEndpoint = new("https://node1:5000/");

    /// <summary>
    /// Builds a substitute <see cref="ISubscriber"/> whose <c>PublishAsync</c>,
    /// <c>SubscribeAsync</c>, and <c>UnsubscribeAsync</c> all complete successfully by default, so
    /// tests only need to override the specific call they care about.
    /// </summary>
    internal static ISubscriber CreateSubscriberSubstitute()
    {
        var subscriber = Substitute.For<ISubscriber>();

        subscriber.PublishAsync(Arg.Any<RedisChannel>(), Arg.Any<RedisValue>()).Returns(Task.FromResult(1L));
        subscriber.SubscribeAsync(Arg.Any<RedisChannel>(), Arg.Any<Action<RedisChannel, RedisValue>>()).Returns(Task.CompletedTask);
        subscriber.UnsubscribeAsync(Arg.Any<RedisChannel>(), Arg.Any<Action<RedisChannel, RedisValue>>()).Returns(Task.CompletedTask);

        return subscriber;
    }

    /// <summary>
    /// Builds a substitute <see cref="IConnectionMultiplexer"/> whose <c>GetSubscriber()</c> returns
    /// the given (or a fresh) substitute <see cref="ISubscriber"/> every time it is called — unlike
    /// ActiveMQ's <c>CreateSessionAsync</c>, <see cref="RedisPubSubClusterMessageBus"/> only ever
    /// calls <c>GetSubscriber()</c> once, during <c>EnsureInitializedAsync</c>, and reuses the same
    /// <see cref="ISubscriber"/> for every publish/subscribe afterwards.
    /// </summary>
    internal static IConnectionMultiplexer CreateSubstituteConnection(ISubscriber? subscriber = null)
    {
        var connection = Substitute.For<IConnectionMultiplexer>();
        connection.GetSubscriber().Returns(subscriber ?? CreateSubscriberSubstitute());

        return connection;
    }

    internal static Func<CancellationToken, Task<IConnectionMultiplexer>> ConnectionFactoryReturning(IConnectionMultiplexer connection)
        => _ => Task.FromResult(connection);

    internal static async Task<RedisPubSubClusterMessageBus> CreateBusAsync(
        IConnectionMultiplexer? connection = null,
        IClusterChannelResolver? channelResolver = null,
        Uri? nodeEndpoint = null,
        TimeSpan? requestTimeout = null)
    {
        connection ??= CreateSubstituteConnection();
        channelResolver ??= Substitute.For<IClusterChannelResolver>();

        var options = Options.Create(new RedisPubSubClusterMessageBusOptions
        {
            ConnectionString = "localhost:6379",
            RequestTimeout = requestTimeout ?? TimeSpan.FromSeconds(30),
        });

        var clusterConfiguration = new ClusterConfiguration { NodeEndpoint = nodeEndpoint ?? DefaultNodeEndpoint };

        var bus = new RedisPubSubClusterMessageBus(
            options,
            clusterConfiguration,
            channelResolver,
            NullLoggerFactory.Instance,
            ConnectionFactoryReturning(connection));

        await bus.EnsureInitializedAsync(CancellationToken.None);

        return bus;
    }
}
