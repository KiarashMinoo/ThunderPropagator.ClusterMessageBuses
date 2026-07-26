using Apache.NMS;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using ThunderPropagator.Application.Channels.Cluster;
using ThunderPropagator.ClusterMessageBuses.ActiveMQ;

using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.UnitTests.ActiveMQ;

/// <summary>
/// Shared construction helpers for <see cref="ActiveMqClusterMessageBus"/> tests. Every test
/// constructs the bus through the same seams the production DI extension uses
/// (<c>IOptions&lt;ActiveMqClusterMessageBusOptions&gt;</c>, <c>ClusterConfiguration</c>,
/// <c>IClusterChannelResolver</c>, a connection-factory delegate) so no test ever needs a live
/// ActiveMQ broker: <c>Apache.NMS</c>'s <see cref="IConnection"/>, <see cref="ISession"/>,
/// <see cref="IMessageProducer"/>, <see cref="IMessageConsumer"/>, <see cref="ITopic"/>,
/// <see cref="IQueue"/>, and <see cref="ITextMessage"/> are all plain interfaces, substituted
/// directly with NSubstitute — mirroring <c>RabbitMqClusterMessageBusTestHelpers</c>'s approach
/// rather than the narrow transport-seam interfaces used for NATS.Net/DotPulsar/MQTTnet.
/// </summary>
internal static class ActiveMqClusterMessageBusTestHelpers
{
    internal static readonly Uri DefaultNodeEndpoint = new("https://node1:5000/");

    /// <summary>Builds a substitute <see cref="ITextMessage"/> whose <c>Text</c> already returns <paramref name="text"/>.</summary>
    internal static ITextMessage CreateTextMessageSubstitute(string text)
    {
        var message = Substitute.For<ITextMessage>();
        message.Text.Returns(text);
        return message;
    }

    /// <summary>
    /// Builds a substitute <see cref="ISession"/> whose <c>CreateProducerAsync()</c> always returns
    /// the same substitute <see cref="IMessageProducer"/> (so a test can fetch it back later by
    /// calling <c>CreateProducerAsync()</c> again — the stub is call-count-insensitive), whose
    /// <c>GetTopicAsync</c>/<c>GetQueueAsync</c> return a fresh substitute destination per call, whose
    /// <c>CreateConsumerAsync</c> returns a fresh substitute consumer per call, and whose
    /// <c>CreateTextMessageAsync(text)</c> returns a substitute <see cref="ITextMessage"/> whose
    /// <c>Text</c> already reflects the given argument.
    /// </summary>
    internal static ISession CreateSessionSubstitute()
    {
        var session = Substitute.For<ISession>();
        var producer = Substitute.For<IMessageProducer>();

        session.CreateProducerAsync().Returns(Task.FromResult(producer));
        session.CreateConsumerAsync(Arg.Any<IDestination>()).Returns(_ => Task.FromResult(Substitute.For<IMessageConsumer>()));
        session.GetTopicAsync(Arg.Any<string>()).Returns(_ => Task.FromResult(Substitute.For<ITopic>()));
        session.GetQueueAsync(Arg.Any<string>()).Returns(_ => Task.FromResult(Substitute.For<IQueue>()));
        session.CreateTextMessageAsync(Arg.Any<string>())
            .Returns(callInfo => Task.FromResult(CreateTextMessageSubstitute((string)callInfo[0])));

        return session;
    }

    /// <summary>
    /// Builds a substitute <see cref="IConnection"/> whose <c>CreateSessionAsync()</c> returns a
    /// fresh substitute <see cref="ISession"/> (see <see cref="CreateSessionSubstitute"/>) on every
    /// call — safe for tests that don't need to inspect which session a particular operation used.
    /// </summary>
    internal static IConnection CreateSubstituteConnection()
    {
        var connection = Substitute.For<IConnection>();
        connection.CreateSessionAsync().Returns(_ => Task.FromResult(CreateSessionSubstitute()));

        return connection;
    }

    /// <summary>
    /// Same as <see cref="CreateSubstituteConnection"/>, but also returns every substitute
    /// <see cref="ISession"/> created, in creation order. <see cref="ActiveMqClusterMessageBus.EnsureInitializedAsync"/>
    /// creates exactly three, in this order: the shared publish session (index 0), the request-
    /// listener session (index 1), and the reply-listener session (index 2); each subsequent
    /// <c>SubscribeAsync</c> call appends one more.
    /// </summary>
    internal static IConnection CreateTrackedConnection(out List<ISession> sessions)
    {
        var createdSessions = new List<ISession>();
        var connection = Substitute.For<IConnection>();
        connection.CreateSessionAsync().Returns(_ =>
        {
            var session = CreateSessionSubstitute();
            createdSessions.Add(session);
            return Task.FromResult(session);
        });

        sessions = createdSessions;
        return connection;
    }

    internal static Func<CancellationToken, Task<IConnection>> ConnectionFactoryReturning(IConnection connection)
        => _ => Task.FromResult(connection);

    internal static async Task<ActiveMqClusterMessageBus> CreateBusAsync(
        IConnection? connection = null,
        IClusterChannelResolver? channelResolver = null,
        Uri? nodeEndpoint = null,
        TimeSpan? requestTimeout = null)
    {
        connection ??= CreateSubstituteConnection();
        channelResolver ??= Substitute.For<IClusterChannelResolver>();

        var options = Options.Create(new ActiveMqClusterMessageBusOptions
        {
            BrokerUri = "activemq:tcp://localhost:61616",
            RequestTimeout = requestTimeout ?? TimeSpan.FromSeconds(30),
        });

        var clusterConfiguration = new ClusterConfiguration { NodeEndpoint = nodeEndpoint ?? DefaultNodeEndpoint };

        var bus = new ActiveMqClusterMessageBus(
            options,
            clusterConfiguration,
            channelResolver,
            NullLoggerFactory.Instance,
            ConnectionFactoryReturning(connection));

        await bus.EnsureInitializedAsync(CancellationToken.None);

        return bus;
    }
}
