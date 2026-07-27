using Azure;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using ThunderPropagator.Application.Channels.Cluster;
using ThunderPropagator.ClusterMessageBuses.AzureServiceBus;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.UnitTests.AzureServiceBus;

/// <summary>
/// Shared construction helpers for <see cref="AzureServiceBusClusterMessageBus"/> tests. Every test
/// constructs the bus through the same seams the production DI extension uses
/// (<c>IOptions&lt;AzureServiceBusClusterMessageBusOptions&gt;</c>, <c>ClusterConfiguration</c>,
/// <c>IClusterChannelResolver</c>, client-factory delegates) so no test ever needs a live Service Bus
/// namespace: <see cref="ServiceBusClient"/>, <see cref="ServiceBusSender"/>,
/// <see cref="ServiceBusReceiver"/>, and <see cref="ServiceBusAdministrationClient"/> are all
/// non-sealed with a protected parameterless constructor and virtual members — explicitly designed
/// by the Azure SDK team to be mocked — so they're substituted directly with NSubstitute, no wrapper
/// interface needed.
/// </summary>
internal static class AzureServiceBusClusterMessageBusTestHelpers
{
    internal static readonly Uri DefaultNodeEndpoint = new("https://node1:5000/");

    internal const string TestConnectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=Test;SharedAccessKey=dGVzdA==";

    /// <summary>
    /// Wraps <paramref name="exists"/> in a <see cref="Response{T}"/> the way
    /// <see cref="ServiceBusAdministrationClient"/>'s real <c>*ExistsAsync</c> methods do. The raw
    /// <see cref="Response"/> passed as the second argument is never inspected by production code
    /// (which only reads <c>.Value</c>), so a bare NSubstitute stand-in is enough.
    /// </summary>
    internal static Response<bool> ExistsResponse(bool exists) => Response.FromValue(exists, Substitute.For<Response>());

    /// <summary>
    /// Builds a substitute <see cref="ServiceBusAdministrationClient"/> whose <c>*ExistsAsync</c>
    /// methods default to <see langword="false"/> (so every <c>Ensure*Async</c> helper proceeds to
    /// create the entity) and whose <c>Create*Async</c>/<c>DeleteSubscriptionAsync</c> calls all
    /// succeed trivially. <see cref="QueueProperties"/>/<see cref="TopicProperties"/>/
    /// <see cref="SubscriptionProperties"/> only have internal constructors, so
    /// <see cref="ServiceBusModelFactory"/> is used to fabricate throwaway instances — production
    /// code never inspects the returned value, only that the call didn't throw.
    /// </summary>
    internal static ServiceBusAdministrationClient CreateAdminSubstitute()
    {
        var admin = Substitute.For<ServiceBusAdministrationClient>();

        admin.QueueExistsAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(ExistsResponse(false)));
        admin.TopicExistsAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(ExistsResponse(false)));
        admin.SubscriptionExistsAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(ExistsResponse(false)));

        admin.CreateQueueAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(callInfo => Task.FromResult(Response.FromValue(
                ServiceBusModelFactory.QueueProperties((string)callInfo[0]), Substitute.For<Response>())));

        admin.CreateTopicAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(callInfo => Task.FromResult(Response.FromValue(
                ServiceBusModelFactory.TopicProperties((string)callInfo[0]), Substitute.For<Response>())));

        admin.CreateSubscriptionAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(callInfo => Task.FromResult(Response.FromValue(
                ServiceBusModelFactory.SubscriptionProperties((string)callInfo[0], (string)callInfo[1]), Substitute.For<Response>())));

        admin.DeleteSubscriptionAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Substitute.For<Response>());

        return admin;
    }

    /// <summary>A substitute <see cref="ServiceBusSender"/> whose <c>SendMessageAsync</c> always succeeds trivially.</summary>
    internal static ServiceBusSender CreateSenderSubstitute()
    {
        var sender = Substitute.For<ServiceBusSender>();
        sender.SendMessageAsync(Arg.Any<ServiceBusMessage>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);
        return sender;
    }

    /// <summary>A substitute <see cref="ServiceBusReceiver"/> whose <c>ReceiveMessageAsync</c> defaults to an empty (null) result, so a background poll loop idles harmlessly, and whose <c>CompleteMessageAsync</c> always succeeds.</summary>
    internal static ServiceBusReceiver CreateReceiverSubstitute()
    {
        var receiver = Substitute.For<ServiceBusReceiver>();
        receiver.ReceiveMessageAsync(Arg.Any<TimeSpan?>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<ServiceBusReceivedMessage?>(null));
        receiver.CompleteMessageAsync(Arg.Any<ServiceBusReceivedMessage>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);
        return receiver;
    }

    /// <summary>
    /// Fabricates a <see cref="ServiceBusReceivedMessage"/> with <paramref name="body"/> as its text
    /// body — <see cref="ServiceBusReceivedMessage"/> has no public constructor, so
    /// <see cref="ServiceBusModelFactory"/> is the only supported way to build one for tests.
    /// </summary>
    internal static ServiceBusReceivedMessage CreateReceivedMessage(string body)
        => ServiceBusModelFactory.ServiceBusReceivedMessage(body: BinaryData.FromString(body));

    /// <summary>
    /// Builds a substitute <see cref="ServiceBusClient"/> whose <c>CreateSender</c>/<c>CreateReceiver</c>
    /// calls are wired to caller-supplied (or freshly created) substitute senders/receivers, keyed by
    /// entity name/topic+subscription so repeated calls for the same entity are consistent within one
    /// test — the way a real client would return equivalent (if not literally the same) links.
    /// </summary>
    internal static ServiceBusClient CreateClientSubstitute(
        Func<string, ServiceBusSender>? senderFactory = null,
        Func<string, ServiceBusReceiver>? queueReceiverFactory = null,
        Func<string, string, ServiceBusReceiver>? subscriptionReceiverFactory = null)
    {
        senderFactory ??= _ => CreateSenderSubstitute();
        queueReceiverFactory ??= _ => CreateReceiverSubstitute();
        subscriptionReceiverFactory ??= (_, _) => CreateReceiverSubstitute();

        var client = Substitute.For<ServiceBusClient>();

        client.CreateSender(Arg.Any<string>())
            .Returns(callInfo => senderFactory((string)callInfo[0]));

        client.CreateReceiver(Arg.Any<string>())
            .Returns(callInfo => queueReceiverFactory((string)callInfo[0]));

        client.CreateReceiver(Arg.Any<string>(), Arg.Any<string>())
            .Returns(callInfo => subscriptionReceiverFactory((string)callInfo[0], (string)callInfo[1]));

        return client;
    }

    internal static Func<CancellationToken, Task<ServiceBusClient>> ClientFactoryReturning(ServiceBusClient client)
        => _ => Task.FromResult(client);

    internal static Func<CancellationToken, Task<ServiceBusAdministrationClient>> AdminFactoryReturning(ServiceBusAdministrationClient admin)
        => _ => Task.FromResult(admin);

    internal static async Task<AzureServiceBusClusterMessageBus> CreateBusAsync(
        ServiceBusClient? client = null,
        ServiceBusAdministrationClient? admin = null,
        IClusterChannelResolver? channelResolver = null,
        Uri? nodeEndpoint = null,
        TimeSpan? requestTimeout = null,
        TimeSpan? emptyPollDelay = null)
    {
        client ??= CreateClientSubstitute();
        admin ??= CreateAdminSubstitute();
        channelResolver ??= Substitute.For<IClusterChannelResolver>();

        var options = Options.Create(new AzureServiceBusClusterMessageBusOptions
        {
            ConnectionString = TestConnectionString,
            RequestTimeout = requestTimeout ?? TimeSpan.FromSeconds(30),
            EmptyPollDelay = emptyPollDelay ?? TimeSpan.FromMilliseconds(5),
        });

        var clusterConfiguration = new ClusterConfiguration { NodeEndpoint = nodeEndpoint ?? DefaultNodeEndpoint };

        var bus = new AzureServiceBusClusterMessageBus(
            options,
            clusterConfiguration,
            channelResolver,
            NullLoggerFactory.Instance,
            ClientFactoryReturning(client),
            AdminFactoryReturning(admin));

        await bus.EnsureInitializedAsync(CancellationToken.None);

        return bus;
    }
}
