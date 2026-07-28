using Google.Cloud.PubSub.V1;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using ThunderPropagator.Application.Channels.Cluster;
using ThunderPropagator.ClusterMessageBuses.GcpPubSub;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.UnitTests.GcpPubSub;

/// <summary>
/// Shared construction helpers for <see cref="GcpPubSubClusterMessageBus"/> tests. Every test
/// constructs the bus through the same seams the production DI extension uses
/// (<c>IOptions&lt;GcpPubSubClusterMessageBusOptions&gt;</c>, <c>ClusterConfiguration</c>,
/// <c>IClusterChannelResolver</c>, client-factory delegates) so no test ever needs a live GCP
/// project: <see cref="PublisherServiceApiClient"/> and <see cref="SubscriberServiceApiClient"/>
/// are GAPIC-generated with a protected parameterless constructor and virtual members specifically
/// designed for direct substitution — no wrapper needed, same approach as
/// <c>Amazon.SQS.IAmazonSQS</c>/<c>IAmazonSimpleNotificationService</c> elsewhere in this repo.
/// </summary>
internal static class GcpPubSubClusterMessageBusTestHelpers
{
    internal static readonly Uri DefaultNodeEndpoint = new("https://node1:5000/");
    internal const string DefaultProjectId = "tp-test-project";

    /// <summary>
    /// Builds a substitute <see cref="PublisherServiceApiClient"/> whose <c>CreateTopicAsync</c> and
    /// <c>PublishAsync</c> both succeed trivially.
    /// </summary>
    internal static PublisherServiceApiClient CreatePublisherSubstitute()
    {
        var publisher = Substitute.For<PublisherServiceApiClient>();

        publisher.CreateTopicAsync(Arg.Any<TopicName>(), Arg.Any<CancellationToken>())
            .Returns(callInfo => Task.FromResult(new Topic { TopicName = (TopicName)callInfo[0] }));

        publisher.PublishAsync(Arg.Any<TopicName>(), Arg.Any<IEnumerable<PubsubMessage>>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new PublishResponse()));

        return publisher;
    }

    /// <summary>
    /// Builds a substitute <see cref="SubscriberServiceApiClient"/> whose <c>CreateSubscriptionAsync</c>,
    /// <c>AcknowledgeAsync</c>, and <c>DeleteSubscriptionAsync</c> all succeed trivially, and whose
    /// <c>PullAsync</c> defaults to an empty result (no messages) so the background poll loop idles
    /// harmlessly.
    /// </summary>
    internal static SubscriberServiceApiClient CreateSubscriberSubstitute()
    {
        var subscriber = Substitute.For<SubscriberServiceApiClient>();

        subscriber.CreateSubscriptionAsync(Arg.Any<SubscriptionName>(), Arg.Any<TopicName>(), Arg.Any<PushConfig>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(callInfo => Task.FromResult(new Subscription
            {
                SubscriptionName = (SubscriptionName)callInfo[0],
                TopicAsTopicName = (TopicName)callInfo[1],
            }));

        subscriber.PullAsync(Arg.Any<SubscriptionName>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new PullResponse()));

        subscriber.AcknowledgeAsync(Arg.Any<SubscriptionName>(), Arg.Any<IEnumerable<string>>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        subscriber.DeleteSubscriptionAsync(Arg.Any<SubscriptionName>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        return subscriber;
    }

    internal static Func<CancellationToken, Task<PublisherServiceApiClient>> PublisherFactoryReturning(PublisherServiceApiClient publisher)
        => _ => Task.FromResult(publisher);

    internal static Func<CancellationToken, Task<SubscriberServiceApiClient>> SubscriberFactoryReturning(SubscriberServiceApiClient subscriber)
        => _ => Task.FromResult(subscriber);

    internal static async Task<GcpPubSubClusterMessageBus> CreateBusAsync(
        PublisherServiceApiClient? publisher = null,
        SubscriberServiceApiClient? subscriber = null,
        IClusterChannelResolver? channelResolver = null,
        Uri? nodeEndpoint = null,
        TimeSpan? requestTimeout = null,
        TimeSpan? emptyPollDelay = null)
    {
        publisher ??= CreatePublisherSubstitute();
        subscriber ??= CreateSubscriberSubstitute();
        channelResolver ??= Substitute.For<IClusterChannelResolver>();

        var options = Options.Create(new GcpPubSubClusterMessageBusOptions
        {
            ProjectId = DefaultProjectId,
            RequestTimeout = requestTimeout ?? TimeSpan.FromSeconds(30),
            EmptyPollDelay = emptyPollDelay ?? TimeSpan.FromMilliseconds(5),
        });

        var clusterConfiguration = new ClusterConfiguration { NodeEndpoint = nodeEndpoint ?? DefaultNodeEndpoint };

        var bus = new GcpPubSubClusterMessageBus(
            options,
            clusterConfiguration,
            channelResolver,
            NullLoggerFactory.Instance,
            PublisherFactoryReturning(publisher),
            SubscriberFactoryReturning(subscriber));

        await bus.EnsureInitializedAsync(CancellationToken.None);

        return bus;
    }
}
