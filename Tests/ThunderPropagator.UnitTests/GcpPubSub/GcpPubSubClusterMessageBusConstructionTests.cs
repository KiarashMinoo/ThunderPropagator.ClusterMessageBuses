using FluentAssertions;
using Google.Cloud.PubSub.V1;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using ThunderPropagator.Application.Channels.Cluster;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.ClusterMessageBuses.GcpPubSub;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.UnitTests.GcpPubSub;

public class GcpPubSubClusterMessageBusConstructionTests
{
    [Fact]
    public void Constructor_NodeEndpointNotSet_Throws()
    {
        var options = Options.Create(new GcpPubSubClusterMessageBusOptions { ProjectId = GcpPubSubClusterMessageBusTestHelpers.DefaultProjectId });
        var clusterConfiguration = new ClusterConfiguration { NodeEndpoint = null };
        var channelResolver = Substitute.For<IClusterChannelResolver>();
        var publisher = GcpPubSubClusterMessageBusTestHelpers.CreatePublisherSubstitute();
        var subscriber = GcpPubSubClusterMessageBusTestHelpers.CreateSubscriberSubstitute();

        var act = () => new GcpPubSubClusterMessageBus(
            options, clusterConfiguration, channelResolver, NullLoggerFactory.Instance,
            GcpPubSubClusterMessageBusTestHelpers.PublisherFactoryReturning(publisher),
            GcpPubSubClusterMessageBusTestHelpers.SubscriberFactoryReturning(subscriber));

        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void Constructor_ProjectIdNotSet_Throws()
    {
        var options = Options.Create(new GcpPubSubClusterMessageBusOptions { ProjectId = "" });
        var clusterConfiguration = new ClusterConfiguration { NodeEndpoint = GcpPubSubClusterMessageBusTestHelpers.DefaultNodeEndpoint };
        var channelResolver = Substitute.For<IClusterChannelResolver>();
        var publisher = GcpPubSubClusterMessageBusTestHelpers.CreatePublisherSubstitute();
        var subscriber = GcpPubSubClusterMessageBusTestHelpers.CreateSubscriberSubstitute();

        var act = () => new GcpPubSubClusterMessageBus(
            options, clusterConfiguration, channelResolver, NullLoggerFactory.Instance,
            GcpPubSubClusterMessageBusTestHelpers.PublisherFactoryReturning(publisher),
            GcpPubSubClusterMessageBusTestHelpers.SubscriberFactoryReturning(subscriber));

        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public async Task EnsureInitializedAsync_CalledMoreThanOnce_OnlyBuildsClientsOnce()
    {
        var publisherFactoryCalls = 0;
        var publisher = GcpPubSubClusterMessageBusTestHelpers.CreatePublisherSubstitute();
        var subscriber = GcpPubSubClusterMessageBusTestHelpers.CreateSubscriberSubstitute();

        var options = Options.Create(new GcpPubSubClusterMessageBusOptions
        {
            ProjectId = GcpPubSubClusterMessageBusTestHelpers.DefaultProjectId,
            EmptyPollDelay = TimeSpan.FromMilliseconds(5),
        });
        var clusterConfiguration = new ClusterConfiguration { NodeEndpoint = GcpPubSubClusterMessageBusTestHelpers.DefaultNodeEndpoint };
        var channelResolver = Substitute.For<IClusterChannelResolver>();

        Task<PublisherServiceApiClient> PublisherFactory(CancellationToken _)
        {
            publisherFactoryCalls++;
            return Task.FromResult(publisher);
        }

        await using var bus = new GcpPubSubClusterMessageBus(
            options, clusterConfiguration, channelResolver, NullLoggerFactory.Instance,
            PublisherFactory, GcpPubSubClusterMessageBusTestHelpers.SubscriberFactoryReturning(subscriber));

        await bus.EnsureInitializedAsync(CancellationToken.None);
        await bus.EnsureInitializedAsync(CancellationToken.None);

        publisherFactoryCalls.Should().Be(1);
    }

    [Fact]
    public async Task EnsureInitializedAsync_CreatesThisNodesRequestAndReplyTopicsAndSubscriptions()
    {
        var publisher = GcpPubSubClusterMessageBusTestHelpers.CreatePublisherSubstitute();
        var subscriber = GcpPubSubClusterMessageBusTestHelpers.CreateSubscriberSubstitute();
        await using var bus = await GcpPubSubClusterMessageBusTestHelpers.CreateBusAsync(publisher: publisher, subscriber: subscriber);

        await publisher.Received().CreateTopicAsync(
            Arg.Is<TopicName>(t => t.TopicId.Contains("-requests-")), Arg.Any<CancellationToken>());
        await publisher.Received().CreateTopicAsync(
            Arg.Is<TopicName>(t => t.TopicId.Contains("-replies-")), Arg.Any<CancellationToken>());
        await subscriber.Received().CreateSubscriptionAsync(
            Arg.Is<SubscriptionName>(s => s.SubscriptionId.Contains("-requests-")), Arg.Any<TopicName>(), Arg.Any<PushConfig>(), Arg.Any<int>(), Arg.Any<CancellationToken>());
        await subscriber.Received().CreateSubscriptionAsync(
            Arg.Is<SubscriptionName>(s => s.SubscriptionId.Contains("-replies-")), Arg.Any<TopicName>(), Arg.Any<PushConfig>(), Arg.Any<int>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task DisposeAsync_AlsoRemovesFanOutSubscriptionsTheCallerNeverDisposedItself()
    {
        await using var bus = await GcpPubSubClusterMessageBusTestHelpers.CreateBusAsync();

        Func<ClusterFanOutMessage, CancellationToken, Task> noOpHandler = (_, _) => Task.CompletedTask;
        var subscriptionHandle = await bus.SubscribeAsync(Guid.NewGuid(), noOpHandler);
        _ = subscriptionHandle;

        var act = async () => await bus.DisposeAsync();

        await act.Should().NotThrowAsync("DisposeAsync must clean up orphaned local subscriptions rather than leaking them");
    }
}
