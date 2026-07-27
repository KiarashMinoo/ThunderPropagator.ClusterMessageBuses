using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using ThunderPropagator.Application.Channels.Cluster;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.ClusterMessageBuses.AwsSqs;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.UnitTests.AwsSqs;

public class AwsSqsClusterMessageBusConstructionTests
{
    [Fact]
    public void Constructor_NodeEndpointNotSet_Throws()
    {
        var options = Options.Create(new AwsSqsClusterMessageBusOptions());
        var clusterConfiguration = new ClusterConfiguration { NodeEndpoint = null };
        var channelResolver = Substitute.For<IClusterChannelResolver>();
        var sqs = AwsSqsClusterMessageBusTestHelpers.CreateSqsSubstitute();
        var sns = AwsSqsClusterMessageBusTestHelpers.CreateSnsSubstitute();

        var act = () => new AwsSqsClusterMessageBus(
            options, clusterConfiguration, channelResolver, NullLoggerFactory.Instance,
            AwsSqsClusterMessageBusTestHelpers.SqsFactoryReturning(sqs),
            AwsSqsClusterMessageBusTestHelpers.SnsFactoryReturning(sns));

        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public async Task EnsureInitializedAsync_CalledMoreThanOnce_OnlyBuildsClientsOnce()
    {
        var sqsFactoryCalls = 0;
        var sqs = AwsSqsClusterMessageBusTestHelpers.CreateSqsSubstitute();
        var sns = AwsSqsClusterMessageBusTestHelpers.CreateSnsSubstitute();

        var options = Options.Create(new AwsSqsClusterMessageBusOptions { EmptyPollDelay = TimeSpan.FromMilliseconds(5) });
        var clusterConfiguration = new ClusterConfiguration { NodeEndpoint = AwsSqsClusterMessageBusTestHelpers.DefaultNodeEndpoint };
        var channelResolver = Substitute.For<IClusterChannelResolver>();

        Task<Amazon.SQS.IAmazonSQS> SqsFactory(CancellationToken _)
        {
            sqsFactoryCalls++;
            return Task.FromResult(sqs);
        }

        await using var bus = new AwsSqsClusterMessageBus(
            options, clusterConfiguration, channelResolver, NullLoggerFactory.Instance,
            SqsFactory, AwsSqsClusterMessageBusTestHelpers.SnsFactoryReturning(sns));

        await bus.EnsureInitializedAsync(CancellationToken.None);
        await bus.EnsureInitializedAsync(CancellationToken.None);

        sqsFactoryCalls.Should().Be(1);
    }

    [Fact]
    public async Task EnsureInitializedAsync_CreatesThisNodesRequestAndReplyQueues()
    {
        var sqs = AwsSqsClusterMessageBusTestHelpers.CreateSqsSubstitute();
        await using var bus = await AwsSqsClusterMessageBusTestHelpers.CreateBusAsync(sqs: sqs);

        await sqs.Received().CreateQueueAsync(
            Arg.Is<Amazon.SQS.Model.CreateQueueRequest>(r => r.QueueName.Contains("-requests-")),
            Arg.Any<CancellationToken>());
        await sqs.Received().CreateQueueAsync(
            Arg.Is<Amazon.SQS.Model.CreateQueueRequest>(r => r.QueueName.Contains("-replies-")),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task DisposeAsync_DisposesBothClients()
    {
        var sqs = AwsSqsClusterMessageBusTestHelpers.CreateSqsSubstitute();
        var sns = AwsSqsClusterMessageBusTestHelpers.CreateSnsSubstitute();

        var bus = await AwsSqsClusterMessageBusTestHelpers.CreateBusAsync(sqs: sqs, sns: sns);
        await bus.DisposeAsync();

        sqs.Received(1).Dispose();
        sns.Received(1).Dispose();
    }

    [Fact]
    public async Task DisposeAsync_AlsoRemovesFanOutSubscriptionsTheCallerNeverDisposedItself()
    {
        await using var bus = await AwsSqsClusterMessageBusTestHelpers.CreateBusAsync();

        Func<ClusterFanOutMessage, CancellationToken, Task> noOpHandler = (_, _) => Task.CompletedTask;
        var subscriptionHandle = await bus.SubscribeAsync(Guid.NewGuid(), noOpHandler);
        _ = subscriptionHandle;

        var act = async () => await bus.DisposeAsync();

        await act.Should().NotThrowAsync("DisposeAsync must clean up orphaned local subscriptions rather than leaking them");
    }
}
