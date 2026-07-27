using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using ThunderPropagator.Application.Channels.Cluster;
using ThunderPropagator.ClusterMessageBuses.AwsSqs;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.UnitTests.AwsSqs;

/// <summary>
/// Shared construction helpers for <see cref="AwsSqsClusterMessageBus"/> tests. Every test
/// constructs the bus through the same seams the production DI extension uses
/// (<c>IOptions&lt;AwsSqsClusterMessageBusOptions&gt;</c>, <c>ClusterConfiguration</c>,
/// <c>IClusterChannelResolver</c>, client-factory delegates) so no test ever needs live AWS
/// resources: <see cref="IAmazonSQS"/> and <see cref="IAmazonSimpleNotificationService"/> are both
/// plain public interfaces, substituted directly with NSubstitute — no wrapper needed, same
/// approach as Confluent.Kafka's <c>IConsumer</c>/<c>IProducer</c> and RabbitMQ.Client's
/// <c>IConnection</c>/<c>IChannel</c> elsewhere in this repo.
/// </summary>
internal static class AwsSqsClusterMessageBusTestHelpers
{
    internal static readonly Uri DefaultNodeEndpoint = new("https://node1:5000/");

    private const string AccountPrefix = "https://sqs.test.local/000000000000/";
    private const string QueueArnPrefix = "arn:aws:sqs:us-east-1:000000000000:";
    private const string TopicArnPrefix = "arn:aws:sns:us-east-1:000000000000:";

    internal static string QueueUrlFor(string queueName) => AccountPrefix + queueName;
    internal static string QueueArnFor(string queueName) => QueueArnPrefix + queueName;
    internal static string TopicArnFor(string topicName) => TopicArnPrefix + topicName;

    private static string QueueNameFromUrl(string queueUrl) => queueUrl[AccountPrefix.Length..];

    /// <summary>
    /// Builds a substitute <see cref="IAmazonSQS"/> whose queue-lifecycle calls (<c>CreateQueueAsync</c>,
    /// <c>GetQueueUrlAsync</c>, <c>GetQueueAttributesAsync</c>) are wired together by a deterministic
    /// name-based URL/ARN scheme, so a test can resolve "another node's" queue via
    /// <c>GetQueueUrlAsync</c> without that node having actually created it first — exactly like
    /// real AWS resolves any queue by name regardless of which process created it. <c>ReceiveMessageAsync</c>
    /// defaults to an empty result (no messages) so the background poll loop idles harmlessly.
    /// </summary>
    internal static IAmazonSQS CreateSqsSubstitute()
    {
        var sqs = Substitute.For<IAmazonSQS>();

        sqs.CreateQueueAsync(Arg.Any<CreateQueueRequest>(), Arg.Any<CancellationToken>())
            .Returns(callInfo => Task.FromResult(new CreateQueueResponse { QueueUrl = QueueUrlFor(((CreateQueueRequest)callInfo[0]).QueueName) }));

        sqs.GetQueueUrlAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(callInfo => Task.FromResult(new GetQueueUrlResponse { QueueUrl = QueueUrlFor((string)callInfo[0]) }));

        sqs.GetQueueAttributesAsync(Arg.Any<GetQueueAttributesRequest>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var request = (GetQueueAttributesRequest)callInfo[0];
                var queueName = QueueNameFromUrl(request.QueueUrl);
                return Task.FromResult(new GetQueueAttributesResponse
                {
                    Attributes = new Dictionary<string, string> { ["QueueArn"] = QueueArnFor(queueName) },
                });
            });

        sqs.SetQueueAttributesAsync(Arg.Any<SetQueueAttributesRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new SetQueueAttributesResponse()));

        sqs.SendMessageAsync(Arg.Any<SendMessageRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new SendMessageResponse()));

        sqs.ReceiveMessageAsync(Arg.Any<ReceiveMessageRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new ReceiveMessageResponse { Messages = [] }));

        sqs.DeleteMessageAsync(Arg.Any<DeleteMessageRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new DeleteMessageResponse()));

        sqs.DeleteQueueAsync(Arg.Any<DeleteQueueRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new DeleteQueueResponse()));

        return sqs;
    }

    /// <summary>
    /// Builds a substitute <see cref="IAmazonSimpleNotificationService"/> whose <c>CreateTopicAsync</c>
    /// returns a deterministic ARN derived from the topic name, and whose <c>SubscribeAsync</c>/
    /// <c>UnsubscribeAsync</c>/<c>PublishAsync</c> all succeed trivially.
    /// </summary>
    internal static IAmazonSimpleNotificationService CreateSnsSubstitute()
    {
        var sns = Substitute.For<IAmazonSimpleNotificationService>();

        sns.CreateTopicAsync(Arg.Any<CreateTopicRequest>(), Arg.Any<CancellationToken>())
            .Returns(callInfo => Task.FromResult(new CreateTopicResponse { TopicArn = TopicArnFor(((CreateTopicRequest)callInfo[0]).Name) }));

        sns.SubscribeAsync(Arg.Any<SubscribeRequest>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(new SubscribeResponse { SubscriptionArn = $"arn:aws:sns:us-east-1:000000000000:sub-{Guid.NewGuid():N}" }));

        sns.UnsubscribeAsync(Arg.Any<UnsubscribeRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new UnsubscribeResponse()));

        sns.PublishAsync(Arg.Any<PublishRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new PublishResponse()));

        return sns;
    }

    internal static Func<CancellationToken, Task<IAmazonSQS>> SqsFactoryReturning(IAmazonSQS sqs)
        => _ => Task.FromResult(sqs);

    internal static Func<CancellationToken, Task<IAmazonSimpleNotificationService>> SnsFactoryReturning(IAmazonSimpleNotificationService sns)
        => _ => Task.FromResult(sns);

    internal static async Task<AwsSqsClusterMessageBus> CreateBusAsync(
        IAmazonSQS? sqs = null,
        IAmazonSimpleNotificationService? sns = null,
        IClusterChannelResolver? channelResolver = null,
        Uri? nodeEndpoint = null,
        TimeSpan? requestTimeout = null,
        TimeSpan? emptyPollDelay = null)
    {
        sqs ??= CreateSqsSubstitute();
        sns ??= CreateSnsSubstitute();
        channelResolver ??= Substitute.For<IClusterChannelResolver>();

        var options = Options.Create(new AwsSqsClusterMessageBusOptions
        {
            RequestTimeout = requestTimeout ?? TimeSpan.FromSeconds(30),
            EmptyPollDelay = emptyPollDelay ?? TimeSpan.FromMilliseconds(5),
        });

        var clusterConfiguration = new ClusterConfiguration { NodeEndpoint = nodeEndpoint ?? DefaultNodeEndpoint };

        var bus = new AwsSqsClusterMessageBus(
            options,
            clusterConfiguration,
            channelResolver,
            NullLoggerFactory.Instance,
            SqsFactoryReturning(sqs),
            SnsFactoryReturning(sns));

        await bus.EnsureInitializedAsync(CancellationToken.None);

        return bus;
    }
}
