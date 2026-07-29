using FluentAssertions;
using NSubstitute;
using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.Mqtt;

namespace ThunderPropagator.UnitTests.Mqtt;

public class MqttClusterMessageBusSubscriptionSyncTests
{
    private static readonly Func<ClusterSubscriptionEvent, CancellationToken, Task> NoOpHandler = (_, _) => Task.CompletedTask;

    private static ClusterSubscriptionEvent CreateEvent(Guid originId) => new(
        originId,
        "https://node1:5000/",
        ClusterSubscriptionEventKind.Added,
        [new ClusterSubscriptionDescriptor("sub-1", "req-1", "conn-1")],
        DateTimeOffset.UtcNow);

    [Fact]
    public async Task PublishAsync_StampsOriginId_AndPublishesToTheSubscriptionEventTopic()
    {
        var transport = MqttClusterMessageBusTestHelpers.CreateSubstituteTransport();
        await using var bus = await MqttClusterMessageBusTestHelpers.CreateBusAsync(transport: transport);

        var channelKey = Guid.NewGuid();
        var subscriptionEvent = CreateEvent(Guid.Empty);

        await bus.PublishAsync(channelKey, subscriptionEvent);

        var expectedTopic = MqttTopicNaming.SubscriptionEventTopic("thunderpropagator/cluster", channelKey);

        await transport.Received(1).PublishAsync(
            expectedTopic,
            Arg.Is<string>(json => json.FromNJson<ClusterSubscriptionEvent>()!.OriginId == bus.SelfId),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task SubscribeAsync_SubscribesToTheChannelsSubscriptionEventTopic()
    {
        var transport = MqttClusterMessageBusTestHelpers.CreateSubstituteTransport();
        await using var bus = await MqttClusterMessageBusTestHelpers.CreateBusAsync(transport: transport);

        var channelKey = Guid.NewGuid();
        var expectedTopic = MqttTopicNaming.SubscriptionEventTopic("thunderpropagator/cluster", channelKey);

        await using var subscription = await bus.SubscribeAsync(channelKey, NoOpHandler);

        transport.Received(1).SubscribeAsync(expectedTopic, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task HandleSubscriptionEventDeliveryAsync_SelfEcho_IsRejected()
    {
        await using var bus = await MqttClusterMessageBusTestHelpers.CreateBusAsync();

        var invoked = false;
        Func<ClusterSubscriptionEvent, CancellationToken, Task> handler = (_, _) => { invoked = true; return Task.CompletedTask; };

        await bus.HandleSubscriptionEventDeliveryAsync(CreateEvent(bus.SelfId).ToNJson(), handler, CancellationToken.None);

        invoked.Should().BeFalse("a node must never re-process its own subscription-event broadcast");
    }

    [Fact]
    public async Task HandleSubscriptionEventDeliveryAsync_ForeignEvent_IsDelivered()
    {
        await using var bus = await MqttClusterMessageBusTestHelpers.CreateBusAsync();

        ClusterSubscriptionEvent? received = null;
        Func<ClusterSubscriptionEvent, CancellationToken, Task> handler = (e, _) => { received = e; return Task.CompletedTask; };

        var foreignEvent = CreateEvent(Guid.NewGuid());

        await bus.HandleSubscriptionEventDeliveryAsync(foreignEvent.ToNJson(), handler, CancellationToken.None);

        received.Should().NotBeNull();
        received!.OriginId.Should().Be(foreignEvent.OriginId);
    }

    [Fact]
    public async Task HandleSubscriptionEventDeliveryAsync_MalformedPayload_IsSkippedWithoutThrowing()
    {
        await using var bus = await MqttClusterMessageBusTestHelpers.CreateBusAsync();

        var invoked = false;
        Func<ClusterSubscriptionEvent, CancellationToken, Task> handler = (_, _) => { invoked = true; return Task.CompletedTask; };

        var act = async () => await bus.HandleSubscriptionEventDeliveryAsync("not json at all", handler, CancellationToken.None);

        await act.Should().NotThrowAsync("a single malformed delivery must not crash the subscription");
        invoked.Should().BeFalse();
    }

    [Fact]
    public async Task SubscribeAsync_DisposedHandle_CancelsItsOwnLoop()
    {
        var transport = MqttClusterMessageBusTestHelpers.CreateSubstituteTransport();
        var channelKey = Guid.NewGuid();
        var expectedTopic = MqttTopicNaming.SubscriptionEventTopic("thunderpropagator/cluster", channelKey);
        transport.SubscribeAsync(expectedTopic, Arg.Any<CancellationToken>())
            .Returns(callInfo => MqttClusterMessageBusTestHelpers.SinglePayloadThenBlock("irrelevant", (CancellationToken)callInfo[1]));

        await using var bus = await MqttClusterMessageBusTestHelpers.CreateBusAsync(transport: transport);

        var subscription = await bus.SubscribeAsync(channelKey, NoOpHandler);

        var act = async () => await subscription.DisposeAsync();

        await act.Should().NotThrowAsync();
    }
}
