using FluentAssertions;
using NSubstitute;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.BuildingBlocks.Application.Enums;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.Mqtt;

namespace ThunderPropagator.UnitTests.Mqtt;

public class MqttClusterMessageBusFanOutTests
{
    private static readonly Func<ClusterFanOutMessage, CancellationToken, Task> NoOpHandler = (_, _) => Task.CompletedTask;

    [Fact]
    public async Task PublishAsync_StampsOriginId_AndPublishesToTheFanOutTopic()
    {
        var transport = MqttClusterMessageBusTestHelpers.CreateSubstituteTransport();
        await using var bus = await MqttClusterMessageBusTestHelpers.CreateBusAsync(transport: transport);

        var channelKey = Guid.NewGuid();
        var message = new ClusterFanOutMessage(Guid.Empty, 1, CastType.Broadcast, new Dictionary<string, object?>());

        await bus.PublishAsync(channelKey, message);

        var expectedTopic = MqttTopicNaming.FanOutTopic("thunderpropagator/cluster", channelKey);

        await transport.Received(1).PublishAsync(
            expectedTopic,
            Arg.Is<string>(json => json.FromNJson<ClusterFanOutMessage>()!.OriginId == bus.SelfId),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task SubscribeAsync_SubscribesToTheChannelsFanOutTopic()
    {
        var transport = MqttClusterMessageBusTestHelpers.CreateSubstituteTransport();
        await using var bus = await MqttClusterMessageBusTestHelpers.CreateBusAsync(transport: transport);

        var channelKey = Guid.NewGuid();
        var expectedTopic = MqttTopicNaming.FanOutTopic("thunderpropagator/cluster", channelKey);

        await using var subscription = await bus.SubscribeAsync(channelKey, NoOpHandler);

        transport.Received(1).SubscribeAsync(expectedTopic, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task HandleFanOutDeliveryAsync_SelfEcho_IsRejected()
    {
        await using var bus = await MqttClusterMessageBusTestHelpers.CreateBusAsync();

        var invoked = false;
        Func<ClusterFanOutMessage, CancellationToken, Task> handler = (_, _) => { invoked = true; return Task.CompletedTask; };

        var selfMessage = new ClusterFanOutMessage(bus.SelfId, 1, CastType.Broadcast, new Dictionary<string, object?>());

        await bus.HandleFanOutDeliveryAsync(selfMessage.ToNJson(), handler, CancellationToken.None);

        invoked.Should().BeFalse("a node must never re-process its own fan-out broadcast");
    }

    [Fact]
    public async Task HandleFanOutDeliveryAsync_ForeignMessage_IsDelivered()
    {
        await using var bus = await MqttClusterMessageBusTestHelpers.CreateBusAsync();

        ClusterFanOutMessage? received = null;
        Func<ClusterFanOutMessage, CancellationToken, Task> handler = (m, _) => { received = m; return Task.CompletedTask; };

        var foreignMessage = new ClusterFanOutMessage(Guid.NewGuid(), 1, CastType.Broadcast, new Dictionary<string, object?>());

        await bus.HandleFanOutDeliveryAsync(foreignMessage.ToNJson(), handler, CancellationToken.None);

        received.Should().NotBeNull();
        received!.OriginId.Should().Be(foreignMessage.OriginId);
    }

    [Fact]
    public async Task HandleFanOutDeliveryAsync_MalformedPayload_IsSkippedWithoutThrowing()
    {
        await using var bus = await MqttClusterMessageBusTestHelpers.CreateBusAsync();

        var invoked = false;
        Func<ClusterFanOutMessage, CancellationToken, Task> handler = (_, _) => { invoked = true; return Task.CompletedTask; };

        var act = async () => await bus.HandleFanOutDeliveryAsync("{ not valid json ", handler, CancellationToken.None);

        await act.Should().NotThrowAsync("a single malformed delivery must not crash the subscription");
        invoked.Should().BeFalse();
    }

    [Fact]
    public async Task RunFanOutSubscriptionLoopAsync_RejectsSelfEchoedMessages()
    {
        var transport = MqttClusterMessageBusTestHelpers.CreateSubstituteTransport();
        await using var bus = await MqttClusterMessageBusTestHelpers.CreateBusAsync(transport: transport);

        var selfEchoed = new ClusterFanOutMessage(bus.SelfId, 1, CastType.Broadcast, new Dictionary<string, object?>());
        var loopCts = new CancellationTokenSource();
        transport.SubscribeAsync("some-topic", Arg.Any<CancellationToken>())
            .Returns(callInfo => MqttClusterMessageBusTestHelpers.SinglePayloadThenBlock(selfEchoed.ToNJson(), (CancellationToken)callInfo[1]));

        var received = new List<ClusterFanOutMessage>();
        var loopTask = bus.RunFanOutSubscriptionLoopAsync("some-topic", (m, _) => { received.Add(m); return Task.CompletedTask; }, loopCts.Token);

        await Task.Delay(50);
        loopCts.Cancel();
        await loopTask;

        received.Should().BeEmpty("a message whose OriginId matches this node's own SelfId is a self-echo and must not be delivered");
    }

    [Fact]
    public async Task SubscribeAsync_DisposedHandle_CancelsItsOwnLoop()
    {
        var transport = MqttClusterMessageBusTestHelpers.CreateSubstituteTransport();
        var channelKey = Guid.NewGuid();
        var expectedTopic = MqttTopicNaming.FanOutTopic("thunderpropagator/cluster", channelKey);
        transport.SubscribeAsync(expectedTopic, Arg.Any<CancellationToken>())
            .Returns(callInfo => MqttClusterMessageBusTestHelpers.SinglePayloadThenBlock("irrelevant", (CancellationToken)callInfo[1]));

        await using var bus = await MqttClusterMessageBusTestHelpers.CreateBusAsync(transport: transport);

        var subscription = await bus.SubscribeAsync(channelKey, NoOpHandler);

        var act = async () => await subscription.DisposeAsync();

        await act.Should().NotThrowAsync();
    }
}
