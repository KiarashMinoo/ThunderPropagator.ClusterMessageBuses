using FluentAssertions;
using NSubstitute;
using ThunderPropagator.Application.Channels.Cluster.MessageBus;
using ThunderPropagator.BuildingBlocks.Application.Enums;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.NATS;

namespace ThunderPropagator.UnitTests.NATS;

public class NatsClusterMessageBusFanOutTests
{
    private static readonly Func<ClusterFanOutMessage, CancellationToken, Task> NoOpHandler = (_, _) => Task.CompletedTask;

    [Fact]
    public async Task PublishAsync_StampsOriginId_AndPublishesToTheFanOutSubject()
    {
        var transport = NatsClusterMessageBusTestHelpers.CreateSubstituteTransport();
        await using var bus = await NatsClusterMessageBusTestHelpers.CreateBusAsync(transport: transport);

        var channelKey = Guid.NewGuid();
        var message = new ClusterFanOutMessage(Guid.Empty, 1, CastType.Broadcast, new Dictionary<string, object?>());

        await bus.PublishAsync(channelKey, message);

        var expectedSubject = NatsSubjectNaming.FanOutSubject("thunderpropagator.cluster", channelKey);

        await transport.Received(1).PublishAsync(
            expectedSubject,
            Arg.Is<string>(json => json.FromNJson<ClusterFanOutMessage>()!.OriginId == bus.SelfId),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task SubscribeAsync_SubscribesToTheChannelsFanOutSubject()
    {
        var transport = NatsClusterMessageBusTestHelpers.CreateSubstituteTransport();
        await using var bus = await NatsClusterMessageBusTestHelpers.CreateBusAsync(transport: transport);

        var channelKey = Guid.NewGuid();
        var expectedSubject = NatsSubjectNaming.FanOutSubject("thunderpropagator.cluster", channelKey);

        await using var subscription = await bus.SubscribeAsync(channelKey, NoOpHandler);

        await transport.Received(1).SubscribeAsync(expectedSubject, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task HandleFanOutDeliveryAsync_SelfEcho_IsRejected()
    {
        await using var bus = await NatsClusterMessageBusTestHelpers.CreateBusAsync();

        var invoked = false;
        Func<ClusterFanOutMessage, CancellationToken, Task> handler = (_, _) => { invoked = true; return Task.CompletedTask; };

        var selfMessage = new ClusterFanOutMessage(bus.SelfId, 1, CastType.Broadcast, new Dictionary<string, object?>());

        await bus.HandleFanOutDeliveryAsync(selfMessage.ToNJson(), handler, CancellationToken.None);

        invoked.Should().BeFalse("a node must never re-process its own fan-out broadcast");
    }

    [Fact]
    public async Task HandleFanOutDeliveryAsync_ForeignMessage_IsDelivered()
    {
        await using var bus = await NatsClusterMessageBusTestHelpers.CreateBusAsync();

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
        await using var bus = await NatsClusterMessageBusTestHelpers.CreateBusAsync();

        var invoked = false;
        Func<ClusterFanOutMessage, CancellationToken, Task> handler = (_, _) => { invoked = true; return Task.CompletedTask; };

        var act = async () => await bus.HandleFanOutDeliveryAsync("{ not valid json ", handler, CancellationToken.None);

        await act.Should().NotThrowAsync("a single malformed delivery must not crash the subscription");
        invoked.Should().BeFalse();
    }

    [Fact]
    public async Task RunFanOutSubscriptionLoopAsync_RejectsSelfEchoedMessages()
    {
        var transport = NatsClusterMessageBusTestHelpers.CreateSubstituteTransport();
        await using var bus = await NatsClusterMessageBusTestHelpers.CreateBusAsync(transport: transport);

        var selfEchoed = new ClusterFanOutMessage(bus.SelfId, 1, CastType.Broadcast, new Dictionary<string, object?>());
        var loopCts = new CancellationTokenSource();
        transport.SubscribeAsync("some-subject", Arg.Any<CancellationToken>())
            .Returns(callInfo => NatsClusterMessageBusTestHelpers.SingleDeliveryThenBlock(
                new NatsClusterDelivery(selfEchoed.ToNJson(), null), (CancellationToken)callInfo[1]));

        var received = new List<ClusterFanOutMessage>();
        var loopTask = bus.RunFanOutSubscriptionLoopAsync("some-subject", (m, _) => { received.Add(m); return Task.CompletedTask; }, loopCts.Token);

        // Give the loop a moment to receive and process the single queued delivery before tearing
        // it down (SingleDeliveryThenBlock blocks forever after yielding its one item, so the loop
        // never completes on its own).
        await Task.Delay(50);
        loopCts.Cancel();
        await loopTask;

        received.Should().BeEmpty("a message whose OriginId matches this node's own SelfId is a self-echo and must not be delivered");
    }

    [Fact]
    public async Task SubscribeAsync_DisposedHandle_CancelsItsOwnLoop()
    {
        var transport = NatsClusterMessageBusTestHelpers.CreateSubstituteTransport();
        var channelKey = Guid.NewGuid();
        var expectedSubject = NatsSubjectNaming.FanOutSubject("thunderpropagator.cluster", channelKey);
        transport.SubscribeAsync(expectedSubject, Arg.Any<CancellationToken>())
            .Returns(callInfo => NatsClusterMessageBusTestHelpers.SingleDeliveryThenBlock(
                new NatsClusterDelivery("irrelevant", null), (CancellationToken)callInfo[1]));

        await using var bus = await NatsClusterMessageBusTestHelpers.CreateBusAsync(transport: transport);

        var subscription = await bus.SubscribeAsync(channelKey, NoOpHandler);

        var act = async () => await subscription.DisposeAsync();

        await act.Should().NotThrowAsync();
    }
}
