using Confluent.Kafka;
using FluentAssertions;
using NSubstitute;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.Kafka;

namespace ThunderPropagator.UnitTests.Kafka;

public class KafkaClusterMessageBusRequestReplyTests
{
    private static IProducer<string, string> CreateSucceedingProducer(Action<KafkaClusterRequestEnvelope>? onProduced = null)
    {
        var producer = Substitute.For<IProducer<string, string>>();
        producer.ProduceAsync(
                Arg.Any<string>(),
                Arg.Do<Message<string, string>>(m => onProduced?.Invoke(m.Value.FromNJson<KafkaClusterRequestEnvelope>()!)),
                Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<DeliveryResult<string, string>>(null!));
        return producer;
    }

    [Fact]
    public async Task SendRequestAsync_NoReplyArrives_ThrowsTimeoutExceptionAfterConfiguredTimeout()
    {
        var producer = CreateSucceedingProducer();
        await using var bus = KafkaClusterMessageBusTestHelpers.CreateBus(producer: producer, requestTimeout: TimeSpan.FromMilliseconds(100));

        var act = () => bus.SendRequestAsync(
            new Uri("https://leader:5000/"), KafkaClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null, CancellationToken.None);

        await act.Should().ThrowAsync<TimeoutException>();
    }

    [Fact]
    public async Task SendRequestAsync_CallerCancelsBeforeTimeout_PropagatesOperationCanceledExceptionNotTimeoutException()
    {
        var producer = CreateSucceedingProducer();
        await using var bus = KafkaClusterMessageBusTestHelpers.CreateBus(producer: producer, requestTimeout: TimeSpan.FromSeconds(30));

        using var callerCts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));

        var act = () => bus.SendRequestAsync(
            new Uri("https://leader:5000/"), KafkaClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null, callerCts.Token);

        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task SendRequestAsync_MatchingReplyArrives_ReturnsIt()
    {
        KafkaClusterRequestEnvelope? sentRequest = null;
        var producer = CreateSucceedingProducer(request => sentRequest = request);
        await using var bus = KafkaClusterMessageBusTestHelpers.CreateBus(producer: producer, requestTimeout: TimeSpan.FromSeconds(5));

        var sendTask = bus.SendRequestAsync(
            new Uri("https://leader:5000/"), KafkaClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null, CancellationToken.None);

        // ProduceAsync (and therefore the Arg.Do capture) runs synchronously inside SendRequestAsync
        // before it ever awaits the reply, so by the time SendRequestAsync itself has yielded control
        // back here, sentRequest is already populated with the correlation id to reply to.
        sentRequest.Should().NotBeNull();
        var completed = bus.TryCompletePendingRequest(new KafkaClusterResponseEnvelope(sentRequest!.CorrelationId, true, null, "{\"answer\":true}"));
        completed.Should().BeTrue();

        var response = await sendTask;

        response.Success.Should().BeTrue();
        response.PayloadJson.Should().Be("{\"answer\":true}");
    }

    [Fact]
    public async Task SendRequestAsync_PeerRejectsTheRequest_ThrowsInvalidOperationExceptionWithThePeersErrorMessage()
    {
        KafkaClusterRequestEnvelope? sentRequest = null;
        var producer = CreateSucceedingProducer(request => sentRequest = request);
        await using var bus = KafkaClusterMessageBusTestHelpers.CreateBus(producer: producer, requestTimeout: TimeSpan.FromSeconds(5));

        var sendTask = bus.SendRequestAsync(
            new Uri("https://leader:5000/"), KafkaClusterRequestKind.RestoreSnapshot, "SomeChannel", null, null, CancellationToken.None);

        sentRequest.Should().NotBeNull();
        bus.TryCompletePendingRequest(new KafkaClusterResponseEnvelope(sentRequest!.CorrelationId, false, "channel not found", null));

        var act = () => sendTask;

        (await act.Should().ThrowAsync<InvalidOperationException>()).WithMessage("*channel not found*");
    }

    [Fact]
    public async Task TryCompletePendingRequest_NoMatchingPendingRequest_ReturnsFalse()
    {
        await using var bus = KafkaClusterMessageBusTestHelpers.CreateBus();

        var completed = bus.TryCompletePendingRequest(new KafkaClusterResponseEnvelope(Guid.NewGuid(), true, null, null));

        completed.Should().BeFalse();
    }
}
