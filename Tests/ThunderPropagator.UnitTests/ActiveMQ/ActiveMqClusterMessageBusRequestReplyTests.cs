using Apache.NMS;
using FluentAssertions;
using NSubstitute;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.ActiveMQ;

namespace ThunderPropagator.UnitTests.ActiveMQ;

public class ActiveMqClusterMessageBusRequestReplyTests
{
    private static ActiveMqClusterRequestEnvelope CapturePublishedRequest(IMessageProducer producer)
    {
        var call = producer.ReceivedCalls()
            .Last(c => c.GetMethodInfo().Name == nameof(IMessageProducer.SendAsync));
        var textMessage = (ITextMessage)call.GetArguments()[1]!;

        return textMessage.Text!.FromNJson<ActiveMqClusterRequestEnvelope>()!;
    }

    [Fact]
    public async Task SendRequestAsync_NoReplyArrives_ThrowsTimeoutException()
    {
        await using var bus = await ActiveMqClusterMessageBusTestHelpers.CreateBusAsync(requestTimeout: TimeSpan.FromMilliseconds(50));

        var act = async () => await bus.SendRequestAsync(
            new Uri("https://peer:5001/"), ActiveMqClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null, CancellationToken.None);

        await act.Should().ThrowAsync<TimeoutException>();
    }

    [Fact]
    public async Task SendRequestAsync_CallerCancels_ThrowsOperationCanceledExceptionNotTimeoutException()
    {
        await using var bus = await ActiveMqClusterMessageBusTestHelpers.CreateBusAsync(requestTimeout: TimeSpan.FromSeconds(30));

        using var cts = new CancellationTokenSource();
        var task = bus.SendRequestAsync(
            new Uri("https://peer:5001/"), ActiveMqClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null, cts.Token);

        cts.Cancel();

        var act = async () => await task;

        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task SendRequestAsync_MatchingReplyArrivesViaTryCompletePendingRequest_ReturnsIt()
    {
        var connection = ActiveMqClusterMessageBusTestHelpers.CreateTrackedConnection(out var sessions);
        await using var bus = await ActiveMqClusterMessageBusTestHelpers.CreateBusAsync(connection: connection, requestTimeout: TimeSpan.FromSeconds(5));

        var producer = await sessions[0].CreateProducerAsync();

        var task = bus.SendRequestAsync(
            new Uri("https://peer:5001/"), ActiveMqClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null, CancellationToken.None);

        var sentRequest = CapturePublishedRequest(producer);
        var expectedPayload = "[{\"SubscriptionId\":\"sub-9\"}]";

        var completed = bus.TryCompletePendingRequest(new ActiveMqClusterResponseEnvelope(sentRequest.CorrelationId, true, null, expectedPayload));

        completed.Should().BeTrue();

        var response = await task;

        response.PayloadJson.Should().Be(expectedPayload);
    }

    [Fact]
    public async Task SendRequestAsync_PeerRejectsRequest_ThrowsInvalidOperationExceptionWithThePeersMessage()
    {
        var connection = ActiveMqClusterMessageBusTestHelpers.CreateTrackedConnection(out var sessions);
        await using var bus = await ActiveMqClusterMessageBusTestHelpers.CreateBusAsync(connection: connection, requestTimeout: TimeSpan.FromSeconds(5));

        var producer = await sessions[0].CreateProducerAsync();

        var task = bus.SendRequestAsync(
            new Uri("https://peer:5001/"), ActiveMqClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null, CancellationToken.None);

        var sentRequest = CapturePublishedRequest(producer);

        var completed = bus.TryCompletePendingRequest(new ActiveMqClusterResponseEnvelope(sentRequest.CorrelationId, false, "channel not found", null));
        completed.Should().BeTrue();

        var act = async () => await task;

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("*channel not found*");
    }

    [Fact]
    public async Task TryCompletePendingRequest_NoMatchingPendingRequest_ReturnsFalse()
    {
        await using var bus = await ActiveMqClusterMessageBusTestHelpers.CreateBusAsync();

        var response = new ActiveMqClusterResponseEnvelope(Guid.NewGuid(), true, null, null);

        bus.TryCompletePendingRequest(response).Should().BeFalse();
    }

    [Fact]
    public async Task HandleRequestDeliveryAsync_MalformedPayload_IsSkippedWithoutThrowing()
    {
        await using var bus = await ActiveMqClusterMessageBusTestHelpers.CreateBusAsync();

        var act = async () => await bus.HandleRequestDeliveryAsync("not json", CancellationToken.None);

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task HandleReplyDeliveryAsync_MalformedPayload_IsSkippedWithoutThrowing()
    {
        await using var bus = await ActiveMqClusterMessageBusTestHelpers.CreateBusAsync();

        var act = async () => await bus.HandleReplyDeliveryAsync("not json", CancellationToken.None);

        await act.Should().NotThrowAsync();
    }
}
