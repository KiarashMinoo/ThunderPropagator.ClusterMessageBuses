using System.Text;
using FluentAssertions;
using NSubstitute;
using RabbitMQ.Client;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.RabbitMQ;

namespace ThunderPropagator.UnitTests.RabbitMQ;

public class RabbitMqClusterMessageBusRequestReplyTests
{
    private static RabbitMqClusterRequestEnvelope CapturePublishedRequest(IChannel publishChannel)
    {
        var call = publishChannel.ReceivedCalls()
            .Last(c => c.GetMethodInfo().Name == nameof(IChannel.BasicPublishAsync));
        var publishedBody = (ReadOnlyMemory<byte>)call.GetArguments()[4]!;

        return Encoding.UTF8.GetString(publishedBody.Span).FromNJson<RabbitMqClusterRequestEnvelope>()!;
    }

    [Fact]
    public async Task SendRequestAsync_NoReplyArrives_ThrowsTimeoutException()
    {
        await using var bus = await RabbitMqClusterMessageBusTestHelpers.CreateBusAsync(requestTimeout: TimeSpan.FromMilliseconds(50));

        var act = async () => await bus.SendRequestAsync(
            new Uri("https://peer:5001/"), RabbitMqClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null, CancellationToken.None);

        await act.Should().ThrowAsync<TimeoutException>();
    }

    [Fact]
    public async Task SendRequestAsync_CallerCancels_ThrowsOperationCanceledExceptionNotTimeoutException()
    {
        await using var bus = await RabbitMqClusterMessageBusTestHelpers.CreateBusAsync(requestTimeout: TimeSpan.FromSeconds(30));

        using var cts = new CancellationTokenSource();
        var task = bus.SendRequestAsync(
            new Uri("https://peer:5001/"), RabbitMqClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null, cts.Token);

        cts.Cancel();

        var act = async () => await task;

        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task SendRequestAsync_MatchingReplyArrivesViaTryCompletePendingRequest_ReturnsIt()
    {
        var connection = RabbitMqClusterMessageBusTestHelpers.CreateTrackedConnection(out var channels);
        await using var bus = await RabbitMqClusterMessageBusTestHelpers.CreateBusAsync(connection: connection, requestTimeout: TimeSpan.FromSeconds(5));

        var task = bus.SendRequestAsync(
            new Uri("https://peer:5001/"), RabbitMqClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null, CancellationToken.None);

        var sentRequest = CapturePublishedRequest(channels[0]);
        var expectedPayload = "[{\"SubscriptionId\":\"sub-9\"}]";

        var completed = bus.TryCompletePendingRequest(new RabbitMqClusterResponseEnvelope(sentRequest.CorrelationId, true, null, expectedPayload));

        completed.Should().BeTrue();

        var response = await task;

        response.PayloadJson.Should().Be(expectedPayload);
    }

    [Fact]
    public async Task SendRequestAsync_PeerRejectsRequest_ThrowsInvalidOperationExceptionWithThePeersMessage()
    {
        var connection = RabbitMqClusterMessageBusTestHelpers.CreateTrackedConnection(out var channels);
        await using var bus = await RabbitMqClusterMessageBusTestHelpers.CreateBusAsync(connection: connection, requestTimeout: TimeSpan.FromSeconds(5));

        var task = bus.SendRequestAsync(
            new Uri("https://peer:5001/"), RabbitMqClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null, CancellationToken.None);

        var sentRequest = CapturePublishedRequest(channels[0]);

        var completed = bus.TryCompletePendingRequest(new RabbitMqClusterResponseEnvelope(sentRequest.CorrelationId, false, "channel not found", null));
        completed.Should().BeTrue();

        var act = async () => await task;

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("*channel not found*");
    }

    [Fact]
    public async Task TryCompletePendingRequest_NoMatchingPendingRequest_ReturnsFalse()
    {
        await using var bus = await RabbitMqClusterMessageBusTestHelpers.CreateBusAsync();

        var response = new RabbitMqClusterResponseEnvelope(Guid.NewGuid(), true, null, null);

        bus.TryCompletePendingRequest(response).Should().BeFalse();
    }
}
