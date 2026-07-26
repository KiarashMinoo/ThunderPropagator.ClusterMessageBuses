using FluentAssertions;
using NSubstitute;
using StackExchange.Redis;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.RedisPubSub;

namespace ThunderPropagator.UnitTests.RedisPubSub;

public class RedisPubSubClusterMessageBusRequestReplyTests
{
    private static RedisPubSubClusterRequestEnvelope CapturePublishedRequest(ISubscriber subscriber)
    {
        var call = subscriber.ReceivedCalls()
            .Last(c => c.GetMethodInfo().Name == nameof(ISubscriber.PublishAsync));
        var value = (RedisValue)call.GetArguments()[1]!;

        return value.ToString().FromNJson<RedisPubSubClusterRequestEnvelope>()!;
    }

    [Fact]
    public async Task SendRequestAsync_NoReplyArrives_ThrowsTimeoutException()
    {
        await using var bus = await RedisPubSubClusterMessageBusTestHelpers.CreateBusAsync(requestTimeout: TimeSpan.FromMilliseconds(50));

        var act = async () => await bus.SendRequestAsync(
            new Uri("https://peer:5001/"), RedisPubSubClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null, CancellationToken.None);

        await act.Should().ThrowAsync<TimeoutException>();
    }

    [Fact]
    public async Task SendRequestAsync_CallerCancels_ThrowsOperationCanceledExceptionNotTimeoutException()
    {
        await using var bus = await RedisPubSubClusterMessageBusTestHelpers.CreateBusAsync(requestTimeout: TimeSpan.FromSeconds(30));

        using var cts = new CancellationTokenSource();
        var task = bus.SendRequestAsync(
            new Uri("https://peer:5001/"), RedisPubSubClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null, cts.Token);

        cts.Cancel();

        var act = async () => await task;

        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task SendRequestAsync_MatchingReplyArrivesViaTryCompletePendingRequest_ReturnsIt()
    {
        var subscriber = RedisPubSubClusterMessageBusTestHelpers.CreateSubscriberSubstitute();
        var connection = RedisPubSubClusterMessageBusTestHelpers.CreateSubstituteConnection(subscriber);
        await using var bus = await RedisPubSubClusterMessageBusTestHelpers.CreateBusAsync(connection: connection, requestTimeout: TimeSpan.FromSeconds(5));

        var task = bus.SendRequestAsync(
            new Uri("https://peer:5001/"), RedisPubSubClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null, CancellationToken.None);

        var sentRequest = CapturePublishedRequest(subscriber);
        var expectedPayload = "[{\"SubscriptionId\":\"sub-9\"}]";

        var completed = bus.TryCompletePendingRequest(new RedisPubSubClusterResponseEnvelope(sentRequest.CorrelationId, true, null, expectedPayload));

        completed.Should().BeTrue();

        var response = await task;

        response.PayloadJson.Should().Be(expectedPayload);
    }

    [Fact]
    public async Task SendRequestAsync_PeerRejectsRequest_ThrowsInvalidOperationExceptionWithThePeersMessage()
    {
        var subscriber = RedisPubSubClusterMessageBusTestHelpers.CreateSubscriberSubstitute();
        var connection = RedisPubSubClusterMessageBusTestHelpers.CreateSubstituteConnection(subscriber);
        await using var bus = await RedisPubSubClusterMessageBusTestHelpers.CreateBusAsync(connection: connection, requestTimeout: TimeSpan.FromSeconds(5));

        var task = bus.SendRequestAsync(
            new Uri("https://peer:5001/"), RedisPubSubClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null, CancellationToken.None);

        var sentRequest = CapturePublishedRequest(subscriber);

        var completed = bus.TryCompletePendingRequest(new RedisPubSubClusterResponseEnvelope(sentRequest.CorrelationId, false, "channel not found", null));
        completed.Should().BeTrue();

        var act = async () => await task;

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("*channel not found*");
    }

    [Fact]
    public async Task TryCompletePendingRequest_NoMatchingPendingRequest_ReturnsFalse()
    {
        await using var bus = await RedisPubSubClusterMessageBusTestHelpers.CreateBusAsync();

        var response = new RedisPubSubClusterResponseEnvelope(Guid.NewGuid(), true, null, null);

        bus.TryCompletePendingRequest(response).Should().BeFalse();
    }

    [Fact]
    public async Task HandleRequestDeliveryAsync_MalformedPayload_IsSkippedWithoutThrowing()
    {
        await using var bus = await RedisPubSubClusterMessageBusTestHelpers.CreateBusAsync();

        var act = async () => await bus.HandleRequestDeliveryAsync("not json", CancellationToken.None);

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task HandleReplyDeliveryAsync_MalformedPayload_IsSkippedWithoutThrowing()
    {
        await using var bus = await RedisPubSubClusterMessageBusTestHelpers.CreateBusAsync();

        var act = async () => await bus.HandleReplyDeliveryAsync("not json", CancellationToken.None);

        await act.Should().NotThrowAsync();
    }
}
