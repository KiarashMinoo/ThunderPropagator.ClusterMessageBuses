using FluentAssertions;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.NATS;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.UnitTests.NATS;

public class NatsClusterMessageBusRequestReplyTests
{
    [Fact]
    public async Task SendRequestAsync_NoReplyArrives_ThrowsTimeoutException()
    {
        var transport = NatsClusterMessageBusTestHelpers.CreateSubstituteTransport();
        transport.RequestAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns((string?)null);

        await using var bus = await NatsClusterMessageBusTestHelpers.CreateBusAsync(transport: transport, requestTimeout: TimeSpan.FromMilliseconds(50));

        var act = async () => await bus.SendRequestAsync(
            new Uri("https://peer:5001/"), NatsClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null, CancellationToken.None);

        await act.Should().ThrowAsync<TimeoutException>();
    }

    [Fact]
    public async Task SendRequestAsync_TransportTimesOutInternally_ThrowsTimeoutException()
    {
        var transport = NatsClusterMessageBusTestHelpers.CreateSubstituteTransport();
        transport.RequestAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Throws(new OperationCanceledException());

        await using var bus = await NatsClusterMessageBusTestHelpers.CreateBusAsync(transport: transport, requestTimeout: TimeSpan.FromSeconds(5));

        var act = async () => await bus.SendRequestAsync(
            new Uri("https://peer:5001/"), NatsClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null, CancellationToken.None);

        await act.Should().ThrowAsync<TimeoutException>();
    }

    [Fact]
    public async Task SendRequestAsync_CallerCancels_ThrowsOperationCanceledExceptionNotTimeoutException()
    {
        var transport = NatsClusterMessageBusTestHelpers.CreateSubstituteTransport();
        transport.RequestAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var token = (CancellationToken)callInfo[2];
                token.ThrowIfCancellationRequested();
                return new ValueTask<string?>((string?)null);
            });

        await using var bus = await NatsClusterMessageBusTestHelpers.CreateBusAsync(transport: transport, requestTimeout: TimeSpan.FromSeconds(30));

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var act = async () => await bus.SendRequestAsync(
            new Uri("https://peer:5001/"), NatsClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null, cts.Token);

        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task SendRequestAsync_SuccessfulReply_ReturnsIt()
    {
        var transport = NatsClusterMessageBusTestHelpers.CreateSubstituteTransport();
        var payload = "[{\"SubscriptionId\":\"sub-9\"}]";
        transport.RequestAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new NatsClusterResponseEnvelope(true, null, payload).ToNJson());

        await using var bus = await NatsClusterMessageBusTestHelpers.CreateBusAsync(transport: transport, requestTimeout: TimeSpan.FromSeconds(5));

        var response = await bus.SendRequestAsync(
            new Uri("https://peer:5001/"), NatsClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null, CancellationToken.None);

        response.Success.Should().BeTrue();
        response.PayloadJson.Should().Be(payload);
    }

    [Fact]
    public async Task SendRequestAsync_PeerRejectsRequest_ThrowsInvalidOperationExceptionWithThePeersMessage()
    {
        var transport = NatsClusterMessageBusTestHelpers.CreateSubstituteTransport();
        transport.RequestAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new NatsClusterResponseEnvelope(false, "channel not found", null).ToNJson());

        await using var bus = await NatsClusterMessageBusTestHelpers.CreateBusAsync(transport: transport, requestTimeout: TimeSpan.FromSeconds(5));

        var act = async () => await bus.SendRequestAsync(
            new Uri("https://peer:5001/"), NatsClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null, CancellationToken.None);

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("*channel not found*");
    }

    [Fact]
    public async Task SendRequestAsync_UnparseableReply_ThrowsInvalidOperationException()
    {
        var transport = NatsClusterMessageBusTestHelpers.CreateSubstituteTransport();
        transport.RequestAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns("not valid json");

        await using var bus = await NatsClusterMessageBusTestHelpers.CreateBusAsync(transport: transport, requestTimeout: TimeSpan.FromSeconds(5));

        var act = async () => await bus.SendRequestAsync(
            new Uri("https://peer:5001/"), NatsClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null, CancellationToken.None);

        await act.Should().ThrowAsync<Exception>();
    }

    [Fact]
    public async Task HandleRequestDeliveryAsync_ValidRequestWithReplyTo_PublishesTheResponseToIt()
    {
        var transport = NatsClusterMessageBusTestHelpers.CreateSubstituteTransport();
        var resolver = Substitute.For<IClusterChannelResolver>();
        var channelKey = Guid.NewGuid();
        var channel = Substitute.For<IChannel>();
        channel.GetLocalClusterSubscriptionDescriptors().Returns([]);
        resolver.GetChannel(channelKey).Returns(channel);

        await using var bus = await NatsClusterMessageBusTestHelpers.CreateBusAsync(transport: transport, channelResolver: resolver);

        var request = new NatsClusterRequestEnvelope(NatsClusterRequestKind.FetchSubscriptions, null, channelKey, null);
        var delivery = new NatsClusterDelivery(request.ToNJson(), "_INBOX.abc123");

        await bus.HandleRequestDeliveryAsync(delivery, CancellationToken.None);

        await transport.Received(1).PublishAsync(
            "_INBOX.abc123",
            Arg.Is<string>(json => json.FromNJson<NatsClusterResponseEnvelope>()!.Success),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task HandleRequestDeliveryAsync_NoReplyTo_DoesNotThrowAndDoesNotPublish()
    {
        var transport = NatsClusterMessageBusTestHelpers.CreateSubstituteTransport();
        await using var bus = await NatsClusterMessageBusTestHelpers.CreateBusAsync(transport: transport);

        var request = new NatsClusterRequestEnvelope(NatsClusterRequestKind.FetchSubscriptions, null, Guid.NewGuid(), null);
        var delivery = new NatsClusterDelivery(request.ToNJson(), null);

        var act = async () => await bus.HandleRequestDeliveryAsync(delivery, CancellationToken.None);

        await act.Should().NotThrowAsync();
        await transport.DidNotReceive().PublishAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task HandleRequestDeliveryAsync_MalformedPayload_IsSkippedWithoutThrowing()
    {
        await using var bus = await NatsClusterMessageBusTestHelpers.CreateBusAsync();

        var delivery = new NatsClusterDelivery("not json", "_INBOX.abc123");

        var act = async () => await bus.HandleRequestDeliveryAsync(delivery, CancellationToken.None);

        await act.Should().NotThrowAsync();
    }
}
