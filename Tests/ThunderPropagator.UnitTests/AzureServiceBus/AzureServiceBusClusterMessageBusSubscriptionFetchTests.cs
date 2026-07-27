using Azure.Messaging.ServiceBus;
using FluentAssertions;
using NSubstitute;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Channels.Cluster.Subscriptions;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.AzureServiceBus;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.UnitTests.AzureServiceBus;

public class AzureServiceBusClusterMessageBusSubscriptionFetchTests
{
    [Fact]
    public async Task FetchPeerSubscriptionsAsync_Success_ReturnsTheDescriptors()
    {
        var descriptors = new[] { new ClusterSubscriptionDescriptor("sub-1", "req-1", "conn-1") };
        var channelKey = Guid.NewGuid();

        AzureServiceBusClusterMessageBus? busHolder = null;
        var sender = AzureServiceBusClusterMessageBusTestHelpers.CreateSenderSubstitute();
        sender.SendMessageAsync(Arg.Any<ServiceBusMessage>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var request = ((ServiceBusMessage)callInfo[0]).Body.ToString().FromNJson<AzureServiceBusClusterRequestEnvelope>()!;
                var response = new AzureServiceBusClusterResponseEnvelope(request.CorrelationId, true, null, descriptors.ToNJson());
                _ = busHolder!.HandleReplyDeliveryAsync(response.ToNJson(), CancellationToken.None);
                return Task.CompletedTask;
            });
        var client = AzureServiceBusClusterMessageBusTestHelpers.CreateClientSubstitute(senderFactory: _ => sender);

        await using var bus = await AzureServiceBusClusterMessageBusTestHelpers.CreateBusAsync(client: client);
        busHolder = bus;

        var result = await bus.FetchPeerSubscriptionsAsync(new Uri("https://peer1:5001/"), channelKey, CancellationToken.None);

        result.Should().ContainSingle(d => d.SubscriptionId == "sub-1");

        await sender.Received(1).SendMessageAsync(
            Arg.Is<ServiceBusMessage>(m =>
                m.Body.ToString().FromNJson<AzureServiceBusClusterRequestEnvelope>()!.Kind == AzureServiceBusClusterRequestKind.FetchSubscriptions &&
                m.Body.ToString().FromNJson<AzureServiceBusClusterRequestEnvelope>()!.ChannelKey == channelKey),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task FetchPeerSubscriptionsAsync_PeerRejectsTheRequest_DegradesToEmptyArray()
    {
        var channelKey = Guid.NewGuid();

        AzureServiceBusClusterMessageBus? busHolder = null;
        var sender = AzureServiceBusClusterMessageBusTestHelpers.CreateSenderSubstitute();
        sender.SendMessageAsync(Arg.Any<ServiceBusMessage>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var request = ((ServiceBusMessage)callInfo[0]).Body.ToString().FromNJson<AzureServiceBusClusterRequestEnvelope>()!;
                var response = new AzureServiceBusClusterResponseEnvelope(request.CorrelationId, false, "no such channel", null);
                _ = busHolder!.HandleReplyDeliveryAsync(response.ToNJson(), CancellationToken.None);
                return Task.CompletedTask;
            });
        var client = AzureServiceBusClusterMessageBusTestHelpers.CreateClientSubstitute(senderFactory: _ => sender);

        await using var bus = await AzureServiceBusClusterMessageBusTestHelpers.CreateBusAsync(client: client);
        busHolder = bus;

        var result = await bus.FetchPeerSubscriptionsAsync(new Uri("https://peer1:5001/"), channelKey, CancellationToken.None);

        result.Should().BeEmpty();
    }

    [Fact]
    public async Task FetchPeerSubscriptionsAsync_Timeout_DegradesToEmptyArray()
    {
        await using var bus = await AzureServiceBusClusterMessageBusTestHelpers.CreateBusAsync(requestTimeout: TimeSpan.FromMilliseconds(50));

        var result = await bus.FetchPeerSubscriptionsAsync(new Uri("https://peer1:5001/"), Guid.NewGuid(), CancellationToken.None);

        result.Should().BeEmpty("an unreachable/unresponsive peer must degrade gracefully rather than throw");
    }

    [Fact]
    public async Task BuildFetchSubscriptionsResponse_ReturnsTheResolvedChannelsLocalDescriptors()
    {
        var channelKey = Guid.NewGuid();
        var channel = Substitute.For<IChannel>();

        var channelResolver = Substitute.For<IClusterChannelResolver>();
        channelResolver.GetChannel(channelKey).Returns(channel);

        await using var bus = await AzureServiceBusClusterMessageBusTestHelpers.CreateBusAsync(channelResolver: channelResolver);

        var request = new AzureServiceBusClusterRequestEnvelope(Guid.NewGuid(), AzureServiceBusClusterRequestKind.FetchSubscriptions, null, channelKey, null, new Uri("https://requester:5002/"));
        var response = await bus.BuildResponseAsync(request, CancellationToken.None);

        response.Success.Should().BeTrue();
        response.PayloadJson.Should().NotBeNull();
    }
}
