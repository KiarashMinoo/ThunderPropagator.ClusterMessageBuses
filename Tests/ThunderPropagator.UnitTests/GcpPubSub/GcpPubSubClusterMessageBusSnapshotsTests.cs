using FluentAssertions;
using Google.Cloud.PubSub.V1;
using NSubstitute;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Channels.Metadata;
using ThunderPropagator.Application.Channels.Snapshots;
using ThunderPropagator.BuildingBlocks.Application.Enums;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.ClusterMessageBuses.GcpPubSub;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.UnitTests.GcpPubSub;

public class GcpPubSubClusterMessageBusSnapshotsTests
{
    /// <summary>
    /// <see cref="SnapshotEntry"/> has an <c>internal</c> constructor and no public/settable
    /// properties, so tests can only construct one via the
    /// <c>InternalsVisibleTo("ThunderPropagator.UnitTests")</c> grant on the core
    /// <c>ThunderPropagator.Application</c> assembly — never via an object initializer. Defaults to
    /// <see cref="SnapshotEntryState.Active"/>, which is all every test here needs.
    /// </summary>
    private static SnapshotEntry CreateActiveEntry(int hashKey) =>
        new(hashKey, new Dictionary<string, object?>(), CastType.Broadcast, new Dictionary<string, object?>());

    private static IChannel CreateChannelSubstitute(string channelName, SnapshotEntry[] searchResults)
    {
        var metadata = Substitute.For<IChannelMetadata>();
        metadata.ChannelName.Returns(channelName);

        var channel = Substitute.For<IChannel>();
        channel.Metadata.Returns(metadata);
        channel.SearchSnapshotsAsync(Arg.Any<Func<SnapshotEntry, bool>>(), Arg.Any<int>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(searchResults));

        return channel;
    }

    [Fact]
    public async Task BuildResponseAsync_RestoreSnapshot_ReturnsTheChannelsActiveEntries()
    {
        var entries = new[] { CreateActiveEntry(1) };
        var channel = CreateChannelSubstitute("orders", entries);

        var channelResolver = Substitute.For<IClusterChannelResolver>();
        channelResolver.GetChannel("orders").Returns(channel);

        await using var bus = await GcpPubSubClusterMessageBusTestHelpers.CreateBusAsync(channelResolver: channelResolver);

        var request = new GcpPubSubClusterRequestEnvelope(Guid.NewGuid(), GcpPubSubClusterRequestKind.RestoreSnapshot, "orders", null, null, new Uri("https://requester:5002/"));
        var response = await bus.BuildResponseAsync(request, CancellationToken.None);

        response.Success.Should().BeTrue();
        var returnedEntries = response.PayloadJson!.FromNJson<SnapshotEntry[]>();
        returnedEntries.Should().ContainSingle(e => e.HashKey == 1);
    }

    [Fact]
    public async Task BuildResponseAsync_RestoreSnapshot_UnknownChannel_ReturnsUnsuccessfulResponse()
    {
        var channelResolver = Substitute.For<IClusterChannelResolver>();
        channelResolver.GetChannel("missing").Returns(_ => throw new InvalidOperationException("Channel 'missing' is not registered."));

        await using var bus = await GcpPubSubClusterMessageBusTestHelpers.CreateBusAsync(channelResolver: channelResolver);

        var request = new GcpPubSubClusterRequestEnvelope(Guid.NewGuid(), GcpPubSubClusterRequestKind.RestoreSnapshot, "missing", null, null, new Uri("https://requester:5002/"));
        var response = await bus.BuildResponseAsync(request, CancellationToken.None);

        response.Success.Should().BeFalse();
        response.ErrorMessage.Should().Contain("missing");
    }

    [Fact]
    public async Task BuildResponseAsync_SyncDelta_ReturnsUpdatedEntriesAsAPayload()
    {
        var entries = new[] { CreateActiveEntry(2) };
        var channel = CreateChannelSubstitute("orders", entries);

        var channelResolver = Substitute.For<IClusterChannelResolver>();
        channelResolver.GetChannel("orders").Returns(channel);

        await using var bus = await GcpPubSubClusterMessageBusTestHelpers.CreateBusAsync(channelResolver: channelResolver);

        var since = DateTimeOffset.UtcNow.AddMinutes(-5);
        var request = new GcpPubSubClusterRequestEnvelope(Guid.NewGuid(), GcpPubSubClusterRequestKind.SyncDelta, "orders", null, since.UtcTicks, new Uri("https://requester:5002/"));
        var response = await bus.BuildResponseAsync(request, CancellationToken.None);

        response.Success.Should().BeTrue();
        var delta = response.PayloadJson!.FromNJson<GcpPubSubSnapshotDeltaPayload>();
        delta!.UpdatedEntries.Should().ContainSingle(e => e.HashKey == 2);
    }

    [Fact]
    public async Task RestoreFromLeaderAsync_SendsARestoreSnapshotRequest_ForTheChannelsName()
    {
        var channel = CreateChannelSubstitute("orders", []);

        GcpPubSubClusterMessageBus? busHolder = null;
        var publisher = GcpPubSubClusterMessageBusTestHelpers.CreatePublisherSubstitute();
        publisher.PublishAsync(Arg.Any<TopicName>(), Arg.Any<IEnumerable<PubsubMessage>>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var topicName = (TopicName)callInfo[0];
                var messages = (IEnumerable<PubsubMessage>)callInfo[1];
                if (topicName.TopicId.Contains("-requests-"))
                {
                    var request = messages.Single().Data.ToStringUtf8().FromNJson<GcpPubSubClusterRequestEnvelope>()!;
                    var response = new GcpPubSubClusterResponseEnvelope(request.CorrelationId, true, null, Array.Empty<SnapshotEntry>().ToNJson());
                    _ = busHolder!.HandleReplyDeliveryAsync(response.ToNJson(), CancellationToken.None);
                }

                return Task.FromResult(new PublishResponse());
            });

        await using var bus = await GcpPubSubClusterMessageBusTestHelpers.CreateBusAsync(publisher: publisher);
        busHolder = bus;

        var act = async () => await bus.RestoreFromLeaderAsync(new Uri("https://leader:5001/"), channel, CancellationToken.None);

        await act.Should().NotThrowAsync();

        var expectedRequestTopicId = "tp-cluster-requests-leader-5001";

        await publisher.Received(1).PublishAsync(
            Arg.Is<TopicName>(t => t.TopicId == expectedRequestTopicId),
            Arg.Is<IEnumerable<PubsubMessage>>(messages =>
                messages.Single().Data.ToStringUtf8().FromNJson<GcpPubSubClusterRequestEnvelope>()!.Kind == GcpPubSubClusterRequestKind.RestoreSnapshot &&
                messages.Single().Data.ToStringUtf8().FromNJson<GcpPubSubClusterRequestEnvelope>()!.ChannelName == "orders"),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task SyncDeltaFromLeaderAsync_SendsASyncDeltaRequest_WithTheSinceTimestamp()
    {
        var channel = CreateChannelSubstitute("orders", []);
        var since = DateTimeOffset.UtcNow.AddMinutes(-10);

        GcpPubSubClusterMessageBus? busHolder = null;
        var publisher = GcpPubSubClusterMessageBusTestHelpers.CreatePublisherSubstitute();
        publisher.PublishAsync(Arg.Any<TopicName>(), Arg.Any<IEnumerable<PubsubMessage>>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var topicName = (TopicName)callInfo[0];
                var messages = (IEnumerable<PubsubMessage>)callInfo[1];
                if (topicName.TopicId.Contains("-requests-"))
                {
                    var request = messages.Single().Data.ToStringUtf8().FromNJson<GcpPubSubClusterRequestEnvelope>()!;
                    var deltaPayload = new GcpPubSubSnapshotDeltaPayload { UpdatedEntries = [], DeletedHashKeys = [] };
                    var response = new GcpPubSubClusterResponseEnvelope(request.CorrelationId, true, null, deltaPayload.ToNJson());
                    _ = busHolder!.HandleReplyDeliveryAsync(response.ToNJson(), CancellationToken.None);
                }

                return Task.FromResult(new PublishResponse());
            });

        await using var bus = await GcpPubSubClusterMessageBusTestHelpers.CreateBusAsync(publisher: publisher);
        busHolder = bus;

        await bus.SyncDeltaFromLeaderAsync(new Uri("https://leader:5001/"), channel, since, CancellationToken.None);

        await publisher.Received(1).PublishAsync(
            Arg.Any<TopicName>(),
            Arg.Is<IEnumerable<PubsubMessage>>(messages =>
                messages.Single().Data.ToStringUtf8().FromNJson<GcpPubSubClusterRequestEnvelope>()!.Kind == GcpPubSubClusterRequestKind.SyncDelta &&
                messages.Single().Data.ToStringUtf8().FromNJson<GcpPubSubClusterRequestEnvelope>()!.SinceTicks == since.UtcTicks),
            Arg.Any<CancellationToken>());
    }
}
