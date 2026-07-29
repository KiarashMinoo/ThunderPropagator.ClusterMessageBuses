using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using ThunderPropagator.Application.Channels.Cluster;
using ThunderPropagator.Application.Channels.Cluster.Discovery;
using ThunderPropagator.ClusterMessageBuses.Grpc;
using ThunderPropagator.ClusterMessageBuses.Grpc.Protos;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.UnitTests.Grpc;

/// <summary>
/// Shared construction helpers for <see cref="GrpcClusterMessageBus"/> tests. Every test constructs
/// the bus through the same seams the production DI extension uses. <c>ClusterFanOutClient</c>/
/// <c>ClusterSubscriptionSyncClient</c>/<c>ClusterSnapshotClient</c>/<c>ClusterSubscriptionFetchClient</c>
/// are <c>ClientBase&lt;T&gt;</c>-derived with a protected parameterless constructor and virtual RPC
/// methods specifically for direct test substitution, so no real network connection or embedded
/// gRPC/ASP.NET Core host is ever needed — <see cref="IGrpcClusterHost"/> is substituted the same way
/// for the same reason <c>IWebSocketClusterListener</c> is elsewhere in this repo.
/// </summary>
internal static class GrpcClusterMessageBusTestHelpers
{
    internal static readonly Uri DefaultNodeEndpoint = new("https://node1:5000/");

    internal static IGrpcClusterHost CreateHostSubstitute()
    {
        var host = Substitute.For<IGrpcClusterHost>();
        host.StartAsync(Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        host.DisposeAsync().Returns(ValueTask.CompletedTask);
        return host;
    }

    internal static IClusterNodeDiscovery CreateDiscoverySubstitute(IReadOnlyList<ClusterNodeEntry> peers)
    {
        var discovery = Substitute.For<IClusterNodeDiscovery>();
        discovery.GetPeersAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(peers));
        return discovery;
    }

    /// <summary>
    /// Builds a substitute <c>ClusterFanOutClient</c> whose <c>Stream</c> call is backed by a fake
    /// in-memory request writer/response reader pair, so a test can inspect what was written and
    /// inject what the "peer" writes back.
    /// </summary>
    internal static (ClusterFanOut.ClusterFanOutClient Client, FakeClientStreamWriter<PushedMessageBatch> RequestWriter, FakeAsyncStreamReader<PushedMessageBatch> ResponseReader)
        CreateFanOutClientSubstitute()
    {
        var requestWriter = new FakeClientStreamWriter<PushedMessageBatch>();
        var responseReader = new FakeAsyncStreamReader<PushedMessageBatch>();

        var client = Substitute.For<ClusterFanOut.ClusterFanOutClient>();
        client.Stream(cancellationToken: Arg.Any<CancellationToken>()).Returns(_ => new AsyncDuplexStreamingCall<PushedMessageBatch, PushedMessageBatch>(
            requestWriter, responseReader, Task.FromResult(new Metadata()), () => Status.DefaultSuccess, () => new Metadata(), () => { }));

        return (client, requestWriter, responseReader);
    }

    /// <summary>Same shape as <see cref="CreateFanOutClientSubstitute"/>, for <c>ClusterSubscriptionSyncClient</c>.</summary>
    internal static (ClusterSubscriptionSync.ClusterSubscriptionSyncClient Client, FakeClientStreamWriter<SubscriptionEvent> RequestWriter, FakeAsyncStreamReader<SubscriptionAck> ResponseReader)
        CreateSubscriptionSyncClientSubstitute()
    {
        var requestWriter = new FakeClientStreamWriter<SubscriptionEvent>();
        var responseReader = new FakeAsyncStreamReader<SubscriptionAck>();

        var client = Substitute.For<ClusterSubscriptionSync.ClusterSubscriptionSyncClient>();
        client.Stream(cancellationToken: Arg.Any<CancellationToken>()).Returns(_ => new AsyncDuplexStreamingCall<SubscriptionEvent, SubscriptionAck>(
            requestWriter, responseReader, Task.FromResult(new Metadata()), () => Status.DefaultSuccess, () => new Metadata(), () => { }));

        return (client, requestWriter, responseReader);
    }

    /// <summary>Builds a fake, immediately-completed <see cref="AsyncUnaryCall{TResponse}"/> for a unary-RPC test double.</summary>
    internal static AsyncUnaryCall<TResponse> CreateUnaryCall<TResponse>(TResponse response) =>
        new(Task.FromResult(response), Task.FromResult(new Metadata()), () => Status.DefaultSuccess, () => new Metadata(), () => { });

    /// <summary>Builds a fake <see cref="AsyncUnaryCall{TResponse}"/> whose response task is already faulted — for simulating a peer that never answers/rejects the call outright.</summary>
    internal static AsyncUnaryCall<TResponse> CreateFailingUnaryCall<TResponse>(Exception exception) =>
        new(Task.FromException<TResponse>(exception), Task.FromResult(new Metadata()), () => Status.DefaultSuccess, () => new Metadata(), () => { });

    /// <summary>Builds a full <see cref="GrpcPeerConnection"/> backed by fake duplex-call plumbing for both streams.</summary>
    internal static (GrpcPeerConnection Connection, FakeClientStreamWriter<PushedMessageBatch> FanOutRequestWriter, FakeAsyncStreamReader<PushedMessageBatch> FanOutResponseReader, FakeClientStreamWriter<SubscriptionEvent> SubscriptionSyncRequestWriter, FakeAsyncStreamReader<SubscriptionAck> SubscriptionSyncResponseReader)
        CreatePeerConnection()
    {
        var (fanOutClient, fanOutWriter, fanOutReader) = CreateFanOutClientSubstitute();
        var (subscriptionSyncClient, subscriptionSyncWriter, subscriptionSyncReader) = CreateSubscriptionSyncClientSubstitute();

        var connection = new GrpcPeerConnection(fanOutClient, subscriptionSyncClient, CancellationToken.None);

        return (connection, fanOutWriter, fanOutReader, subscriptionSyncWriter, subscriptionSyncReader);
    }

    internal static async Task<GrpcClusterMessageBus> CreateBusAsync(
        IClusterNodeDiscovery? discovery = null,
        IClusterChannelResolver? channelResolver = null,
        Uri? nodeEndpoint = null,
        TimeSpan? requestTimeout = null,
        TimeSpan? initialReconnectDelay = null,
        TimeSpan? maxReconnectDelay = null,
        Func<CancellationToken, Task<IGrpcClusterHost>>? hostFactory = null,
        Func<Uri, CancellationToken, Task<GrpcPeerConnection>>? peerConnectionFactory = null,
        Func<Uri, CancellationToken, Task<GrpcUnaryClients>>? unaryClientsFactory = null)
    {
        discovery ??= CreateDiscoverySubstitute([]);
        channelResolver ??= Substitute.For<IClusterChannelResolver>();

        var options = Options.Create(new GrpcClusterMessageBusOptions
        {
            RequestTimeout = requestTimeout ?? TimeSpan.FromSeconds(30),
            InitialReconnectDelay = initialReconnectDelay ?? TimeSpan.FromMilliseconds(20),
            MaxReconnectDelay = maxReconnectDelay ?? TimeSpan.FromMilliseconds(200),
        });

        var clusterConfiguration = new ClusterConfiguration { NodeEndpoint = nodeEndpoint ?? DefaultNodeEndpoint };

        var bus = new GrpcClusterMessageBus(
            options,
            clusterConfiguration,
            channelResolver,
            discovery,
            NullLoggerFactory.Instance,
            hostFactory ?? (_ => Task.FromResult(CreateHostSubstitute())),
            peerConnectionFactory,
            unaryClientsFactory);

        await bus.EnsureInitializedAsync(CancellationToken.None);

        return bus;
    }
}
