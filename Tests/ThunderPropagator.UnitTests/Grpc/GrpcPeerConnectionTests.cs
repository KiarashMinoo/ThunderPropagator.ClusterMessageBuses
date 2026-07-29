using FluentAssertions;
using ThunderPropagator.ClusterMessageBuses.Grpc.Protos;

namespace ThunderPropagator.UnitTests.Grpc;

public class GrpcPeerConnectionTests
{
    [Fact]
    public async Task SendFanOutAsync_WritesToTheRequestStream()
    {
        var (connection, fanOutWriter, _, _, _) = GrpcClusterMessageBusTestHelpers.CreatePeerConnection();
        var batch = new PushedMessageBatch { ChannelKey = Guid.NewGuid().ToString(), PayloadJson = "{}" };

        await connection.SendFanOutAsync(batch, CancellationToken.None);

        fanOutWriter.Written.Should().ContainSingle().Which.Should().BeEquivalentTo(batch);
    }

    [Fact]
    public async Task SendSubscriptionEventAsync_WritesToTheRequestStream()
    {
        var (connection, _, _, syncWriter, _) = GrpcClusterMessageBusTestHelpers.CreatePeerConnection();
        var wireEvent = new SubscriptionEvent { ChannelKey = Guid.NewGuid().ToString(), PayloadJson = "{}" };

        await connection.SendSubscriptionEventAsync(wireEvent, CancellationToken.None);

        syncWriter.Written.Should().ContainSingle().Which.Should().BeEquivalentTo(wireEvent);
    }

    [Fact]
    public async Task DisposeAsync_CompletesBothRequestStreams()
    {
        var (connection, fanOutWriter, _, syncWriter, _) = GrpcClusterMessageBusTestHelpers.CreatePeerConnection();

        await connection.DisposeAsync();

        fanOutWriter.Completed.Should().BeTrue();
        syncWriter.Completed.Should().BeTrue();
    }
}
