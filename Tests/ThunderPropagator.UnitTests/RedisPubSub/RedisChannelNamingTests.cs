using FluentAssertions;
using ThunderPropagator.ClusterMessageBuses.RedisPubSub;

namespace ThunderPropagator.UnitTests.RedisPubSub;

public class RedisChannelNamingTests
{
    [Fact]
    public void FanOutChannel_IncludesPrefixAndChannelKey()
    {
        var channelKey = Guid.NewGuid();

        var channel = RedisChannelNaming.FanOutChannel("thunderpropagator:cluster", channelKey);

        channel.Should().Be($"thunderpropagator:cluster:fanout:{channelKey:N}");
    }

    [Fact]
    public void SubscriptionEventChannel_IncludesPrefixAndChannelKey()
    {
        var channelKey = Guid.NewGuid();

        var channel = RedisChannelNaming.SubscriptionEventChannel("thunderpropagator:cluster", channelKey);

        channel.Should().Be($"thunderpropagator:cluster:subscriptions:{channelKey:N}");
    }

    [Fact]
    public void RequestChannel_AndReplyChannel_NeverCollideForTheSameNode()
    {
        var endpoint = new Uri("https://node1:5001/");

        var request = RedisChannelNaming.RequestChannel("prefix", endpoint);
        var reply = RedisChannelNaming.ReplyChannel("prefix", endpoint);

        request.Should().NotBe(reply);
    }

    [Fact]
    public void Slugify_DefaultPort_OmitsPortSegment()
    {
        var slug = RedisChannelNaming.Slugify(new Uri("https://follower1/"));

        slug.Should().Be("follower1");
    }

    [Fact]
    public void Slugify_NonDefaultPort_AppendsPortSegment()
    {
        var slug = RedisChannelNaming.Slugify(new Uri("https://follower1:5001/"));

        slug.Should().Be("follower1-5001");
    }

    [Fact]
    public void RequestChannel_DifferentNodes_ProduceDifferentChannels()
    {
        var first = RedisChannelNaming.RequestChannel("prefix", new Uri("https://node1:5001/"));
        var second = RedisChannelNaming.RequestChannel("prefix", new Uri("https://node2:5001/"));

        first.Should().NotBe(second);
    }
}
