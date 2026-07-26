using FluentAssertions;
using ThunderPropagator.ClusterMessageBuses.ActiveMQ;

namespace ThunderPropagator.UnitTests.ActiveMQ;

public class ActiveMqTopicNamingTests
{
    [Fact]
    public void FanOutTopic_IncludesPrefixAndChannelKey()
    {
        var channelKey = Guid.NewGuid();

        var topic = ActiveMqTopicNaming.FanOutTopic("thunderpropagator.cluster", channelKey);

        topic.Should().Be($"thunderpropagator.cluster.fanout.{channelKey:N}");
    }

    [Fact]
    public void SubscriptionEventTopic_IncludesPrefixAndChannelKey()
    {
        var channelKey = Guid.NewGuid();

        var topic = ActiveMqTopicNaming.SubscriptionEventTopic("thunderpropagator.cluster", channelKey);

        topic.Should().Be($"thunderpropagator.cluster.subscriptions.{channelKey:N}");
    }

    [Fact]
    public void RequestQueue_AndReplyQueue_NeverCollideForTheSameNode()
    {
        var endpoint = new Uri("https://node1:5001/");

        var request = ActiveMqTopicNaming.RequestQueue("prefix", endpoint);
        var reply = ActiveMqTopicNaming.ReplyQueue("prefix", endpoint);

        request.Should().NotBe(reply);
    }

    [Fact]
    public void Slugify_DefaultPort_OmitsPortSegment()
    {
        var slug = ActiveMqTopicNaming.Slugify(new Uri("https://follower1/"));

        slug.Should().Be("follower1");
    }

    [Fact]
    public void Slugify_NonDefaultPort_AppendsPortSegment()
    {
        var slug = ActiveMqTopicNaming.Slugify(new Uri("https://follower1:5001/"));

        slug.Should().Be("follower1-5001");
    }

    [Fact]
    public void RequestQueue_DifferentNodes_ProduceDifferentQueues()
    {
        var first = ActiveMqTopicNaming.RequestQueue("prefix", new Uri("https://node1:5001/"));
        var second = ActiveMqTopicNaming.RequestQueue("prefix", new Uri("https://node2:5001/"));

        first.Should().NotBe(second);
    }
}
