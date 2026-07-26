using FluentAssertions;
using ThunderPropagator.ClusterMessageBuses.Mqtt;

namespace ThunderPropagator.UnitTests.Mqtt;

public class MqttTopicNamingTests
{
    [Fact]
    public void FanOutTopic_IncludesPrefixAndChannelKey()
    {
        var channelKey = Guid.NewGuid();

        var topic = MqttTopicNaming.FanOutTopic("thunderpropagator/cluster", channelKey);

        topic.Should().Be($"thunderpropagator/cluster/fanout/{channelKey:N}");
    }

    [Fact]
    public void SubscriptionEventTopic_IncludesPrefixAndChannelKey()
    {
        var channelKey = Guid.NewGuid();

        var topic = MqttTopicNaming.SubscriptionEventTopic("thunderpropagator/cluster", channelKey);

        topic.Should().Be($"thunderpropagator/cluster/subscriptions/{channelKey:N}");
    }

    [Fact]
    public void RequestTopic_AndReplyTopic_NeverCollideForTheSameNode()
    {
        var endpoint = new Uri("https://node1:5001/");

        var request = MqttTopicNaming.RequestTopic("prefix", endpoint);
        var reply = MqttTopicNaming.ReplyTopic("prefix", endpoint);

        request.Should().NotBe(reply);
    }

    [Fact]
    public void Slugify_DefaultPort_OmitsPortSegment()
    {
        var slug = MqttTopicNaming.Slugify(new Uri("https://follower1/"));

        slug.Should().Be("follower1");
    }

    [Fact]
    public void Slugify_NonDefaultPort_AppendsPortSegment()
    {
        var slug = MqttTopicNaming.Slugify(new Uri("https://follower1:5001/"));

        slug.Should().Be("follower1-5001");
    }

    [Fact]
    public void RequestTopic_DifferentNodes_ProduceDifferentTopics()
    {
        var first = MqttTopicNaming.RequestTopic("prefix", new Uri("https://node1:5001/"));
        var second = MqttTopicNaming.RequestTopic("prefix", new Uri("https://node2:5001/"));

        first.Should().NotBe(second);
    }
}
