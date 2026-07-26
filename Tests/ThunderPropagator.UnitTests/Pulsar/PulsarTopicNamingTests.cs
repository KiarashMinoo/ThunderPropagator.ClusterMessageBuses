using FluentAssertions;
using ThunderPropagator.ClusterMessageBuses.Pulsar;

namespace ThunderPropagator.UnitTests.Pulsar;

public class PulsarTopicNamingTests
{
    [Fact]
    public void FanOutTopic_IncludesPrefixAndChannelKey()
    {
        var channelKey = Guid.NewGuid();

        var topic = PulsarTopicNaming.FanOutTopic("persistent://public/default/prefix", channelKey);

        topic.Should().Be($"persistent://public/default/prefix-fanout-{channelKey:N}");
    }

    [Fact]
    public void SubscriptionEventTopic_IncludesPrefixAndChannelKey()
    {
        var channelKey = Guid.NewGuid();

        var topic = PulsarTopicNaming.SubscriptionEventTopic("persistent://public/default/prefix", channelKey);

        topic.Should().Be($"persistent://public/default/prefix-subscriptions-{channelKey:N}");
    }

    [Fact]
    public void RequestTopic_AndReplyTopic_NeverCollideForTheSameNode()
    {
        var endpoint = new Uri("https://node1:5001/");

        var request = PulsarTopicNaming.RequestTopic("prefix", endpoint);
        var reply = PulsarTopicNaming.ReplyTopic("prefix", endpoint);

        request.Should().NotBe(reply);
    }

    [Fact]
    public void Slugify_DefaultPort_OmitsPortSegment()
    {
        var slug = PulsarTopicNaming.Slugify(new Uri("https://follower1/"));

        slug.Should().Be("follower1");
    }

    [Fact]
    public void Slugify_NonDefaultPort_AppendsPortSegment()
    {
        var slug = PulsarTopicNaming.Slugify(new Uri("https://follower1:5001/"));

        slug.Should().Be("follower1-5001");
    }

    [Fact]
    public void RequestTopic_DifferentNodes_ProduceDifferentTopics()
    {
        var first = PulsarTopicNaming.RequestTopic("prefix", new Uri("https://node1:5001/"));
        var second = PulsarTopicNaming.RequestTopic("prefix", new Uri("https://node2:5001/"));

        first.Should().NotBe(second);
    }
}
