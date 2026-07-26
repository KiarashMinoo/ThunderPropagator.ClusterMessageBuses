using FluentAssertions;
using ThunderPropagator.ClusterMessageBuses.Kafka;

namespace ThunderPropagator.UnitTests.Kafka;

public class KafkaTopicNamingTests
{
    private static readonly Guid ChannelKey = Guid.Parse("0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0");

    [Fact]
    public void FanOutTopic_IncludesPrefixAndChannelKey()
    {
        var topic = KafkaTopicNaming.FanOutTopic("prefix", ChannelKey);

        topic.Should().Be($"prefix.fanout.{ChannelKey:N}");
    }

    [Fact]
    public void SubscriptionEventTopic_IncludesPrefixAndChannelKey()
    {
        var topic = KafkaTopicNaming.SubscriptionEventTopic("prefix", ChannelKey);

        topic.Should().Be($"prefix.subscriptions.{ChannelKey:N}");
    }

    [Fact]
    public void RequestTopic_And_ReplyTopic_UseDistinctNamespacesForTheSameNode()
    {
        var endpoint = new Uri("https://follower1:5001/");

        var requestTopic = KafkaTopicNaming.RequestTopic("prefix", endpoint);
        var replyTopic = KafkaTopicNaming.ReplyTopic("prefix", endpoint);

        requestTopic.Should().Be("prefix.requests.follower1-5001");
        replyTopic.Should().Be("prefix.replies.follower1-5001");
        requestTopic.Should().NotBe(replyTopic);
    }

    [Fact]
    public void Slugify_DefaultPort_OmitsPortSegment()
    {
        var slug = KafkaTopicNaming.Slugify(new Uri("https://leader/"));

        slug.Should().Be("leader");
    }

    [Fact]
    public void Slugify_NonDefaultPort_AppendsPortSegment()
    {
        var slug = KafkaTopicNaming.Slugify(new Uri("https://leader:8443/"));

        slug.Should().Be("leader-8443");
    }

    [Fact]
    public void Slugify_IsCaseInsensitive()
    {
        var lower = KafkaTopicNaming.Slugify(new Uri("https://Follower-One:5001/"));
        var upper = KafkaTopicNaming.Slugify(new Uri("https://FOLLOWER-ONE:5001/"));

        lower.Should().Be(upper);
    }

    [Fact]
    public void Slugify_ReplacesNonAlphanumericCharactersWithHyphens()
    {
        // Uri normalizes most illegal host characters away, but underscores survive — verifying
        // the fallback branch of Slugify's per-character loop is actually exercised.
        var slug = KafkaTopicNaming.Slugify(new Uri("https://follower_one/"));

        slug.Should().Be("follower-one");
    }

    [Fact]
    public void RequestTopic_ForDifferentNodes_ProducesDifferentTopics()
    {
        var topicA = KafkaTopicNaming.RequestTopic("prefix", new Uri("https://node-a:5000/"));
        var topicB = KafkaTopicNaming.RequestTopic("prefix", new Uri("https://node-b:5000/"));

        topicA.Should().NotBe(topicB);
    }
}
