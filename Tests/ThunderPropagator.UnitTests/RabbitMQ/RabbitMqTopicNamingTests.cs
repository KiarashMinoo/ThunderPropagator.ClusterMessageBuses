using FluentAssertions;
using ThunderPropagator.ClusterMessageBuses.RabbitMQ;

namespace ThunderPropagator.UnitTests.RabbitMQ;

public class RabbitMqTopicNamingTests
{
    [Fact]
    public void FanOutExchange_IncludesPrefixAndChannelKey()
    {
        var channelKey = Guid.NewGuid();

        var exchange = RabbitMqTopicNaming.FanOutExchange("thunderpropagator.cluster", channelKey);

        exchange.Should().Be($"thunderpropagator.cluster.fanout.{channelKey:N}");
    }

    [Fact]
    public void SubscriptionEventExchange_IncludesPrefixAndChannelKey()
    {
        var channelKey = Guid.NewGuid();

        var exchange = RabbitMqTopicNaming.SubscriptionEventExchange("thunderpropagator.cluster", channelKey);

        exchange.Should().Be($"thunderpropagator.cluster.subscriptions.{channelKey:N}");
    }

    [Fact]
    public void FanOutExchange_AndSubscriptionEventExchange_NeverCollideForTheSameChannel()
    {
        var channelKey = Guid.NewGuid();

        var fanOut = RabbitMqTopicNaming.FanOutExchange("prefix", channelKey);
        var subscriptions = RabbitMqTopicNaming.SubscriptionEventExchange("prefix", channelKey);

        fanOut.Should().NotBe(subscriptions);
    }

    [Fact]
    public void RequestQueue_AndReplyQueue_NeverCollideForTheSameNode()
    {
        var endpoint = new Uri("https://node1:5001/");

        var request = RabbitMqTopicNaming.RequestQueue("prefix", endpoint);
        var reply = RabbitMqTopicNaming.ReplyQueue("prefix", endpoint);

        request.Should().NotBe(reply);
    }

    [Fact]
    public void Slugify_DefaultPort_OmitsPortSegment()
    {
        var slug = RabbitMqTopicNaming.Slugify(new Uri("https://follower1/"));

        slug.Should().Be("follower1");
    }

    [Fact]
    public void Slugify_NonDefaultPort_AppendsPortSegment()
    {
        var slug = RabbitMqTopicNaming.Slugify(new Uri("https://follower1:5001/"));

        slug.Should().Be("follower1-5001");
    }

    [Fact]
    public void Slugify_NonAlphanumericCharacters_AreReplacedWithHyphens()
    {
        var slug = RabbitMqTopicNaming.Slugify(new Uri("https://follower_one.example.com:5001/"));

        slug.Should().Be("follower-one-example-com-5001");
    }

    [Fact]
    public void Slugify_IsCaseInsensitive()
    {
        var lower = RabbitMqTopicNaming.Slugify(new Uri("https://Follower1:5001/"));
        var upper = RabbitMqTopicNaming.Slugify(new Uri("https://FOLLOWER1:5001/"));

        lower.Should().Be(upper);
    }

    [Fact]
    public void RequestQueue_DifferentNodes_ProduceDifferentQueueNames()
    {
        var first = RabbitMqTopicNaming.RequestQueue("prefix", new Uri("https://node1:5001/"));
        var second = RabbitMqTopicNaming.RequestQueue("prefix", new Uri("https://node2:5001/"));

        first.Should().NotBe(second);
    }
}
