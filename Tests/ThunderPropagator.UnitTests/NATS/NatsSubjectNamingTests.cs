using FluentAssertions;
using ThunderPropagator.ClusterMessageBuses.NATS;

namespace ThunderPropagator.UnitTests.NATS;

public class NatsSubjectNamingTests
{
    [Fact]
    public void FanOutSubject_IncludesPrefixAndChannelKey()
    {
        var channelKey = Guid.NewGuid();

        var subject = NatsSubjectNaming.FanOutSubject("thunderpropagator.cluster", channelKey);

        subject.Should().Be($"thunderpropagator.cluster.fanout.{channelKey:N}");
    }

    [Fact]
    public void SubscriptionEventSubject_IncludesPrefixAndChannelKey()
    {
        var channelKey = Guid.NewGuid();

        var subject = NatsSubjectNaming.SubscriptionEventSubject("thunderpropagator.cluster", channelKey);

        subject.Should().Be($"thunderpropagator.cluster.subscriptions.{channelKey:N}");
    }

    [Fact]
    public void FanOutSubject_AndSubscriptionEventSubject_NeverCollideForTheSameChannel()
    {
        var channelKey = Guid.NewGuid();

        var fanOut = NatsSubjectNaming.FanOutSubject("prefix", channelKey);
        var subscriptions = NatsSubjectNaming.SubscriptionEventSubject("prefix", channelKey);

        fanOut.Should().NotBe(subscriptions);
    }

    [Fact]
    public void Slugify_DefaultPort_OmitsPortSegment()
    {
        var slug = NatsSubjectNaming.Slugify(new Uri("https://follower1/"));

        slug.Should().Be("follower1");
    }

    [Fact]
    public void Slugify_NonDefaultPort_AppendsPortSegment()
    {
        var slug = NatsSubjectNaming.Slugify(new Uri("https://follower1:5001/"));

        slug.Should().Be("follower1-5001");
    }

    [Fact]
    public void Slugify_NonAlphanumericCharacters_AreReplacedWithHyphens()
    {
        var slug = NatsSubjectNaming.Slugify(new Uri("https://follower_one.example.com:5001/"));

        slug.Should().Be("follower-one-example-com-5001");
    }

    [Fact]
    public void RequestSubject_DifferentNodes_ProduceDifferentSubjects()
    {
        var first = NatsSubjectNaming.RequestSubject("prefix", new Uri("https://node1:5001/"));
        var second = NatsSubjectNaming.RequestSubject("prefix", new Uri("https://node2:5001/"));

        first.Should().NotBe(second);
    }
}
