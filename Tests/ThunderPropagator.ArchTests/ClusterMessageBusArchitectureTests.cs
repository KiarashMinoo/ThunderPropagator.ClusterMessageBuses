using System.Reflection;
using FluentAssertions;
using NetArchTest.Rules;
using ThunderPropagator.ClusterMessageBuses.ActiveMQ;
using ThunderPropagator.ClusterMessageBuses.Kafka;
using ThunderPropagator.ClusterMessageBuses.Mqtt;
using ThunderPropagator.ClusterMessageBuses.NATS;
using ThunderPropagator.ClusterMessageBuses.Pulsar;
using ThunderPropagator.ClusterMessageBuses.RabbitMQ;
using ThunderPropagator.ClusterMessageBuses.RedisPubSub;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;
using ThunderPropagator.ClusterMessageBuses.WebSocket;

namespace ThunderPropagator.ArchTests;

/// <summary>
/// Architecture rules for ThunderPropagator.ClusterMessageBuses, matching the three rules already
/// enforced in ThunderPropagator.RecoveryHandlers and ThunderPropagator.Feeviders: namespace
/// containment per assembly, no sibling-transport cross-dependencies, and an acyclic dependency
/// graph across the whole repo.
///
/// Kafka, RabbitMQ, NATS, Pulsar, MQTT, ActiveMQ, RedisPubSub, and WebSocket are the first eight
/// transport projects (the rest of the roadmap in CLAUDE.md, plus gRPC #2 and ZeroMQ #3, are still
/// to come). <see cref="ClusterMessageBusAssemblies" /> and <see cref="ForbiddenDependencies" /> are
/// written so adding another transport later is just a new TheoryData row, not a new test method.
/// </summary>
public class ClusterMessageBusArchitectureTests
{
    private const string SharedKernelNamespace = "ThunderPropagator.ClusterMessageBuses.SharedKernel";
    private const string KafkaNamespace = "ThunderPropagator.ClusterMessageBuses.Kafka";
    private const string RabbitMqNamespace = "ThunderPropagator.ClusterMessageBuses.RabbitMQ";
    private const string NatsNamespace = "ThunderPropagator.ClusterMessageBuses.NATS";
    private const string PulsarNamespace = "ThunderPropagator.ClusterMessageBuses.Pulsar";
    private const string MqttNamespace = "ThunderPropagator.ClusterMessageBuses.Mqtt";
    private const string ActiveMqNamespace = "ThunderPropagator.ClusterMessageBuses.ActiveMQ";
    private const string RedisPubSubNamespace = "ThunderPropagator.ClusterMessageBuses.RedisPubSub";
    private const string WebSocketNamespace = "ThunderPropagator.ClusterMessageBuses.WebSocket";

    public static TheoryData<Assembly, string> ClusterMessageBusAssemblies => new()
    {
        { typeof(ThunderPropagatorExtensions).Assembly, SharedKernelNamespace },
        { typeof(KafkaClusterMessageBusExtensions).Assembly, KafkaNamespace },
        { typeof(RabbitMqClusterMessageBusExtensions).Assembly, RabbitMqNamespace },
        { typeof(NatsClusterMessageBusExtensions).Assembly, NatsNamespace },
        { typeof(PulsarClusterMessageBusExtensions).Assembly, PulsarNamespace },
        { typeof(MqttClusterMessageBusExtensions).Assembly, MqttNamespace },
        { typeof(ActiveMqClusterMessageBusExtensions).Assembly, ActiveMqNamespace },
        { typeof(RedisPubSubClusterMessageBusExtensions).Assembly, RedisPubSubNamespace },
        { typeof(WebSocketClusterMessageBusExtensions).Assembly, WebSocketNamespace },
    };

    [Theory]
    [MemberData(nameof(ClusterMessageBusAssemblies))]
    public void Assemblies_WhenInspected_MustKeepTypesInExpectedNamespace(
        Assembly assembly,
        string expectedNamespace)
    {
        // Arrange
        var productionTypes = assembly.DefinedTypes
            .Where(type => !type.IsNested && !type.Name.StartsWith('<'))
            .ToArray();

        // Act
        var misplacedTypes = productionTypes
            .Where(type => type.Namespace is null ||
                           !type.Namespace.StartsWith(expectedNamespace, StringComparison.Ordinal))
            .Select(type => type.FullName)
            .ToArray();

        // Assert
        misplacedTypes.Should().BeEmpty();
    }

    // Now that a second real transport assembly exists (RabbitMQ, alongside Kafka), the
    // sibling-cross-dependency rule has something meaningful to check. Add one more row per
    // transport as gRPC (#2), ZeroMQ (#3), and the rest of the CLAUDE.md roadmap land — each row
    // forbids every *other* transport's namespace.
    public static TheoryData<Assembly, string[]> ForbiddenDependencies => new()
    {
        { typeof(KafkaClusterMessageBusExtensions).Assembly, [RabbitMqNamespace, NatsNamespace, PulsarNamespace, MqttNamespace, ActiveMqNamespace, RedisPubSubNamespace, WebSocketNamespace] },
        { typeof(RabbitMqClusterMessageBusExtensions).Assembly, [KafkaNamespace, NatsNamespace, PulsarNamespace, MqttNamespace, ActiveMqNamespace, RedisPubSubNamespace, WebSocketNamespace] },
        { typeof(NatsClusterMessageBusExtensions).Assembly, [KafkaNamespace, RabbitMqNamespace, PulsarNamespace, MqttNamespace, ActiveMqNamespace, RedisPubSubNamespace, WebSocketNamespace] },
        { typeof(PulsarClusterMessageBusExtensions).Assembly, [KafkaNamespace, RabbitMqNamespace, NatsNamespace, MqttNamespace, ActiveMqNamespace, RedisPubSubNamespace, WebSocketNamespace] },
        { typeof(MqttClusterMessageBusExtensions).Assembly, [KafkaNamespace, RabbitMqNamespace, NatsNamespace, PulsarNamespace, ActiveMqNamespace, RedisPubSubNamespace, WebSocketNamespace] },
        { typeof(ActiveMqClusterMessageBusExtensions).Assembly, [KafkaNamespace, RabbitMqNamespace, NatsNamespace, PulsarNamespace, MqttNamespace, RedisPubSubNamespace, WebSocketNamespace] },
        { typeof(RedisPubSubClusterMessageBusExtensions).Assembly, [KafkaNamespace, RabbitMqNamespace, NatsNamespace, PulsarNamespace, MqttNamespace, ActiveMqNamespace, WebSocketNamespace] },
        { typeof(WebSocketClusterMessageBusExtensions).Assembly, [KafkaNamespace, RabbitMqNamespace, NatsNamespace, PulsarNamespace, MqttNamespace, ActiveMqNamespace, RedisPubSubNamespace] },
    };

    [Theory]
    [MemberData(nameof(ForbiddenDependencies))]
    public void Assemblies_WhenInspected_MustNotDependOnSiblingTransports(
        Assembly assembly,
        string[] forbiddenNamespaces)
    {
        var result = Types.InAssembly(assembly).Should().NotHaveDependencyOnAny(forbiddenNamespaces).GetResult();
        result.IsSuccessful.Should().BeTrue(
            "transport implementations must remain independently deployable; failing types: {0}",
            string.Join(", ", result.FailingTypeNames ?? []));
    }

    [Fact]
    public void ClusterMessageBusAssemblies_WhenDependencyGraphIsInspected_MustBeAcyclic()
    {
        // Arrange
        var assemblies = ClusterMessageBusAssemblies
            .Select(row => (Assembly)row[0])
            .Distinct()
            .ToDictionary(assembly => assembly.GetName().Name!, StringComparer.Ordinal);

        // Act
        var hasCycle = assemblies.Keys.Any(name =>
            HasCycle(name, assemblies, [], []));

        // Assert
        hasCycle.Should().BeFalse("cluster-message-bus project references must form an acyclic graph");
    }

    // path.Add/path.Remove are correctly balanced in every branch below — the early
    // "if (!path.Add(assemblyName)) return true" only fires when this frame did NOT add the entry
    // (it was already on the path from an ancestor call), so there is nothing for this frame to
    // remove; the ancestor frame that owns the entry removes it. The try/finally is defense-in-depth
    // so a thrown exception mid-recursion (e.g. from GetReferencedAssemblies) can't leave a stale
    // entry in `path` for a caller that reuses the same set.
    private static bool HasCycle(
        string assemblyName,
        IReadOnlyDictionary<string, Assembly> assemblies,
        HashSet<string> visited,
        HashSet<string> path)
    {
        if (!path.Add(assemblyName))
            return true;

        try
        {
            if (!visited.Add(assemblyName))
                return false;

            return assemblies[assemblyName]
                .GetReferencedAssemblies()
                .Select(reference => reference.Name)
                .Where(reference => reference is not null && assemblies.ContainsKey(reference))
                .Any(reference => HasCycle(reference!, assemblies, visited, path));
        }
        finally
        {
            path.Remove(assemblyName);
        }
    }
}
