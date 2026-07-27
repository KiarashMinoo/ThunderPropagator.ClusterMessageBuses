# Kafka

## Contents

- [Overview](#overview)
- [Files](#files)
- [Types and Members](#types-and-members)
- [Serialization and Contracts](#serialization-and-contracts)
- [Validation and Constraints](#validation-and-constraints)
- [Performance Notes](#performance-notes)
- [Package Dependencies](#package-dependencies)
- [Diagrams](#diagrams)
- [Examples](#examples)
- [See Also](#see-also)

## Overview

The **Kafka** area groups 2 documented types, including `KafkaClusterMessageBusExtensions`, `KafkaClusterMessageBusOptions`. It provides the contracts and implementation used by this part of ThunderPropagator.ClusterMessageBuses.

## Files

| File | Primary type(s)/symbol(s) | LOC (approx.) | Responsibility |
|---|---|---:|---|
| `AssemblyInfo.cs` | — | 4 | Contains the assembly info implementation or configuration. |
| `ChannelManagerResolver.cs` | — | 4 | Contains the channel manager resolver implementation or configuration. |
| `IKafkaChannelResolver.cs` | — | 4 | Contains the ikafka channel resolver implementation or configuration. |
| `KafkaClusterMessageBus.cs` | `KafkaClusterMessageBus`, `Log` | 164 | Defines KafkaClusterMessageBus, Log and its related behavior. |
| `KafkaClusterMessageBus.FanOut.cs` | `KafkaClusterMessageBus`, `FanOutSubscription`, `Log` | 157 | Defines KafkaClusterMessageBus, FanOutSubscription, Log and its related behavior. |
| `KafkaClusterMessageBus.RequestReply.cs` | `KafkaClusterMessageBus`, `Log` | 242 | Defines KafkaClusterMessageBus, Log and its related behavior. |
| `KafkaClusterMessageBus.Snapshots.cs` | `KafkaClusterMessageBus`, `Log` | 96 | Defines KafkaClusterMessageBus, Log and its related behavior. |
| `KafkaClusterMessageBus.SubscriptionFetch.cs` | `KafkaClusterMessageBus`, `Log` | 42 | Defines KafkaClusterMessageBus, Log and its related behavior. |
| `KafkaClusterMessageBus.SubscriptionSync.cs` | `KafkaClusterMessageBus`, `SubscriptionEventSubscription`, `Log` | 155 | Defines KafkaClusterMessageBus, SubscriptionEventSubscription, Log and its related behavior. |
| `KafkaClusterMessageBusExtensions.cs` | `KafkaClusterMessageBusExtensions` | 48 | Defines KafkaClusterMessageBusExtensions and its related behavior. |
| `KafkaClusterMessageBusOptions.cs` | `KafkaClusterMessageBusOptions` | 53 | Defines KafkaClusterMessageBusOptions and its related behavior. |
| `KafkaClusterRequestEnvelope.cs` | `KafkaClusterRequestEnvelope` | 42 | Defines KafkaClusterRequestEnvelope and its related behavior. |
| `KafkaClusterRequestKind.cs` | `KafkaClusterRequestKind` | 13 | Defines KafkaClusterRequestKind and its related behavior. |
| `KafkaClusterResponseEnvelope.cs` | `KafkaClusterResponseEnvelope` | 26 | Defines KafkaClusterResponseEnvelope and its related behavior. |
| `KafkaSnapshotDeltaPayload.cs` | `KafkaSnapshotDeltaPayload` | 17 | Defines KafkaSnapshotDeltaPayload and its related behavior. |
| `KafkaTopicNaming.cs` | `KafkaTopicNaming` | 57 | Defines KafkaTopicNaming and its related behavior. |
| `ThunderPropagator.ClusterMessageBuses.Kafka.csproj` | — | 13 | Defines project build targets, dependencies, and package metadata. |

## Types and Members

| Type | Kind | Summary | Inherits/Implements | Key Members |
|---|---|---|---|---|
| [`KafkaClusterMessageBusExtensions`](#kafkaclustermessagebusextensions) | class | DI registration for the Kafka transport. | — | `AddClusterKafkaMessageBus(…)` |
| [`KafkaClusterMessageBusOptions`](#kafkaclustermessagebusoptions) | class | Configuration for . | — | `BootstrapServers`, `TopicPrefix`, `ConsumerGroupPrefix`, `RequestTimeout`, `ConfigureProducer`, `ConfigureConsumer` |

### KafkaClusterMessageBusExtensions

- **Kind:** class
- **Namespace:** `ThunderPropagator.ClusterMessageBuses.Kafka`
- **Inherits/implements:** None declared
- **Attributes:** None detected
- **Key members:** `AddClusterKafkaMessageBus(…)`
- **Summary:** DI registration for the Kafka transport.
- **Thread safety:** Follow the lifetime and concurrency guarantees of the owning component; no additional guarantee is inferred.

**Usage recipe**

```csharp
// Resolve KafkaClusterMessageBusExtensions from the configured service container or construct it with its declared dependencies.
```

[↑ Back to top](#contents)

### KafkaClusterMessageBusOptions

- **Kind:** class
- **Namespace:** `ThunderPropagator.ClusterMessageBuses.Kafka`
- **Inherits/implements:** None declared
- **Attributes:** None detected
- **Key members:** `BootstrapServers`, `TopicPrefix`, `ConsumerGroupPrefix`, `RequestTimeout`, `ConfigureProducer`, `ConfigureConsumer`
- **Summary:** Configuration for .
- **Thread safety:** Follow the lifetime and concurrency guarantees of the owning component; no additional guarantee is inferred.

**Usage recipe**

```csharp
// Resolve KafkaClusterMessageBusOptions from the configured service container or construct it with its declared dependencies.
```

[↑ Back to top](#contents)

## Serialization and Contracts

Serialization behavior is part of the public wire or persistence contract in this area. Preserve field names, ordering rules, content negotiation, and backward-compatibility expectations when changing these types.

## Validation and Constraints

Inputs are validated at component boundaries. Callers should provide non-null required values and handle domain or argument exceptions without retrying invalid requests unchanged.

## Performance Notes

This area contains performance-sensitive constructs such as pooled buffers, spans, asynchronous value types, or concurrent collections. Avoid unnecessary allocations and blocking calls on streaming or message-processing paths.

## Package Dependencies

| Package | Version | Description | Links |
|---|---|---|---|
| `Apache.NMS` | `2.2.0` | External dependency used by the repository. | [Registry](https://www.nuget.org/packages/Apache.NMS) |
| `Apache.NMS.ActiveMQ` | `2.2.0` | External dependency used by the repository. | [Registry](https://www.nuget.org/packages/Apache.NMS.ActiveMQ) |
| `AWSSDK.SimpleNotificationService` | `4.0.100.6` | External dependency used by the repository. | [Registry](https://www.nuget.org/packages/AWSSDK.SimpleNotificationService) |
| `AWSSDK.SQS` | `4.0.100.5` | External dependency used by the repository. | [Registry](https://www.nuget.org/packages/AWSSDK.SQS) |
| `Azure.Messaging.ServiceBus` | `7.20.2` | External dependency used by the repository. | [Registry](https://www.nuget.org/packages/Azure.Messaging.ServiceBus) |
| `Confluent.Kafka` | `2.15.0` | External dependency used by the repository. | [Registry](https://www.nuget.org/packages/Confluent.Kafka) |
| `DotPulsar` | `5.3.1` | External dependency used by the repository. | [Registry](https://www.nuget.org/packages/DotPulsar) |
| `Microsoft.Extensions.Logging.Abstractions` | `10.*` | External dependency used by the repository. | [Registry](https://www.nuget.org/packages/Microsoft.Extensions.Logging.Abstractions) |
| `Microsoft.Extensions.Options` | `10.*` | External dependency used by the repository. | [Registry](https://www.nuget.org/packages/Microsoft.Extensions.Options) |
| `MQTTnet` | `5.2.0.1603` | External dependency used by the repository. | [Registry](https://www.nuget.org/packages/MQTTnet) |
| `NATS.Net` | `3.0.1` | External dependency used by the repository. | [Registry](https://www.nuget.org/packages/NATS.Net) |
| `Polly.Core` | `8.6.4` | External dependency used by the repository. | [Registry](https://www.nuget.org/packages/Polly.Core) |
| `RabbitMQ.Client` | `7.2.1` | External dependency used by the repository. | [Registry](https://www.nuget.org/packages/RabbitMQ.Client) |
| `StackExchange.Redis` | `3.0.17` | External dependency used by the repository. | [Registry](https://www.nuget.org/packages/StackExchange.Redis) |

## Diagrams

### Component overview

```mermaid
graph TD
  Current["Kafka"]
  Current --> T0["KafkaClusterMessageBusExtensions"]
  Current --> T1["KafkaClusterMessageBusOptions"]
```

The diagram shows the direct components documented by the **Kafka** area.

## Examples

Start with `KafkaClusterMessageBusExtensions` as the primary entry point for this folder, then follow its linked contracts and collaborators.

## See Also

- [Documentation home](../README.md)
- [ActiveMQ](../ActiveMQ/README.md)
- [AwsSqs](../AwsSqs/README.md)
- [AzureServiceBus](../AzureServiceBus/README.md)
- [Mqtt](../Mqtt/README.md)
- [NATS](../NATS/README.md)
- [Pulsar](../Pulsar/README.md)
- [RabbitMQ](../RabbitMQ/README.md)
- [RedisPubSub](../RedisPubSub/README.md)
- [SharedKernel](../SharedKernel/README.md)
- [TcpSocket](../TcpSocket/README.md)
- [UdpClient](../UdpClient/README.md)
- [WebApi](../WebApi/README.md)
- [WebSocket](../WebSocket/README.md)

[↑ Back to top](#contents)
