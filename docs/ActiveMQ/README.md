# ActiveMQ

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

The **ActiveMQ** area groups 2 documented types, including `ActiveMqClusterMessageBusExtensions`, `ActiveMqClusterMessageBusOptions`. It provides the contracts and implementation used by this part of ThunderPropagator.ClusterMessageBuses.

## Files

| File | Primary type(s)/symbol(s) | LOC (approx.) | Responsibility |
|---|---|---:|---|
| `ActiveMqClusterMessageBus.cs` | `ActiveMqClusterMessageBus`, `Log` | 251 | Defines ActiveMqClusterMessageBus, Log and its related behavior. |
| `ActiveMqClusterMessageBus.FanOut.cs` | `ActiveMqClusterMessageBus`, `FanOutSubscription`, `Log` | 145 | Defines ActiveMqClusterMessageBus, FanOutSubscription, Log and its related behavior. |
| `ActiveMqClusterMessageBus.RequestReply.cs` | `ActiveMqClusterMessageBus`, `Log` | 208 | Defines ActiveMqClusterMessageBus, Log and its related behavior. |
| `ActiveMqClusterMessageBus.Snapshots.cs` | `ActiveMqClusterMessageBus`, `Log` | 96 | Defines ActiveMqClusterMessageBus, Log and its related behavior. |
| `ActiveMqClusterMessageBus.SubscriptionFetch.cs` | `ActiveMqClusterMessageBus`, `Log` | 42 | Defines ActiveMqClusterMessageBus, Log and its related behavior. |
| `ActiveMqClusterMessageBus.SubscriptionSync.cs` | `ActiveMqClusterMessageBus`, `SubscriptionEventSubscription`, `Log` | 141 | Defines ActiveMqClusterMessageBus, SubscriptionEventSubscription, Log and its related behavior. |
| `ActiveMqClusterMessageBusExtensions.cs` | `ActiveMqClusterMessageBusExtensions` | 50 | Defines ActiveMqClusterMessageBusExtensions and its related behavior. |
| `ActiveMqClusterMessageBusOptions.cs` | `ActiveMqClusterMessageBusOptions` | 36 | Defines ActiveMqClusterMessageBusOptions and its related behavior. |
| `ActiveMqClusterRequestEnvelope.cs` | `ActiveMqClusterRequestEnvelope` | 41 | Defines ActiveMqClusterRequestEnvelope and its related behavior. |
| `ActiveMqClusterRequestKind.cs` | `ActiveMqClusterRequestKind` | 13 | Defines ActiveMqClusterRequestKind and its related behavior. |
| `ActiveMqClusterResponseEnvelope.cs` | `ActiveMqClusterResponseEnvelope` | 25 | Defines ActiveMqClusterResponseEnvelope and its related behavior. |
| `ActiveMqSnapshotDeltaPayload.cs` | `ActiveMqSnapshotDeltaPayload` | 15 | Defines ActiveMqSnapshotDeltaPayload and its related behavior. |
| `ActiveMqTopicNaming.cs` | `ActiveMqTopicNaming` | 52 | Defines ActiveMqTopicNaming and its related behavior. |
| `AssemblyInfo.cs` | — | 4 | Contains the assembly info implementation or configuration. |
| `ThunderPropagator.ClusterMessageBuses.ActiveMQ.csproj` | — | 14 | Defines project build targets, dependencies, and package metadata. |

## Types and Members

| Type | Kind | Summary | Inherits/Implements | Key Members |
|---|---|---|---|---|
| [`ActiveMqClusterMessageBusExtensions`](#activemqclustermessagebusextensions) | class | DI registration for the ActiveMQ transport. | — | `AddClusterActiveMqMessageBus(…)` |
| [`ActiveMqClusterMessageBusOptions`](#activemqclustermessagebusoptions) | class | Configuration for . | — | `BrokerUri`, `TopicPrefix`, `RequestTimeout`, `ConfigureConnectionFactory` |

### ActiveMqClusterMessageBusExtensions

- **Kind:** class
- **Namespace:** `ThunderPropagator.ClusterMessageBuses.ActiveMQ`
- **Inherits/implements:** None declared
- **Attributes:** None detected
- **Key members:** `AddClusterActiveMqMessageBus(…)`
- **Summary:** DI registration for the ActiveMQ transport.
- **Thread safety:** Follow the lifetime and concurrency guarantees of the owning component; no additional guarantee is inferred.

**Usage recipe**

```csharp
// Resolve ActiveMqClusterMessageBusExtensions from the configured service container or construct it with its declared dependencies.
```

[↑ Back to top](#contents)

### ActiveMqClusterMessageBusOptions

- **Kind:** class
- **Namespace:** `ThunderPropagator.ClusterMessageBuses.ActiveMQ`
- **Inherits/implements:** None declared
- **Attributes:** None detected
- **Key members:** `BrokerUri`, `TopicPrefix`, `RequestTimeout`, `ConfigureConnectionFactory`
- **Summary:** Configuration for .
- **Thread safety:** Follow the lifetime and concurrency guarantees of the owning component; no additional guarantee is inferred.

**Usage recipe**

```csharp
// Resolve ActiveMqClusterMessageBusOptions from the configured service container or construct it with its declared dependencies.
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
  Current["ActiveMQ"]
  Current --> T0["ActiveMqClusterMessageBusExtensions"]
  Current --> T1["ActiveMqClusterMessageBusOptions"]
```

The diagram shows the direct components documented by the **ActiveMQ** area.

## Examples

Start with `ActiveMqClusterMessageBusExtensions` as the primary entry point for this folder, then follow its linked contracts and collaborators.

## See Also

- [Documentation home](../README.md)
- [AwsSqs](../AwsSqs/README.md)
- [AzureServiceBus](../AzureServiceBus/README.md)
- [Kafka](../Kafka/README.md)
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
