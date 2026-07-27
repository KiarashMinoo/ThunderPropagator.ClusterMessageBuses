# RabbitMQ

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

The **RabbitMQ** area groups 2 documented types, including `RabbitMqClusterMessageBusExtensions`, `RabbitMqClusterMessageBusOptions`. It provides the contracts and implementation used by this part of ThunderPropagator.ClusterMessageBuses.

## Files

| File | Primary type(s)/symbol(s) | LOC (approx.) | Responsibility |
|---|---|---:|---|
| `AssemblyInfo.cs` | — | 4 | Contains the assembly info implementation or configuration. |
| `RabbitMqClusterMessageBus.cs` | `RabbitMqClusterMessageBus`, `Log` | 235 | Defines RabbitMqClusterMessageBus, Log and its related behavior. |
| `RabbitMqClusterMessageBus.FanOut.cs` | `RabbitMqClusterMessageBus`, `FanOutSubscription`, `Log` | 151 | Defines RabbitMqClusterMessageBus, FanOutSubscription, Log and its related behavior. |
| `RabbitMqClusterMessageBus.RequestReply.cs` | `RabbitMqClusterMessageBus`, `Log` | 210 | Defines RabbitMqClusterMessageBus, Log and its related behavior. |
| `RabbitMqClusterMessageBus.Snapshots.cs` | `RabbitMqClusterMessageBus`, `Log` | 96 | Defines RabbitMqClusterMessageBus, Log and its related behavior. |
| `RabbitMqClusterMessageBus.SubscriptionFetch.cs` | `RabbitMqClusterMessageBus`, `Log` | 42 | Defines RabbitMqClusterMessageBus, Log and its related behavior. |
| `RabbitMqClusterMessageBus.SubscriptionSync.cs` | `RabbitMqClusterMessageBus`, `SubscriptionEventSubscription`, `Log` | 147 | Defines RabbitMqClusterMessageBus, SubscriptionEventSubscription, Log and its related behavior. |
| `RabbitMqClusterMessageBusExtensions.cs` | `RabbitMqClusterMessageBusExtensions` | 48 | Defines RabbitMqClusterMessageBusExtensions and its related behavior. |
| `RabbitMqClusterMessageBusOptions.cs` | `RabbitMqClusterMessageBusOptions` | 36 | Defines RabbitMqClusterMessageBusOptions and its related behavior. |
| `RabbitMqClusterRequestEnvelope.cs` | `RabbitMqClusterRequestEnvelope` | 39 | Defines RabbitMqClusterRequestEnvelope and its related behavior. |
| `RabbitMqClusterRequestKind.cs` | `RabbitMqClusterRequestKind` | 13 | Defines RabbitMqClusterRequestKind and its related behavior. |
| `RabbitMqClusterResponseEnvelope.cs` | `RabbitMqClusterResponseEnvelope` | 25 | Defines RabbitMqClusterResponseEnvelope and its related behavior. |
| `RabbitMqSnapshotDeltaPayload.cs` | `RabbitMqSnapshotDeltaPayload` | 15 | Defines RabbitMqSnapshotDeltaPayload and its related behavior. |
| `RabbitMqTopicNaming.cs` | `RabbitMqTopicNaming` | 59 | Defines RabbitMqTopicNaming and its related behavior. |
| `ThunderPropagator.ClusterMessageBuses.RabbitMQ.csproj` | — | 13 | Defines project build targets, dependencies, and package metadata. |

## Types and Members

| Type | Kind | Summary | Inherits/Implements | Key Members |
|---|---|---|---|---|
| [`RabbitMqClusterMessageBusExtensions`](#rabbitmqclustermessagebusextensions) | class | DI registration for the RabbitMQ transport. | — | `AddClusterRabbitMqMessageBus(…)` |
| [`RabbitMqClusterMessageBusOptions`](#rabbitmqclustermessagebusoptions) | class | Configuration for . | — | `ConnectionString`, `ExchangePrefix`, `RequestTimeout`, `ConfigureConnectionFactory` |

### RabbitMqClusterMessageBusExtensions

- **Kind:** class
- **Namespace:** `ThunderPropagator.ClusterMessageBuses.RabbitMQ`
- **Inherits/implements:** None declared
- **Attributes:** None detected
- **Key members:** `AddClusterRabbitMqMessageBus(…)`
- **Summary:** DI registration for the RabbitMQ transport.
- **Thread safety:** Follow the lifetime and concurrency guarantees of the owning component; no additional guarantee is inferred.

**Usage recipe**

```csharp
// Resolve RabbitMqClusterMessageBusExtensions from the configured service container or construct it with its declared dependencies.
```

[↑ Back to top](#contents)

### RabbitMqClusterMessageBusOptions

- **Kind:** class
- **Namespace:** `ThunderPropagator.ClusterMessageBuses.RabbitMQ`
- **Inherits/implements:** None declared
- **Attributes:** None detected
- **Key members:** `ConnectionString`, `ExchangePrefix`, `RequestTimeout`, `ConfigureConnectionFactory`
- **Summary:** Configuration for .
- **Thread safety:** Follow the lifetime and concurrency guarantees of the owning component; no additional guarantee is inferred.

**Usage recipe**

```csharp
// Resolve RabbitMqClusterMessageBusOptions from the configured service container or construct it with its declared dependencies.
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
  Current["RabbitMQ"]
  Current --> T0["RabbitMqClusterMessageBusExtensions"]
  Current --> T1["RabbitMqClusterMessageBusOptions"]
```

The diagram shows the direct components documented by the **RabbitMQ** area.

## Examples

Start with `RabbitMqClusterMessageBusExtensions` as the primary entry point for this folder, then follow its linked contracts and collaborators.

## See Also

- [Documentation home](../README.md)
- [ActiveMQ](../ActiveMQ/README.md)
- [AwsSqs](../AwsSqs/README.md)
- [AzureServiceBus](../AzureServiceBus/README.md)
- [Kafka](../Kafka/README.md)
- [Mqtt](../Mqtt/README.md)
- [NATS](../NATS/README.md)
- [Pulsar](../Pulsar/README.md)
- [RedisPubSub](../RedisPubSub/README.md)
- [SharedKernel](../SharedKernel/README.md)
- [TcpSocket](../TcpSocket/README.md)
- [UdpClient](../UdpClient/README.md)
- [WebApi](../WebApi/README.md)
- [WebSocket](../WebSocket/README.md)

[↑ Back to top](#contents)
