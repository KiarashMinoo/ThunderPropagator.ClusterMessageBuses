# RedisPubSub

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

The **RedisPubSub** area groups 2 documented types, including `RedisPubSubClusterMessageBusExtensions`, `RedisPubSubClusterMessageBusOptions`. It provides the contracts and implementation used by this part of ThunderPropagator.ClusterMessageBuses.

## Files

| File | Primary type(s)/symbol(s) | LOC (approx.) | Responsibility |
|---|---|---:|---|
| `AssemblyInfo.cs` | — | 4 | Contains the assembly info implementation or configuration. |
| `RedisChannelNaming.cs` | `RedisChannelNaming` | 52 | Defines RedisChannelNaming and its related behavior. |
| `RedisPubSubClusterMessageBus.cs` | `RedisPubSubClusterMessageBus`, `Log` | 229 | Defines RedisPubSubClusterMessageBus, Log and its related behavior. |
| `RedisPubSubClusterMessageBus.FanOut.cs` | `RedisPubSubClusterMessageBus`, `FanOutSubscription`, `Log` | 129 | Defines RedisPubSubClusterMessageBus, FanOutSubscription, Log and its related behavior. |
| `RedisPubSubClusterMessageBus.RequestReply.cs` | `RedisPubSubClusterMessageBus`, `Log` | 191 | Defines RedisPubSubClusterMessageBus, Log and its related behavior. |
| `RedisPubSubClusterMessageBus.Snapshots.cs` | `RedisPubSubClusterMessageBus`, `Log` | 96 | Defines RedisPubSubClusterMessageBus, Log and its related behavior. |
| `RedisPubSubClusterMessageBus.SubscriptionFetch.cs` | `RedisPubSubClusterMessageBus`, `Log` | 42 | Defines RedisPubSubClusterMessageBus, Log and its related behavior. |
| `RedisPubSubClusterMessageBus.SubscriptionSync.cs` | `RedisPubSubClusterMessageBus`, `SubscriptionEventSubscription`, `Log` | 125 | Defines RedisPubSubClusterMessageBus, SubscriptionEventSubscription, Log and its related behavior. |
| `RedisPubSubClusterMessageBusExtensions.cs` | `RedisPubSubClusterMessageBusExtensions` | 50 | Defines RedisPubSubClusterMessageBusExtensions and its related behavior. |
| `RedisPubSubClusterMessageBusOptions.cs` | `RedisPubSubClusterMessageBusOptions` | 36 | Defines RedisPubSubClusterMessageBusOptions and its related behavior. |
| `RedisPubSubClusterRequestEnvelope.cs` | `RedisPubSubClusterRequestEnvelope` | 42 | Defines RedisPubSubClusterRequestEnvelope and its related behavior. |
| `RedisPubSubClusterRequestKind.cs` | `RedisPubSubClusterRequestKind` | 13 | Defines RedisPubSubClusterRequestKind and its related behavior. |
| `RedisPubSubClusterResponseEnvelope.cs` | `RedisPubSubClusterResponseEnvelope` | 25 | Defines RedisPubSubClusterResponseEnvelope and its related behavior. |
| `RedisPubSubSnapshotDeltaPayload.cs` | `RedisPubSubSnapshotDeltaPayload` | 15 | Defines RedisPubSubSnapshotDeltaPayload and its related behavior. |
| `ThunderPropagator.ClusterMessageBuses.RedisPubSub.csproj` | — | 13 | Defines project build targets, dependencies, and package metadata. |

## Types and Members

| Type | Kind | Summary | Inherits/Implements | Key Members |
|---|---|---|---|---|
| [`RedisPubSubClusterMessageBusExtensions`](#redispubsubclustermessagebusextensions) | class | DI registration for the Redis pub/sub transport. | — | `AddClusterRedisPubSubMessageBus(…)` |
| [`RedisPubSubClusterMessageBusOptions`](#redispubsubclustermessagebusoptions) | class | Configuration for . | — | `ConnectionString`, `ChannelPrefix`, `RequestTimeout`, `ConfigureOptions` |

### RedisPubSubClusterMessageBusExtensions

- **Kind:** class
- **Namespace:** `ThunderPropagator.ClusterMessageBuses.RedisPubSub`
- **Inherits/implements:** None declared
- **Attributes:** None detected
- **Key members:** `AddClusterRedisPubSubMessageBus(…)`
- **Summary:** DI registration for the Redis pub/sub transport.
- **Thread safety:** Follow the lifetime and concurrency guarantees of the owning component; no additional guarantee is inferred.

**Usage recipe**

```csharp
// Resolve RedisPubSubClusterMessageBusExtensions from the configured service container or construct it with its declared dependencies.
```

[↑ Back to top](#contents)

### RedisPubSubClusterMessageBusOptions

- **Kind:** class
- **Namespace:** `ThunderPropagator.ClusterMessageBuses.RedisPubSub`
- **Inherits/implements:** None declared
- **Attributes:** None detected
- **Key members:** `ConnectionString`, `ChannelPrefix`, `RequestTimeout`, `ConfigureOptions`
- **Summary:** Configuration for .
- **Thread safety:** Follow the lifetime and concurrency guarantees of the owning component; no additional guarantee is inferred.

**Usage recipe**

```csharp
// Resolve RedisPubSubClusterMessageBusOptions from the configured service container or construct it with its declared dependencies.
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
  Current["RedisPubSub"]
  Current --> T0["RedisPubSubClusterMessageBusExtensions"]
  Current --> T1["RedisPubSubClusterMessageBusOptions"]
```

The diagram shows the direct components documented by the **RedisPubSub** area.

## Examples

Start with `RedisPubSubClusterMessageBusExtensions` as the primary entry point for this folder, then follow its linked contracts and collaborators.

## See Also

- [Documentation home](../README.md)
- [ActiveMQ](../ActiveMQ/README.md)
- [AwsSqs](../AwsSqs/README.md)
- [AzureServiceBus](../AzureServiceBus/README.md)
- [Kafka](../Kafka/README.md)
- [Mqtt](../Mqtt/README.md)
- [NATS](../NATS/README.md)
- [Pulsar](../Pulsar/README.md)
- [RabbitMQ](../RabbitMQ/README.md)
- [SharedKernel](../SharedKernel/README.md)
- [TcpSocket](../TcpSocket/README.md)
- [UdpClient](../UdpClient/README.md)
- [WebApi](../WebApi/README.md)
- [WebSocket](../WebSocket/README.md)

[↑ Back to top](#contents)
