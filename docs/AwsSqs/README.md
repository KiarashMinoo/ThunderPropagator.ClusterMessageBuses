# AwsSqs

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

The **AwsSqs** area groups 2 documented types, including `AwsSqsClusterMessageBusExtensions`, `AwsSqsClusterMessageBusOptions`. It provides the contracts and implementation used by this part of ThunderPropagator.ClusterMessageBuses.

## Files

| File | Primary type(s)/symbol(s) | LOC (approx.) | Responsibility |
|---|---|---:|---|
| `AssemblyInfo.cs` | — | 4 | Contains the assembly info implementation or configuration. |
| `AwsSqsClusterMessageBus.cs` | `AwsSqsClusterMessageBus`, `Log` | 377 | Defines AwsSqsClusterMessageBus, Log and its related behavior. |
| `AwsSqsClusterMessageBus.FanOut.cs` | `AwsSqsClusterMessageBus`, `FanOutSubscription`, `Log` | 161 | Defines AwsSqsClusterMessageBus, FanOutSubscription, Log and its related behavior. |
| `AwsSqsClusterMessageBus.RequestReply.cs` | `AwsSqsClusterMessageBus`, `Log` | 199 | Defines AwsSqsClusterMessageBus, Log and its related behavior. |
| `AwsSqsClusterMessageBus.Snapshots.cs` | `AwsSqsClusterMessageBus`, `Log` | 96 | Defines AwsSqsClusterMessageBus, Log and its related behavior. |
| `AwsSqsClusterMessageBus.SubscriptionFetch.cs` | `AwsSqsClusterMessageBus`, `Log` | 42 | Defines AwsSqsClusterMessageBus, Log and its related behavior. |
| `AwsSqsClusterMessageBus.SubscriptionSync.cs` | `AwsSqsClusterMessageBus`, `SubscriptionEventSubscription`, `Log` | 160 | Defines AwsSqsClusterMessageBus, SubscriptionEventSubscription, Log and its related behavior. |
| `AwsSqsClusterMessageBusExtensions.cs` | `AwsSqsClusterMessageBusExtensions` | 51 | Defines AwsSqsClusterMessageBusExtensions and its related behavior. |
| `AwsSqsClusterMessageBusOptions.cs` | `AwsSqsClusterMessageBusOptions` | 53 | Defines AwsSqsClusterMessageBusOptions and its related behavior. |
| `AwsSqsClusterRequestEnvelope.cs` | `AwsSqsClusterRequestEnvelope` | 38 | Defines AwsSqsClusterRequestEnvelope and its related behavior. |
| `AwsSqsClusterRequestKind.cs` | `AwsSqsClusterRequestKind` | 13 | Defines AwsSqsClusterRequestKind and its related behavior. |
| `AwsSqsClusterResponseEnvelope.cs` | `AwsSqsClusterResponseEnvelope` | 24 | Defines AwsSqsClusterResponseEnvelope and its related behavior. |
| `AwsSqsQueuePolicy.cs` | `AwsSqsQueuePolicy` | 42 | Defines AwsSqsQueuePolicy and its related behavior. |
| `AwsSqsResourceNaming.cs` | `AwsSqsResourceNaming` | 78 | Defines AwsSqsResourceNaming and its related behavior. |
| `AwsSqsSnapshotDeltaPayload.cs` | `AwsSqsSnapshotDeltaPayload` | 15 | Defines AwsSqsSnapshotDeltaPayload and its related behavior. |
| `ThunderPropagator.ClusterMessageBuses.AwsSqs.csproj` | — | 14 | Defines project build targets, dependencies, and package metadata. |

## Types and Members

| Type | Kind | Summary | Inherits/Implements | Key Members |
|---|---|---|---|---|
| [`AwsSqsClusterMessageBusExtensions`](#awssqsclustermessagebusextensions) | class | DI registration for the AWS SNS/SQS transport. | — | `AddClusterAwsSqsMessageBus(…)` |
| [`AwsSqsClusterMessageBusOptions`](#awssqsclustermessagebusoptions) | class | Configuration for . | — | `ResourcePrefix`, `RequestTimeout`, `ReceiveWaitTimeSeconds`, `VisibilityTimeoutSeconds`, `EmptyPollDelay`, `ConfigureSqsClient` |

### AwsSqsClusterMessageBusExtensions

- **Kind:** class
- **Namespace:** `ThunderPropagator.ClusterMessageBuses.AwsSqs`
- **Inherits/implements:** None declared
- **Attributes:** None detected
- **Key members:** `AddClusterAwsSqsMessageBus(…)`
- **Summary:** DI registration for the AWS SNS/SQS transport.
- **Thread safety:** Follow the lifetime and concurrency guarantees of the owning component; no additional guarantee is inferred.

**Usage recipe**

```csharp
// Resolve AwsSqsClusterMessageBusExtensions from the configured service container or construct it with its declared dependencies.
```

[↑ Back to top](#contents)

### AwsSqsClusterMessageBusOptions

- **Kind:** class
- **Namespace:** `ThunderPropagator.ClusterMessageBuses.AwsSqs`
- **Inherits/implements:** None declared
- **Attributes:** None detected
- **Key members:** `ResourcePrefix`, `RequestTimeout`, `ReceiveWaitTimeSeconds`, `VisibilityTimeoutSeconds`, `EmptyPollDelay`, `ConfigureSqsClient`, `ConfigureSnsClient`
- **Summary:** Configuration for .
- **Thread safety:** Follow the lifetime and concurrency guarantees of the owning component; no additional guarantee is inferred.

**Usage recipe**

```csharp
// Resolve AwsSqsClusterMessageBusOptions from the configured service container or construct it with its declared dependencies.
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
  Current["AwsSqs"]
  Current --> T0["AwsSqsClusterMessageBusExtensions"]
  Current --> T1["AwsSqsClusterMessageBusOptions"]
```

The diagram shows the direct components documented by the **AwsSqs** area.

## Examples

Start with `AwsSqsClusterMessageBusExtensions` as the primary entry point for this folder, then follow its linked contracts and collaborators.

## See Also

- [Documentation home](../README.md)
- [ActiveMQ](../ActiveMQ/README.md)
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
