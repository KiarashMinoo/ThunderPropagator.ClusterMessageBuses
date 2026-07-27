# AzureServiceBus

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

The **AzureServiceBus** area groups 2 documented types, including `AzureServiceBusClusterMessageBusExtensions`, `AzureServiceBusClusterMessageBusOptions`. It provides the contracts and implementation used by this part of ThunderPropagator.ClusterMessageBuses.

## Files

| File | Primary type(s)/symbol(s) | LOC (approx.) | Responsibility |
|---|---|---:|---|
| `AssemblyInfo.cs` | — | 4 | Contains the assembly info implementation or configuration. |
| `AzureServiceBusClusterMessageBus.cs` | `AzureServiceBusClusterMessageBus`, `Log` | 370 | Defines AzureServiceBusClusterMessageBus, Log and its related behavior. |
| `AzureServiceBusClusterMessageBus.FanOut.cs` | `AzureServiceBusClusterMessageBus`, `FanOutSubscription`, `Log` | 162 | Defines AzureServiceBusClusterMessageBus, FanOutSubscription, Log and its related behavior. |
| `AzureServiceBusClusterMessageBus.RequestReply.cs` | `AzureServiceBusClusterMessageBus`, `Log` | 191 | Defines AzureServiceBusClusterMessageBus, Log and its related behavior. |
| `AzureServiceBusClusterMessageBus.Snapshots.cs` | `AzureServiceBusClusterMessageBus`, `Log` | 96 | Defines AzureServiceBusClusterMessageBus, Log and its related behavior. |
| `AzureServiceBusClusterMessageBus.SubscriptionFetch.cs` | `AzureServiceBusClusterMessageBus`, `Log` | 42 | Defines AzureServiceBusClusterMessageBus, Log and its related behavior. |
| `AzureServiceBusClusterMessageBus.SubscriptionSync.cs` | `AzureServiceBusClusterMessageBus`, `SubscriptionEventSubscription`, `Log` | 161 | Defines AzureServiceBusClusterMessageBus, SubscriptionEventSubscription, Log and its related behavior. |
| `AzureServiceBusClusterMessageBusExtensions.cs` | `AzureServiceBusClusterMessageBusExtensions` | 50 | Defines AzureServiceBusClusterMessageBusExtensions and its related behavior. |
| `AzureServiceBusClusterMessageBusOptions.cs` | `AzureServiceBusClusterMessageBusOptions` | 46 | Defines AzureServiceBusClusterMessageBusOptions and its related behavior. |
| `AzureServiceBusClusterRequestEnvelope.cs` | `AzureServiceBusClusterRequestEnvelope` | 39 | Defines AzureServiceBusClusterRequestEnvelope and its related behavior. |
| `AzureServiceBusClusterRequestKind.cs` | `AzureServiceBusClusterRequestKind` | 14 | Defines AzureServiceBusClusterRequestKind and its related behavior. |
| `AzureServiceBusClusterResponseEnvelope.cs` | `AzureServiceBusClusterResponseEnvelope` | 24 | Defines AzureServiceBusClusterResponseEnvelope and its related behavior. |
| `AzureServiceBusResourceNaming.cs` | `AzureServiceBusResourceNaming` | 96 | Defines AzureServiceBusResourceNaming and its related behavior. |
| `AzureServiceBusSnapshotDeltaPayload.cs` | `AzureServiceBusSnapshotDeltaPayload` | 15 | Defines AzureServiceBusSnapshotDeltaPayload and its related behavior. |
| `ThunderPropagator.ClusterMessageBuses.AzureServiceBus.csproj` | — | 13 | Defines project build targets, dependencies, and package metadata. |

## Types and Members

| Type | Kind | Summary | Inherits/Implements | Key Members |
|---|---|---|---|---|
| [`AzureServiceBusClusterMessageBusExtensions`](#azureservicebusclustermessagebusextensions) | class | DI registration for the Azure Service Bus transport. | — | `AddClusterAzureServiceBusMessageBus(…)` |
| [`AzureServiceBusClusterMessageBusOptions`](#azureservicebusclustermessagebusoptions) | class | Configuration for . | — | `ConnectionString`, `ResourcePrefix`, `RequestTimeout`, `ReceiveMaxWaitTime`, `EmptyPollDelay`, `ConfigureClientOptions` |

### AzureServiceBusClusterMessageBusExtensions

- **Kind:** class
- **Namespace:** `ThunderPropagator.ClusterMessageBuses.AzureServiceBus`
- **Inherits/implements:** None declared
- **Attributes:** None detected
- **Key members:** `AddClusterAzureServiceBusMessageBus(…)`
- **Summary:** DI registration for the Azure Service Bus transport.
- **Thread safety:** Follow the lifetime and concurrency guarantees of the owning component; no additional guarantee is inferred.

**Usage recipe**

```csharp
// Resolve AzureServiceBusClusterMessageBusExtensions from the configured service container or construct it with its declared dependencies.
```

[↑ Back to top](#contents)

### AzureServiceBusClusterMessageBusOptions

- **Kind:** class
- **Namespace:** `ThunderPropagator.ClusterMessageBuses.AzureServiceBus`
- **Inherits/implements:** None declared
- **Attributes:** None detected
- **Key members:** `ConnectionString`, `ResourcePrefix`, `RequestTimeout`, `ReceiveMaxWaitTime`, `EmptyPollDelay`, `ConfigureClientOptions`
- **Summary:** Configuration for .
- **Thread safety:** Follow the lifetime and concurrency guarantees of the owning component; no additional guarantee is inferred.

**Usage recipe**

```csharp
// Resolve AzureServiceBusClusterMessageBusOptions from the configured service container or construct it with its declared dependencies.
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
  Current["AzureServiceBus"]
  Current --> T0["AzureServiceBusClusterMessageBusExtensions"]
  Current --> T1["AzureServiceBusClusterMessageBusOptions"]
```

The diagram shows the direct components documented by the **AzureServiceBus** area.

## Examples

Start with `AzureServiceBusClusterMessageBusExtensions` as the primary entry point for this folder, then follow its linked contracts and collaborators.

## See Also

- [Documentation home](../README.md)
- [ActiveMQ](../ActiveMQ/README.md)
- [AwsSqs](../AwsSqs/README.md)
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
