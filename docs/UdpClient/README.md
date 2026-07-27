# UdpClient

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

The **UdpClient** area groups 2 documented types, including `UdpClusterMessageBusExtensions`, `UdpClusterMessageBusOptions`. It provides the contracts and implementation used by this part of ThunderPropagator.ClusterMessageBuses.

## Files

| File | Primary type(s)/symbol(s) | LOC (approx.) | Responsibility |
|---|---|---:|---|
| `AssemblyInfo.cs` | — | 4 | Contains the assembly info implementation or configuration. |
| `IUdpClusterSocket.cs` | `IUdpClusterSocket` | 26 | Defines IUdpClusterSocket and its related behavior. |
| `ThunderPropagator.ClusterMessageBuses.UdpClient.csproj` | — | 12 | Defines project build targets, dependencies, and package metadata. |
| `UdpAddressing.cs` | `UdpAddressing` | 31 | Defines UdpAddressing and its related behavior. |
| `UdpClientClusterSocket.cs` | `UdpClientClusterSocket` | 64 | Defines UdpClientClusterSocket and its related behavior. |
| `UdpClusterFrame.cs` | `UdpClusterFrame` | 15 | Defines UdpClusterFrame and its related behavior. |
| `UdpClusterFrameKind.cs` | `UdpClusterFrameKind` | 15 | Defines UdpClusterFrameKind and its related behavior. |
| `UdpClusterMessageBus.cs` | `UdpClusterMessageBus`, `Log` | 285 | Defines UdpClusterMessageBus, Log and its related behavior. |
| `UdpClusterMessageBus.FanOut.cs` | `UdpClusterMessageBus`, `FanOutSubscription`, `Log` | 105 | Defines UdpClusterMessageBus, FanOutSubscription, Log and its related behavior. |
| `UdpClusterMessageBus.RequestReply.cs` | `UdpClusterMessageBus`, `Log` | 199 | Defines UdpClusterMessageBus, Log and its related behavior. |
| `UdpClusterMessageBus.Snapshots.cs` | `UdpClusterMessageBus`, `Log` | 96 | Defines UdpClusterMessageBus, Log and its related behavior. |
| `UdpClusterMessageBus.SubscriptionFetch.cs` | `UdpClusterMessageBus`, `Log` | 42 | Defines UdpClusterMessageBus, Log and its related behavior. |
| `UdpClusterMessageBus.SubscriptionSync.cs` | `UdpClusterMessageBus`, `SubscriptionEventSubscription`, `Log` | 102 | Defines UdpClusterMessageBus, SubscriptionEventSubscription, Log and its related behavior. |
| `UdpClusterMessageBusExtensions.cs` | `UdpClusterMessageBusExtensions` | 57 | Defines UdpClusterMessageBusExtensions and its related behavior. |
| `UdpClusterMessageBusOptions.cs` | `UdpClusterMessageBusOptions` | 44 | Defines UdpClusterMessageBusOptions and its related behavior. |
| `UdpClusterRequestEnvelope.cs` | `UdpClusterRequestEnvelope` | 21 | Defines UdpClusterRequestEnvelope and its related behavior. |
| `UdpClusterRequestKind.cs` | `UdpClusterRequestKind` | 13 | Defines UdpClusterRequestKind and its related behavior. |
| `UdpClusterResponseEnvelope.cs` | `UdpClusterResponseEnvelope` | 24 | Defines UdpClusterResponseEnvelope and its related behavior. |
| `UdpFanOutPayload.cs` | `UdpFanOutPayload` | 7 | Defines UdpFanOutPayload and its related behavior. |
| `UdpReceivedDatagram.cs` | `UdpReceivedDatagram` | 13 | Defines UdpReceivedDatagram and its related behavior. |
| `UdpSnapshotDeltaPayload.cs` | `UdpSnapshotDeltaPayload` | 15 | Defines UdpSnapshotDeltaPayload and its related behavior. |
| `UdpSubscriptionEventPayload.cs` | `UdpSubscriptionEventPayload` | 7 | Defines UdpSubscriptionEventPayload and its related behavior. |

## Types and Members

| Type | Kind | Summary | Inherits/Implements | Key Members |
|---|---|---|---|---|
| [`UdpClusterMessageBusExtensions`](#udpclustermessagebusextensions) | class | DI registration for the raw-UDP direct-peer-messaging transport. | — | `AddClusterUdpMessageBus(…)` |
| [`UdpClusterMessageBusOptions`](#udpclustermessagebusoptions) | class | Configuration for . | — | `Port`, `MaxDatagramSize`, `RequestTimeout`, `ResendInterval` |

### UdpClusterMessageBusExtensions

- **Kind:** class
- **Namespace:** `ThunderPropagator.ClusterMessageBuses.UdpClient`
- **Inherits/implements:** None declared
- **Attributes:** None detected
- **Key members:** `AddClusterUdpMessageBus(…)`
- **Summary:** DI registration for the raw-UDP direct-peer-messaging transport.
- **Thread safety:** Follow the lifetime and concurrency guarantees of the owning component; no additional guarantee is inferred.

**Usage recipe**

```csharp
// Resolve UdpClusterMessageBusExtensions from the configured service container or construct it with its declared dependencies.
```

[↑ Back to top](#contents)

### UdpClusterMessageBusOptions

- **Kind:** class
- **Namespace:** `ThunderPropagator.ClusterMessageBuses.UdpClient`
- **Inherits/implements:** None declared
- **Attributes:** None detected
- **Key members:** `Port`, `MaxDatagramSize`, `RequestTimeout`, `ResendInterval`
- **Summary:** Configuration for .
- **Thread safety:** Follow the lifetime and concurrency guarantees of the owning component; no additional guarantee is inferred.

**Usage recipe**

```csharp
// Resolve UdpClusterMessageBusOptions from the configured service container or construct it with its declared dependencies.
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
  Current["UdpClient"]
  Current --> T0["UdpClusterMessageBusExtensions"]
  Current --> T1["UdpClusterMessageBusOptions"]
```

The diagram shows the direct components documented by the **UdpClient** area.

## Examples

Start with `UdpClusterMessageBusExtensions` as the primary entry point for this folder, then follow its linked contracts and collaborators.

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
- [RedisPubSub](../RedisPubSub/README.md)
- [SharedKernel](../SharedKernel/README.md)
- [TcpSocket](../TcpSocket/README.md)
- [WebApi](../WebApi/README.md)
- [WebSocket](../WebSocket/README.md)

[↑ Back to top](#contents)
