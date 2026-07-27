# Mqtt

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

The **Mqtt** area groups 2 documented types, including `MqttClusterMessageBusExtensions`, `MqttClusterMessageBusOptions`. It provides the contracts and implementation used by this part of ThunderPropagator.ClusterMessageBuses.

## Files

| File | Primary type(s)/symbol(s) | LOC (approx.) | Responsibility |
|---|---|---:|---|
| `AssemblyInfo.cs` | — | 4 | Contains the assembly info implementation or configuration. |
| `IMqttClusterTransport.cs` | `IMqttClusterTransport` | 18 | Defines IMqttClusterTransport and its related behavior. |
| `MqttClusterMessageBus.cs` | `MqttClusterMessageBus`, `Log` | 167 | Defines MqttClusterMessageBus, Log and its related behavior. |
| `MqttClusterMessageBus.FanOut.cs` | `MqttClusterMessageBus`, `FanOutSubscription`, `Log` | 139 | Defines MqttClusterMessageBus, FanOutSubscription, Log and its related behavior. |
| `MqttClusterMessageBus.RequestReply.cs` | `MqttClusterMessageBus`, `Log` | 219 | Defines MqttClusterMessageBus, Log and its related behavior. |
| `MqttClusterMessageBus.Snapshots.cs` | `MqttClusterMessageBus`, `Log` | 96 | Defines MqttClusterMessageBus, Log and its related behavior. |
| `MqttClusterMessageBus.SubscriptionFetch.cs` | `MqttClusterMessageBus`, `Log` | 42 | Defines MqttClusterMessageBus, Log and its related behavior. |
| `MqttClusterMessageBus.SubscriptionSync.cs` | `MqttClusterMessageBus`, `SubscriptionEventSubscription`, `Log` | 136 | Defines MqttClusterMessageBus, SubscriptionEventSubscription, Log and its related behavior. |
| `MqttClusterMessageBusExtensions.cs` | `MqttClusterMessageBusExtensions` | 49 | Defines MqttClusterMessageBusExtensions and its related behavior. |
| `MqttClusterMessageBusOptions.cs` | `MqttClusterMessageBusOptions` | 38 | Defines MqttClusterMessageBusOptions and its related behavior. |
| `MqttClusterRequestEnvelope.cs` | `MqttClusterRequestEnvelope` | 40 | Defines MqttClusterRequestEnvelope and its related behavior. |
| `MqttClusterRequestKind.cs` | `MqttClusterRequestKind` | 13 | Defines MqttClusterRequestKind and its related behavior. |
| `MqttClusterResponseEnvelope.cs` | `MqttClusterResponseEnvelope` | 25 | Defines MqttClusterResponseEnvelope and its related behavior. |
| `MqttClusterTransport.cs` | `MqttClusterTransport` | 131 | Defines MqttClusterTransport and its related behavior. |
| `MqttSnapshotDeltaPayload.cs` | `MqttSnapshotDeltaPayload` | 15 | Defines MqttSnapshotDeltaPayload and its related behavior. |
| `MqttTopicNaming.cs` | `MqttTopicNaming` | 52 | Defines MqttTopicNaming and its related behavior. |
| `ThunderPropagator.ClusterMessageBuses.Mqtt.csproj` | — | 13 | Defines project build targets, dependencies, and package metadata. |

## Types and Members

| Type | Kind | Summary | Inherits/Implements | Key Members |
|---|---|---|---|---|
| [`MqttClusterMessageBusExtensions`](#mqttclustermessagebusextensions) | class | DI registration for the MQTT transport. | — | `AddClusterMqttMessageBus(…)` |
| [`MqttClusterMessageBusOptions`](#mqttclustermessagebusoptions) | class | Configuration for . | — | `Host`, `Port`, `ClientId`, `TopicPrefix`, `RequestTimeout` |

### MqttClusterMessageBusExtensions

- **Kind:** class
- **Namespace:** `ThunderPropagator.ClusterMessageBuses.Mqtt`
- **Inherits/implements:** None declared
- **Attributes:** None detected
- **Key members:** `AddClusterMqttMessageBus(…)`
- **Summary:** DI registration for the MQTT transport.
- **Thread safety:** Follow the lifetime and concurrency guarantees of the owning component; no additional guarantee is inferred.

**Usage recipe**

```csharp
// Resolve MqttClusterMessageBusExtensions from the configured service container or construct it with its declared dependencies.
```

[↑ Back to top](#contents)

### MqttClusterMessageBusOptions

- **Kind:** class
- **Namespace:** `ThunderPropagator.ClusterMessageBuses.Mqtt`
- **Inherits/implements:** None declared
- **Attributes:** None detected
- **Key members:** `Host`, `Port`, `ClientId`, `TopicPrefix`, `RequestTimeout`
- **Summary:** Configuration for .
- **Thread safety:** Follow the lifetime and concurrency guarantees of the owning component; no additional guarantee is inferred.

**Usage recipe**

```csharp
// Resolve MqttClusterMessageBusOptions from the configured service container or construct it with its declared dependencies.
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
  Current["Mqtt"]
  Current --> T0["MqttClusterMessageBusExtensions"]
  Current --> T1["MqttClusterMessageBusOptions"]
```

The diagram shows the direct components documented by the **Mqtt** area.

## Examples

Start with `MqttClusterMessageBusExtensions` as the primary entry point for this folder, then follow its linked contracts and collaborators.

## See Also

- [Documentation home](../README.md)
- [ActiveMQ](../ActiveMQ/README.md)
- [AwsSqs](../AwsSqs/README.md)
- [AzureServiceBus](../AzureServiceBus/README.md)
- [Kafka](../Kafka/README.md)
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
