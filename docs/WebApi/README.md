# WebApi

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

The **WebApi** area groups 2 documented types, including `WebApiClusterMessageBusExtensions`, `WebApiClusterMessageBusOptions`. It provides the contracts and implementation used by this part of ThunderPropagator.ClusterMessageBuses.

## Files

| File | Primary type(s)/symbol(s) | LOC (approx.) | Responsibility |
|---|---|---:|---|
| `AssemblyInfo.cs` | — | 4 | Contains the assembly info implementation or configuration. |
| `HttpClientWebApiClusterHttpClient.cs` | `HttpClientWebApiClusterHttpClient` | 24 | Defines HttpClientWebApiClusterHttpClient and its related behavior. |
| `HttpListenerWebApiClusterListener.cs` | `HttpListenerWebApiClusterListener` | 87 | Defines HttpListenerWebApiClusterListener and its related behavior. |
| `IWebApiClusterHttpClient.cs` | `IWebApiClusterHttpClient` | 16 | Defines IWebApiClusterHttpClient and its related behavior. |
| `IWebApiClusterListener.cs` | `IWebApiClusterListener` | 21 | Defines IWebApiClusterListener and its related behavior. |
| `ThunderPropagator.ClusterMessageBuses.WebApi.csproj` | — | 12 | Defines project build targets, dependencies, and package metadata. |
| `WebApiClusterMessageBus.cs` | `WebApiClusterMessageBus`, `Log` | 209 | Defines WebApiClusterMessageBus, Log and its related behavior. |
| `WebApiClusterMessageBus.FanOut.cs` | `WebApiClusterMessageBus`, `FanOutSubscription`, `Log` | 117 | Defines WebApiClusterMessageBus, FanOutSubscription, Log and its related behavior. |
| `WebApiClusterMessageBus.Routing.cs` | `WebApiClusterMessageBus`, `Log` | 73 | Defines WebApiClusterMessageBus, Log and its related behavior. |
| `WebApiClusterMessageBus.Snapshots.cs` | `WebApiClusterMessageBus`, `Log` | 101 | Defines WebApiClusterMessageBus, Log and its related behavior. |
| `WebApiClusterMessageBus.SubscriptionFetch.cs` | `WebApiClusterMessageBus`, `Log` | 54 | Defines WebApiClusterMessageBus, Log and its related behavior. |
| `WebApiClusterMessageBus.SubscriptionSync.cs` | `WebApiClusterMessageBus`, `SubscriptionEventSubscription`, `Log` | 115 | Defines WebApiClusterMessageBus, SubscriptionEventSubscription, Log and its related behavior. |
| `WebApiClusterMessageBusExtensions.cs` | `WebApiClusterMessageBusExtensions` | 56 | Defines WebApiClusterMessageBusExtensions and its related behavior. |
| `WebApiClusterMessageBusOptions.cs` | `WebApiClusterMessageBusOptions` | 30 | Defines WebApiClusterMessageBusOptions and its related behavior. |
| `WebApiClusterRouting.cs` | `WebApiClusterRouting` | 115 | Defines WebApiClusterRouting and its related behavior. |
| `WebApiIncomingRequest.cs` | `WebApiIncomingRequest` | 19 | Defines WebApiIncomingRequest and its related behavior. |
| `WebApiRouteKind.cs` | `WebApiRouteKind` | 12 | Defines WebApiRouteKind and its related behavior. |
| `WebApiRouteMatch.cs` | `WebApiRouteMatch` | 5 | Defines WebApiRouteMatch and its related behavior. |
| `WebApiSnapshotDeltaPayload.cs` | `WebApiSnapshotDeltaPayload` | 14 | Defines WebApiSnapshotDeltaPayload and its related behavior. |

## Types and Members

| Type | Kind | Summary | Inherits/Implements | Key Members |
|---|---|---|---|---|
| [`WebApiClusterMessageBusExtensions`](#webapiclustermessagebusextensions) | class | DI registration for the self-hosted HTTP direct-peer-connection transport. | — | `AddClusterWebApiMessageBus(…)` |
| [`WebApiClusterMessageBusOptions`](#webapiclustermessagebusoptions) | class | Configuration for . | — | `ListenPath`, `RequestTimeout`, `ConfigureHttpClient` |

### WebApiClusterMessageBusExtensions

- **Kind:** class
- **Namespace:** `ThunderPropagator.ClusterMessageBuses.WebApi`
- **Inherits/implements:** None declared
- **Attributes:** None detected
- **Key members:** `AddClusterWebApiMessageBus(…)`
- **Summary:** DI registration for the self-hosted HTTP direct-peer-connection transport.
- **Thread safety:** Follow the lifetime and concurrency guarantees of the owning component; no additional guarantee is inferred.

**Usage recipe**

```csharp
// Resolve WebApiClusterMessageBusExtensions from the configured service container or construct it with its declared dependencies.
```

[↑ Back to top](#contents)

### WebApiClusterMessageBusOptions

- **Kind:** class
- **Namespace:** `ThunderPropagator.ClusterMessageBuses.WebApi`
- **Inherits/implements:** None declared
- **Attributes:** None detected
- **Key members:** `ListenPath`, `RequestTimeout`, `ConfigureHttpClient`
- **Summary:** Configuration for .
- **Thread safety:** Follow the lifetime and concurrency guarantees of the owning component; no additional guarantee is inferred.

**Usage recipe**

```csharp
// Resolve WebApiClusterMessageBusOptions from the configured service container or construct it with its declared dependencies.
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
  Current["WebApi"]
  Current --> T0["WebApiClusterMessageBusExtensions"]
  Current --> T1["WebApiClusterMessageBusOptions"]
```

The diagram shows the direct components documented by the **WebApi** area.

## Examples

Start with `WebApiClusterMessageBusExtensions` as the primary entry point for this folder, then follow its linked contracts and collaborators.

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
- [UdpClient](../UdpClient/README.md)
- [WebSocket](../WebSocket/README.md)

[↑ Back to top](#contents)
