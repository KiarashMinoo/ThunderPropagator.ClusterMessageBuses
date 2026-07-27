# ThunderPropagator.ClusterMessageBuses Documentation

Pluggable cluster message buses for coordinating ThunderPropagator nodes and distributing internal cluster events.

## Contents

- [Documentation areas](#documentation-areas)
- [Package dependencies](#package-dependencies)
- [Coverage audit](#coverage-audit)

## Documentation areas

- [ActiveMQ](./ActiveMQ/README.md) `Types:2` `Files:15` `Diagrams:✓`
- [AwsSqs](./AwsSqs/README.md) `Types:2` `Files:16` `Diagrams:✓`
- [AzureServiceBus](./AzureServiceBus/README.md) `Types:2` `Files:15` `Diagrams:✓`
- [Kafka](./Kafka/README.md) `Types:2` `Files:17` `Diagrams:✓`
- [Mqtt](./Mqtt/README.md) `Types:2` `Files:17` `Diagrams:✓`
- [NATS](./NATS/README.md) `Types:2` `Files:18` `Diagrams:✓`
- [Pulsar](./Pulsar/README.md) `Types:2` `Files:17` `Diagrams:✓`
- [RabbitMQ](./RabbitMQ/README.md) `Types:2` `Files:15` `Diagrams:✓`
- [RedisPubSub](./RedisPubSub/README.md) `Types:2` `Files:15` `Diagrams:✓`
- [SharedKernel](./SharedKernel/README.md) `Types:6` `Files:8` `Diagrams:✓`
- [TcpSocket](./TcpSocket/README.md) `Types:2` `Files:22` `Diagrams:✓`
- [UdpClient](./UdpClient/README.md) `Types:2` `Files:22` `Diagrams:✓`
- [WebApi](./WebApi/README.md) `Types:2` `Files:19` `Diagrams:✓`
- [WebSocket](./WebSocket/README.md) `Types:2` `Files:22` `Diagrams:✓`

## Package dependencies

| Package | Version | Registry |
|---|---|---|
| `Apache.NMS` | `2.2.0` | [Package](https://www.nuget.org/packages/Apache.NMS) |
| `Apache.NMS.ActiveMQ` | `2.2.0` | [Package](https://www.nuget.org/packages/Apache.NMS.ActiveMQ) |
| `AWSSDK.SimpleNotificationService` | `4.0.100.6` | [Package](https://www.nuget.org/packages/AWSSDK.SimpleNotificationService) |
| `AWSSDK.SQS` | `4.0.100.5` | [Package](https://www.nuget.org/packages/AWSSDK.SQS) |
| `Azure.Messaging.ServiceBus` | `7.20.2` | [Package](https://www.nuget.org/packages/Azure.Messaging.ServiceBus) |
| `Confluent.Kafka` | `2.15.0` | [Package](https://www.nuget.org/packages/Confluent.Kafka) |
| `DotPulsar` | `5.3.1` | [Package](https://www.nuget.org/packages/DotPulsar) |
| `Microsoft.Extensions.Logging.Abstractions` | `10.*` | [Package](https://www.nuget.org/packages/Microsoft.Extensions.Logging.Abstractions) |
| `Microsoft.Extensions.Options` | `10.*` | [Package](https://www.nuget.org/packages/Microsoft.Extensions.Options) |
| `MQTTnet` | `5.2.0.1603` | [Package](https://www.nuget.org/packages/MQTTnet) |
| `NATS.Net` | `3.0.1` | [Package](https://www.nuget.org/packages/NATS.Net) |
| `Polly.Core` | `8.6.4` | [Package](https://www.nuget.org/packages/Polly.Core) |
| `RabbitMQ.Client` | `7.2.1` | [Package](https://www.nuget.org/packages/RabbitMQ.Client) |
| `StackExchange.Redis` | `3.0.17` | [Package](https://www.nuget.org/packages/StackExchange.Redis) |

## Coverage audit

| Documentation area | Status | Files | Types | Retry passes |
|---|---|---:|---:|---:|
| [`ActiveMQ`](./ActiveMQ/README.md) | ✅ Complete | 15 | 2 | 1 |
| [`AwsSqs`](./AwsSqs/README.md) | ✅ Complete | 16 | 2 | 1 |
| [`AzureServiceBus`](./AzureServiceBus/README.md) | ✅ Complete | 15 | 2 | 1 |
| [`Kafka`](./Kafka/README.md) | ✅ Complete | 17 | 2 | 1 |
| [`Mqtt`](./Mqtt/README.md) | ✅ Complete | 17 | 2 | 1 |
| [`NATS`](./NATS/README.md) | ✅ Complete | 18 | 2 | 1 |
| [`Pulsar`](./Pulsar/README.md) | ✅ Complete | 17 | 2 | 1 |
| [`RabbitMQ`](./RabbitMQ/README.md) | ✅ Complete | 15 | 2 | 1 |
| [`RedisPubSub`](./RedisPubSub/README.md) | ✅ Complete | 15 | 2 | 1 |
| [`SharedKernel`](./SharedKernel/README.md) | ✅ Complete | 8 | 6 | 1 |
| [`TcpSocket`](./TcpSocket/README.md) | ✅ Complete | 22 | 2 | 1 |
| [`UdpClient`](./UdpClient/README.md) | ✅ Complete | 22 | 2 | 1 |
| [`WebApi`](./WebApi/README.md) | ✅ Complete | 19 | 2 | 1 |
| [`WebSocket`](./WebSocket/README.md) | ✅ Complete | 22 | 2 | 1 |

**Last generated:** July 27, 2026
