---
paths:
  - "src/ThunderPropagator.ClusterMessageBuses.ActiveMQ/**"
  - "src/ThunderPropagator.ClusterMessageBuses.AwsSqs/**"
  - "src/ThunderPropagator.ClusterMessageBuses.AzureServiceBus/**"
  - "src/ThunderPropagator.ClusterMessageBuses.GcpPubSub/**"
  - "src/ThunderPropagator.ClusterMessageBuses.Grpc/**"
  - "src/ThunderPropagator.ClusterMessageBuses.Kafka/**"
  - "src/ThunderPropagator.ClusterMessageBuses.Mqtt/**"
  - "src/ThunderPropagator.ClusterMessageBuses.NATS/**"
  - "src/ThunderPropagator.ClusterMessageBuses.Pulsar/**"
  - "src/ThunderPropagator.ClusterMessageBuses.RabbitMQ/**"
  - "src/ThunderPropagator.ClusterMessageBuses.RedisPubSub/**"
  - "src/ThunderPropagator.ClusterMessageBuses.TcpSocket/**"
  - "src/ThunderPropagator.ClusterMessageBuses.UdpClient/**"
  - "src/ThunderPropagator.ClusterMessageBuses.WebApi/**"
  - "src/ThunderPropagator.ClusterMessageBuses.WebSocket/**"
  - "src/ThunderPropagator.ClusterMessageBuses.ZeroMQ/**"
---

# Request/Reply Shapes

- **Queue-primitive brokers** — every node owns a request queue + reply queue named from its own node identity; a peer sends to the target's request queue, awaits reply on its own reply queue. Reply destination is always derived from the request envelope's own-identity field — never trust a wire-supplied destination.
- **Topic/subscription-only brokers** — every node owns a request topic + reply topic, each with exactly one subscription (its own). Same non-trusting-the-wire rule for reply destination.

Fan-out and subscription-sync: one topic (or broadcast primitive) per channel, every node's own exclusive consumer on it — regardless of request/reply shape.

Transports with no built-in delivery guarantee (e.g. connectionless) must implement their own resend-on-timer reliability layer for the request side.
