## [0.1.0-beta.2] — 2026-07-28

### 🚀 Features

- Implement the Kafka IClusterMessageBus transport (first of the 14 remaining brokers) `(c6d1ecd)` — Kiarash Minoo
- Move IClusterChannelResolver/ChannelManagerResolver into SharedKernel `(9eef8a7)` — Kiarash Minoo
- Implement the RabbitMQ IClusterMessageBus transport `(400fb40)` — Kiarash Minoo
- Implement the NATS IClusterMessageBus transport `(3edbaf3)` — Kiarash Minoo
- Implement the Pulsar IClusterMessageBus transport `(95b2c15)` — Kiarash Minoo
- Implement the MQTT IClusterMessageBus transport `(8f5f12e)` — Kiarash Minoo
- Implement the ActiveMQ IClusterMessageBus transport `(ed35f79)` — Kiarash Minoo
- Implement the RedisPubSub IClusterMessageBus transport `(22a8214)` — Kiarash Minoo
- Implement the WebSocket IClusterMessageBus transport `(b83b749)` — Kiarash Minoo
- Implement the WebApi IClusterMessageBus transport `(41633b5)` — Kiarash Minoo
- Implement the TcpSocket IClusterMessageBus transport `(0979415)` — Kiarash Minoo
- Implement the UdpClient IClusterMessageBus transport `(0eac8cd)` — Kiarash Minoo
- Implement the AwsSqs IClusterMessageBus transport `(5bcec53)` — Kiarash Minoo
- add RequiresPreviewFeatures attribute and update project references `(5c2a644)` — Kiarash Minoo

### 📦 Dependencies

| Package | Old | New |
|---------|-----|-----|
| Confluent.Kafka | 2.14.2 | 2.15.0 |
| NATS.Net | 3.0.0 | 3.0.1 |
| AWSSDK.SimpleNotificationService | 4.0.100.5 | 4.0.100.6 |
| FluentAssertions | 7.2.0 | 8.10.0 |
| Polly.Core | 8.6.4 | 8.7.0 |
| AWSSDK.SQS | 4.0.100.5 | 4.0.100.6 |

- Bump the messaging group with 2 updates `(6c5f551)` — dependabot[bot]
- Bump AWSSDK.SimpleNotificationService from 4.0.100.5 to 4.0.100.6 `(6a9a1b6)` — dependabot[bot]
- Bump FluentAssertions from 7.2.0 to 8.10.0 `(a8f783e)` — dependabot[bot]
- Bump Polly.Core from 8.6.4 to 8.7.0 `(305d15c)` — dependabot[bot]
- Bump AWSSDK.SQS from 4.0.100.5 to 4.0.100.6 `(e15e395)` — dependabot[bot]

### ⚙️ CI / Tooling

- Implement the Azure Service Bus IClusterMessageBus transport `(cdce1ab)` — Kiarash Minoo
- enable nuget-filter-enabled to stop publishing every platform/config package variant `(ef6172d)` — Kiarash Minoo

### 📝 Documentation

- rebuild repository documentation `(d579de6)` — Codex

### 🧪 Tests

- Fix missing AzureServiceBus ProjectReference in ThunderPropagator.UnitTests `(fcdc68a)` — Kiarash Minoo

### 🏠 Chores

- Repo scaffolding: build infra, SharedKernel, ArchTests, UnitTests, CI `(891fb7d)` — Kiarash Minoo
- Pin ThunderPropagator core deps to the Cluster-flavored packages at 1.0.1-beta.195 `(01e21d0)` — Kiarash Minoo
