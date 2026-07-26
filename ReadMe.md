# ThunderPropagator.ClusterMessageBuses

Pluggable `IClusterMessageBus` implementations for [ThunderPropagator](https://github.com/KiarashMinoo/ThunderPropagator) cluster inter-node transport.

ThunderPropagator core ships only an HttpClient-based cluster transport out of the box (see core issue [#340](https://github.com/KiarashMinoo/ThunderPropagator/issues/340)). Every other transport — message-broker-backed or direct-protocol — lives in this repo instead, registered into a consuming application via standard dependency injection.

This repo mirrors the structure of [ThunderPropagator.Feeviders](https://github.com/KiarashMinoo/ThunderPropagator.Feeviders): a shared kernel plus one project per transport. See `CLAUDE.md` for the full architecture and roadmap.

## Status

Scaffolding only (see issue [#1](https://github.com/KiarashMinoo/ThunderPropagator.ClusterMessageBuses/issues/1)). Transport implementations (gRPC, ZeroMQ, and the remaining message brokers) are tracked as separate issues and land incrementally.
