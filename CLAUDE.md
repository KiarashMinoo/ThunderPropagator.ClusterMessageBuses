# CLAUDE.md

Guidance for working in this repository.

## What this repo is

Pluggable cluster inter-node transport implementations of the core cluster-message-bus contract — registered via DI to replace or supplement the host application's built-in cluster transport. Each transport implements the same seven operations: two fan-out publish/subscribe pairs (broadcast messages, subscription-sync events) and three leader/peer-pull operations (restore a snapshot from a leader, sync a delta since a timestamp, fetch a peer's local subscriptions).

This repo contains no channels, feeders, or data-plane providers — only cluster control/data-plane transport.

## Commands

```bash
dotnet restore    # fetches shared build configuration on first use; also needs read access to the core package feed
dotnet build
dotnet test
dotnet test <TestProject> --filter "FullyQualifiedName~<Name>"
dotnet clean      # also clears the downloaded shared build cache
```

## Architecture

A shared-kernel area plus one project per transport, each independently deployable/versionable:

- **Shared kernel** — a feature-gate DI helper, a generic keyed connection/client cache for transports whose client is expensive to construct per peer (dedups concurrent connects, retries instead of caching a failure, disposes everything on shutdown), and a resilience-pipeline factory (retry + circuit-breaker) transports should build their per-peer resilience through instead of hand-rolling retry loops.
- **Transport project** — implements the bus contract; a per-transport options class; a DI extension registering it, feature-gated through the shared helper.

## The two request/reply shapes

The three leader/peer-pull operations need point-to-point delivery, which not every broker primitive offers directly:

- **Brokers with a queue primitive** — every node owns a request queue and a reply queue named from its own node identity; a peer sends directly to the target's request queue and awaits a reply on its own reply queue. The answering side always derives the reply destination itself from the request envelope's own-identity field — never trusts a destination supplied on the wire, so a request can never redirect a reply to an arbitrary destination.
- **Brokers with only topics/subscriptions (no queue primitive)** — every node instead owns a request topic and a reply topic, each with exactly one subscription (its own) pulling from it; a topic with a single subscription behaves like a point-to-point queue as long as no second subscription is ever added. Same non-trusting-the-wire rule applies to the reply destination.

Fan-out and subscription-sync always use one topic (or equivalent broadcast primitive) per channel, with every node's own exclusive consumer on it — regardless of which request/reply shape that transport uses.

Transports whose underlying protocol has no built-in delivery guarantee (e.g. a connectionless one) must implement their own resend-on-timer reliability layer for the request side; every other transport can rely on the protocol's own guarantee.

## Architecture rules (enforced)

- Each transport assembly keeps its public/internal types inside its own namespace.
- No transport assembly may reference a sibling transport's namespace.
- The full assembly-reference graph (shared kernel plus every transport) must be acyclic.

## Conventions

- `internal sealed` (non-sealed in Debug via `#if !DEBUG sealed #endif`) for concrete transport classes.
- Source-generated logging methods for all logging — never pass a runtime-built string to a logger call.
- All culture-sensitive parsing/formatting must use the invariant culture explicitly.
- Guard-clause library for required constructor parameters — never suppress with the null-forgiving operator.
- XML docs required on all public API.
- A single self-echo identifier stamped on every outbound fan-out/subscription-sync message lets a node reject messages that are its own broadcast looped back.

## Adding a transport

New transport area → wire envelope types (request kind enum, request envelope with a reply-destination field, response envelope, delta-payload type) → resource-naming helper for whatever topic/queue/subscription names the broker needs → the bus class plus one partial file per concern (fan-out, subscription-sync, request/reply, snapshot restore/delta, subscription fetch) → a DI extension → a full unit-test suite substituting the broker client directly if it's designed for mocking (non-sealed, virtual members, a protected parameterless constructor), or behind a narrow custom interface if it isn't → an architecture-test row for the new transport's namespace isolation.

## Testing

xUnit, NSubstitute, FluentAssertions. A separate architecture-test project checks namespace containment, sibling-transport isolation, and the acyclic dependency graph — it's structured so a new transport just adds a data row, not a new test method. Both test projects have internal access to the shared kernel and every transport assembly.

## Build & versioning

Version and target frameworks are centralized; CI bumps automatically. Restore fetches shared build configuration into a local, gitignored cache — a clean removes it, the next restore refetches it.
