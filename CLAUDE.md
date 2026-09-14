# CLAUDE.md

<!-- Humans: keep this file lean — per-transport request/reply patterns live in .claude/rules/, not here. -->

## What This Repo Is

Pluggable cluster inter-node transport implementations of the core cluster-message-bus contract, DI-registered to replace/supplement the host app's built-in cluster transport. Each transport implements the same seven operations: two fan-out publish/subscribe pairs (broadcast messages, subscription-sync events) + three leader/peer-pull operations (restore a snapshot from a leader, sync a delta since a timestamp, fetch a peer's local subscriptions).

No channels, feeders, or data-plane providers here — cluster control/data-plane transport only.

## Commands

```bash
dotnet restore    # fetches shared build config on first use; also needs read access to the core package feed
dotnet build
dotnet test
dotnet test <TestProject> --filter "FullyQualifiedName~<Name>"
dotnet clean      # also clears the downloaded shared build cache
```

## Architecture

- **Shared kernel** — feature-gate DI helper; generic keyed connection/client cache for transports whose client is expensive to construct per peer (dedups concurrent connects, retries instead of caching a failure, disposes on shutdown); resilience-pipeline factory (retry + circuit-breaker) — build per-peer resilience through this, not hand-rolled retry loops.
- **Transport project** — implements the bus contract; a per-transport options class; a DI extension, feature-gated through the shared helper.

Per-transport request/reply shapes: `.claude/rules/transport-patterns.md`.

## Architecture Rules (Enforced)

- Each transport assembly's public/internal types stay in its own namespace.
- No transport assembly references a sibling transport's namespace.
- The full assembly-reference graph (shared kernel + every transport) is acyclic.

## Conventions

- `internal sealed` (non-sealed in Debug via `#if !DEBUG sealed #endif`) for concrete transport classes.
- Source-generated logging methods only — never a runtime-built string to a logger call.
- Invariant culture explicitly for all culture-sensitive parsing/formatting.
- Guard-clause library for required constructor parameters — never null-forgiving operator.
- XML docs on all public API.
- Stamp a self-echo identifier on every outbound fan-out/subscription-sync message so a node can reject its own looped-back broadcast.

## Adding a Transport

New transport area → wire envelope types (request-kind enum, request envelope w/ reply-destination field, response envelope, delta-payload type) → resource-naming helper for topic/queue/subscription names → bus class + one partial file per concern (fan-out, subscription-sync, request/reply, snapshot restore/delta, subscription fetch) → DI extension → unit tests (substitute the broker client directly if mockable — non-sealed, virtual members, protected parameterless ctor — else behind a narrow custom interface) → architecture-test row for namespace isolation.

## Testing

xUnit, NSubstitute, FluentAssertions. Architecture-test project checks namespace containment, sibling isolation, acyclic graph — a new transport adds a data row, not a new test method. Both test projects have internal access to the shared kernel and every transport assembly.

## Build & Versioning

Version/TFMs centralized; CI bumps automatically. Restore fetches shared build config into a local, gitignored cache — `dotnet clean` removes it, next restore refetches.
