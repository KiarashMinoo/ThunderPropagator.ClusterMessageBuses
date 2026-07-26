# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

`ThunderPropagator.ClusterMessageBuses` provides pluggable **cluster inter-node transport**
implementations for [ThunderPropagator](https://github.com/KiarashMinoo/ThunderPropagator) — i.e.
implementations of `IClusterMessageBus` (contract defined in core `ThunderPropagator.Application`,
see core issue [#340](https://github.com/KiarashMinoo/ThunderPropagator/issues/340)) that a
consuming application registers via dependency injection to replace or supplement core's built-in
HttpClient-based cluster transport.

This repo is intentionally structured like
[ThunderPropagator.Feeviders](https://github.com/KiarashMinoo/ThunderPropagator.Feeviders): a
shared kernel plus one project per transport. The difference is what each transport project
implements — Feeviders projects implement `IFeeder<TChannel>` / `IProvider<T>` (data-plane
consume/publish against external messaging systems), while this repo's projects implement
`IClusterMessageBus` (cluster control/data-plane: fan-out push to peer nodes, subscription
replication across peers, node discovery).

This repo does **not** contain channels, feeders, or data-plane providers — those live in core
`ThunderPropagator`, `ThunderPropagator.Channels`, and `ThunderPropagator.Feeviders`.

## Build & Test Commands

```bash
# Restore (also downloads shared build props from ThunderPropagator.SharedBuild)
dotnet restore

# Build all projects
dotnet build

# Run all tests
dotnet test

# Run a specific test project
dotnet test Tests/ThunderPropagator.UnitTests/
dotnet test Tests/ThunderPropagator.ArchTests/

# Run a single test by name
dotnet test Tests/ThunderPropagator.UnitTests/ --filter "FullyQualifiedName~<TestMethodName>"

# Clean (removes .shared-props/ folder — next restore re-downloads them)
dotnet clean
```

**Important:** The first `dotnet restore` or `dotnet build` downloads `Shared.Build.props` and
`Shared.Nuget.props` from the `ThunderPropagator.SharedBuild` GitHub repo into `.shared-props/`. If
the build fails with `CS0246` type-not-found errors, re-run `dotnet restore` (network issue during
download). Restoring also requires read access to the `KiarashMinoo` GitHub Packages feed for the
`ThunderPropagator` and `ThunderPropagator.BuildingBlocks` package dependencies (`GH_TOKEN`).

## Current State (scaffolding only)

As of issue [#1](https://github.com/KiarashMinoo/ThunderPropagator.ClusterMessageBuses/issues/1),
this repo contains only:

```
src/
└── ThunderPropagator.ClusterMessageBuses.SharedKernel/   # shared helpers, no transports yet

Tests/
├── ThunderPropagator.ArchTests/     # NetArchTest.Rules; namespace/dependency-direction enforcement
└── ThunderPropagator.UnitTests/     # xUnit + NSubstitute + FluentAssertions
```

No `IClusterMessageBus` implementation exists yet — `SharedKernel` only has the cross-transport
helpers every future implementation will build on. **Do not add a hard compile-time dependency on
`IClusterMessageBus` itself** until core issue #340 ships and this repo's `ThunderPropagatorVersion`
pin (`Directory.Packages.props`) is bumped to a version that publishes it; `SharedKernel`'s
`AddFeature<T>()` wrapper only needs `IFeature`, which already exists in the currently pinned
version.

### Roadmap

One `IClusterMessageBus` implementation project per transport is planned, mirroring the systems
already covered in Feeviders, plus two protocols Feeviders itself only just gained:

- gRPC — tracked by issue [#2](https://github.com/KiarashMinoo/ThunderPropagator.ClusterMessageBuses/issues/2)
- ZeroMQ — tracked by issue [#3](https://github.com/KiarashMinoo/ThunderPropagator.ClusterMessageBuses/issues/3)
- Kafka, RabbitMQ, NATS, Pulsar, MQTT, ActiveMQ, RedisPubSub, WebSocket, WebApi, TcpSocket,
  UdpClient, AwsSqs, AzureServiceBus, GcpPubSub — to be filed incrementally as follow-up tickets

Each transport, once added, follows this layout (adapt names per transport):

| File/Project | Base Type | Visibility |
|---|---|---|
| `ThunderPropagator.ClusterMessageBuses.{Transport}/{Transport}ClusterMessageBus.cs` | implements `IClusterMessageBus` | internal sealed (non-sealed in DEBUG) |
| `ThunderPropagator.ClusterMessageBuses.{Transport}/{Transport}ClusterMessageBusOptions.cs` | plain options class | public |
| `ThunderPropagator.ClusterMessageBuses.{Transport}/{Transport}ClusterMessageBusExtensions.cs` | static DI registration (`AddCluster{Transport}MessageBus()`) | public static |

## Architecture

### SharedKernel (`src/ThunderPropagator.ClusterMessageBuses.SharedKernel/`)

- **`ThunderPropagatorExtensions.AddFeature<TFeature>()`** — thin wrapper delegating to core
  `ThunderPropagator.Infrastructure.Extensions.ThunderPropagatorExtensions.AddFeature<TFeature>()`,
  matching the pattern already used in `ThunderPropagator.RecoveryHandlers.SharedKernel`. Every
  transport's DI extension should feature-gate itself through this rather than registering
  unconditionally.
- **`ClusterConnectionCache<TConnection>`** — generic keyed connection/client cache
  (`ConcurrentDictionary<string, Lazy<Task<TConnection>>>` under the hood) for transports whose
  underlying client is expensive to construct per peer (a gRPC `ChannelBase`, a ZeroMQ socket, a
  broker client). Dedups concurrent construction for the same key, retries instead of permanently
  caching a failed connect, and disposes every cached connection on container shutdown if
  `TConnection` is `IAsyncDisposable`/`IDisposable`. Mirrors the pattern established by
  `RedisConnectionMultiplexerCache` / `MongoClientCache` in `ThunderPropagator.RecoveryHandlers`,
  generalized so every future transport project can reuse the same cache instead of reimplementing
  it.
- **`ClusterResiliencePipelineFactory`** — builds a `Polly.Core` `ResiliencePipeline` with a retry
  (exponential backoff) plus circuit-breaker strategy, using `Polly.Core` directly rather than
  `Microsoft.Extensions.Http.Resilience` / `Microsoft.Extensions.Http.Polly` — those are
  HttpClient-specific and don't apply to gRPC streams, ZeroMQ sockets, or broker connections.
  Transport projects should build their per-peer resilience pipeline through this factory instead
  of hand-rolling retry loops, so behavior (and its unit test coverage) stays consistent across
  transports.

### Architecture Rules (Enforced by ArchTests)

- Each transport assembly (once added) must keep all its public/internal types within its own
  namespace.
- No transport assembly may reference a sibling transport's namespace — transports must remain
  independently deployable/versionable, same rule as `ThunderPropagator.RecoveryHandlers` and
  `ThunderPropagator.Feeviders`.
- The full assembly reference graph (SharedKernel + every transport) must be acyclic.

`Tests/ThunderPropagator.ArchTests/ClusterMessageBusArchitectureTests.cs` currently only checks
`SharedKernel`'s own namespace containment (there's nothing else to check yet) but is structured so
each new transport project just adds a row to the existing `TheoryData` sets rather than requiring
new test methods.

### Code Conventions

- `internal sealed` (non-sealed in `DEBUG` via `#if !DEBUG sealed #endif`) for concrete transport
  classes, matching the pattern used across every `ThunderPropagator.*` repo.
- `[LoggerMessage]` source-generated partial methods for all logging — never
  `Logger.LogError(exception, someRuntimeString)`.
- All culture-sensitive parsing/formatting (numeric types, `DateTime`) must use
  `CultureInfo.InvariantCulture` explicitly.
- `Guard.Against.Null(...)` / `Guard.Against.NullOrWhiteSpace(...)` (Ardalis.GuardClauses) for
  required constructor parameters — never suppress with the `!` null-forgiving operator.
- XML docs are required for all public APIs (`GenerateDocumentationFile=true`; build fails without
  them).

### Build Infrastructure

`Directory.Build.props` is the single source of truth for versioning (`<Version>`) and target
frameworks (`net8.0;net9.0;net10.0`). It downloads two shared property files at restore/build time:

| File | Purpose |
|---|---|
| `.shared-props/Shared.Build.props` | SDK-wide settings: TFM, nullable, warnings-as-errors, etc. |
| `.shared-props/Shared.Nuget.props` | NuGet metadata: authors, license, icon, package tags |

`Directory.Packages.props` manages all NuGet dependency versions centrally (CPM). Add new packages
there; never specify `Version` on individual `<PackageReference>` items.

### Versioning

The version is set in `Directory.Build.props` under `<Version>`. This repo has not published a
NuGet package yet — the CI pipeline will begin managing beta/release version bumps automatically
once publishing is switched on (see `.github/workflows/ci.yml`).

### Solution File

The solution uses the `.slnx` format (`ThunderPropagator.ClusterMessageBuses.slnx`) required by
JetBrains Rider, matching every other repo in the ThunderPropagator family.

### Testing

- `ThunderPropagator.UnitTests` — xUnit, NSubstitute, FluentAssertions; targets `net10.0` only.
- `ThunderPropagator.ArchTests` — NetArchTest.Rules; namespace containment and acyclic dependency
  graph checks, extensible per transport.
