# ThunderPropagator - Real-Time Data Streaming

## Project Overview
ThunderPropagator is a real-time data streaming solution providing protocol-agnostic abstractions for WebSocket, MQTT 5.0, QUIC, and WebTransport. The solution targets .NET 8.0, 9.0, and 10.0 with multi-platform support (AnyCPU, x86, x64, ARM64). Internal codename: **Project ARC** (Application Runtime Components).

## Architecture

### Layer Structure
- **Application Layer** (`src/ThunderPropagator.Application/`): Protocol-agnostic streaming abstractions (channels, feeders, pipelines, subscriptions)
- **Infrastructure Layer** (`src/ThunderPropagator.Infrastructure/`): Protocol-specific implementations (WebSocket, MQTT, QUIC, WebTransport)
- **Dependency**: Both layers depend on `ThunderPropagator.BuildingBlocks` NuGet package for core utilities (DisposableObject, EquatableObject, helpers, collections)

### Key Design Patterns

**1. Partial Class Channel Architecture**
`AbstractChannel` is split across 6 files using `partial class` to organize concerns:
- **AbstractChannel.cs** — Core properties, lifecycle, initialization
- **AbstractChannel.Subscription.cs** — Subscription management (`AddSubscriptionAsync`, `RemoveSubscriptionAsync`)
- **AbstractChannel.MessagesHandler.cs** — Message routing and distribution (`HandleMessageAsync`)
- **AbstractChannel.Metadata.cs** — Metadata initialization and script execution
- **AbstractChannel.HealthCheckSupport.cs** — Health check integration (`IHealthCheckSupport`)
- **AbstractChannel.RecoveryHandler.cs** — Snapshot backup/restore (`IRecoveryHandler`)

**2. Three-Level Channel Inheritance**
Channels use progressive specialization:
```csharp
// Base: No generics
AbstractChannel : DisposableObject, IChannel

// Typed metadata
AbstractChannel<TChannelMetadata> : AbstractChannel

// Full specialization
AbstractChannel<TChannelMetadata, TChannelConfiguration> : AbstractChannel<TChannelMetadata>
```
See [AbstractChannel.cs](src/ThunderPropagator.Application/Channels/AbstractChannel.cs) and [docs](docs/Application/Channels/README.md)

**3. Protocol Container Pattern**
Protocol implementations use container/handler separation:
- **Container**: Manages connection pool, background jobs (cleanup, health probes, send queues), implements `IHealthCheckSupport`
- **Handler**: Wraps individual connection, implements protocol-specific sending
- Factory method: `CreateConnectionHandler()` in container creates handlers
- Example: `WebSocketConnectionContainer` → `WebSocketConnectionHandler`

**4. Event-Driven Configuration with C# Scripting**
`AbstractChannelConfiguration` supports C# script hooks via `ChannelConfigurationEvents`:
```csharp
public class StockChannelConfiguration : AbstractChannelConfiguration
{
    public StockChannelConfiguration()
    {
        Events.MessageEmitting = @"(channel, message) => {
            message[""timestamp""] = DateTime.UtcNow;
        }";
    }
}
```
Scripts compiled at runtime using `Microsoft.CodeAnalysis.CSharp.Scripting`

**5. Subscription Key/Field Filtering**
Messages filtered by key values and selected fields:
- **SubscribedKey**: Key-value pairs (e.g., `symbol=AAPL`)
- **SubscribedFields**: Dictionary of field descriptors (only selected fields sent)
- **SubscriptionMode**: Full (all fields) or Incremental (changed fields only)
- Managed by `Subscription` and `SubscriptionCollection` in [Channels/Subscribers](src/ThunderPropagator.Application/Channels/Subscribers/)

**6. Pipeline Chain Pattern**
Request/response processing uses middleware-style pipelines:
- **IReceivePipeline**: Processes incoming requests (subscribe, unsubscribe, custom actions)
- **IPushPipeline**: Transforms outgoing messages before protocol sending
- Delegates: `ReceivePipelineDelegate`, `PushPipelineDelegate`
- Infrastructure provides: `SubscribePipeline`, `UnsubscribePipeline`, `AuthorizationPipeline`

## Build & Package Management

### Central Package Management
- **Versioning**: All versions in `Directory.Build.props` (e.g., `1.0.1-beta.12`)
- **Dependencies**: Centrally managed in `Directory.Packages.props` with `ManagePackageVersionsCentrally`
- **Multi-targeting**: Projects target `net8.0;net9.0;net10.0` via `TargetFrameworks` in `Directory.Build.props`
- **Multi-platform**: Supports AnyCPU, x86, x64, ARM64 via `Platforms` property
- **BuildingBlocks Dependency**: Uses `$(BuildingBlocksPackageId)` variable for package reference

### Build Commands
```powershell
dotnet restore
dotnet build -c Release
dotnet test
dotnet pack -c Release -o artifacts/pkg
```

### Configuration Flags
- `AllowUnsafeBlocks=true`: Enables unsafe code
- `GenerateDocumentationFile=true`: XML docs required for all public APIs
- `NoWarn`: Suppresses CS1591 (missing XML docs) and CS0067 (unused events)
- `LangVersion=latestmajor`: Uses latest major C# version
- Debug builds append `.Debug` suffix to package IDs
- `EnablePreviewFeatures=true` in test projects only

### Package Publishing
- Package IDs: `ThunderPropagator.Application`, `ThunderPropagator.Infrastructure`
- All packages include `ThunderPropagator.png` and `README.md`
- Auto-generated on build when `IsPackable=true` and `GeneratePackageOnBuild=true`
- Output to `artifacts/pkg/` directory

## Testing Strategy

### Test Organization
- **Unit Tests**: `Tests/ThunderPropagator.UnitTests/` - xUnit with NSubstitute for mocking
- **Arch Tests**: `Tests/ThunderPropagator.ArchTests/` - NetArchTest.Rules for architecture validation (currently minimal)
- **Test Mocks**: `ChannelMock.cs`, `ServiceProviderMock.cs` for test infrastructure

### Running Tests
```powershell
dotnet test -c Release
# For specific test
dotnet test --filter "FullyQualifiedName~ConnectionSubscriptionPushingMessageTest"
```

## CI/CD Workflows

### Release Process
- **develop** branch → `develop-beta-ci.yml` → increments beta version (e.g., `1.0.1-beta.5`)
- **release/** branch → `develop-release-ci.yml` → strips beta suffix, creates GitHub release, syncs back to develop
- GitHub Packages feed: `https://nuget.pkg.github.com/KiarashMinoo/index.json`

### Version Management
Scripts in `.github/scripts/` handle version bumps. Never manually edit version in `Directory.Build.props` outside of release workflows.

## Code Conventions

### Naming & Style
- Use `CallerArgumentExpression` for guard clauses: `Guard.Against.Null(param)`
- Internal fields: `_camelCase` with underscore prefix
- Platform names: `MacOs` not `MacOS`, `onAcPower` not `onACPower`
- Activity naming convention: `{ClassName}_{MethodName}` for telemetry
- Sealed classes in DEBUG builds become non-sealed for testability

### FeederMessage Pattern
`FeederMessage` is the core message abstraction from BuildingBlocks - a dictionary-based class implementing `IDictionary<string, object?>`:
- Properties stored in internal `ConcurrentDictionary`
- Use `GetValueOrDefault<T>()` and `SetValue()` for type-safe access
- Supports correlation ID tracking via `ICorrelationIdSupport`

### DisposableObject Base Class
From BuildingBlocks - consistent disposal pattern for all resources:
- Abstract base class with `IDisposable` and `IAsyncDisposable`
- Override `DisposeManagedResources()` or `DisposeUnmanagedResources()`
- Thread-safe disposal tracking with `IsDisposed` flag

### DI Registration Pattern
Infrastructure components use extension methods on `IServiceCollection`:
```csharp
services.AddThunderPropagator(configuration.GetSection("ThunderPropagator"));
app.UseThunderPropagator();
```
See [ThunderPropagatorExtensions.cs](src/ThunderPropagator.Infrastructure/Extensions/ThunderPropagatorExtensions.cs)

### Specialized Collections
From BuildingBlocks package:
- **BindingDictionary<TKey, TValue>**: Dictionary with data binding support
- **GenericOrderedDictionary<TKey, TValue>**: Ordered dictionary implementation

## Documentation

- Main docs: `docs/README.md` - comprehensive catalog
- Component-level: `docs/Application/README.md` and `docs/Infrastructure/README.md`
- Feature docs: See `docs/Application/Channels/README.md` for detailed channel documentation

## Common Tasks

### Adding New Channel
1. Create configuration class inheriting `AbstractChannelConfiguration`
2. Define metadata class implementing `IChannelMetadata`
3. Create channel class inheriting `AbstractChannel<TMetadata, TConfiguration>`
4. Register in DI via `services.TryAddSingleton<YourChannel>()`
5. Add to `ChannelManager` initialization
6. Document in `docs/`

### Adding New Protocol
1. Create connection info class in `Protocols/{ProtocolName}/`
2. Create connection handler inheriting `AbstractConnectionHandler<TGateway, TConnectionInfo, TPushMessageConfiguration>`
3. Create connection container inheriting `AbstractConnectionContainer<...>`
4. Implement `CreateConnectionHandler()` factory method in container
5. Register container as singleton with `AddHealthCheckSupport<TContainer>()`
6. Add protocol configuration to `ThunderPropagatorExtensions.AddThunderPropagator()`

### Adding Pipeline
1. Create pipeline class inheriting `AbstractReceivePipeline` or `AbstractPushPipeline`
2. Override `InvokeAsync(context, next)` method
3. Call `await next(context)` to continue chain
4. Register in DI and configure in pipeline builder
5. Add tests in `Tests/ThunderPropagator.UnitTests/Pipelines/`

### Creating Custom FeederMessage
Inherit from `FeederMessage` (from BuildingBlocks) and add strongly-typed properties:
```csharp
public class MyMessage : FeederMessage
{
    public Guid Id
    {
        get => GetValueOrDefault(Guid.NewGuid());
        set => SetValue(value);
    }
    
    public string? Name
    {
        get => GetValueOrNull<string>();
        set => SetValue(value);
    }
}
```

### Creating Feeder
Inherit from `AbstractFeeder` and implement data fetching:
```csharp
public class MyFeeder : IterativeFeeder<MyChannel, MyMessage, MyFeederConfiguration>
{
    protected override async Task<IEnumerable<MyMessage>> FetchAsync()
    {
        // Fetch data from source
        return await _dataSource.GetLatestAsync();
    }
}
```

### Publishing Packages
Packages auto-publish via GitHub Actions. Manual publish:
```powershell
dotnet pack -c Release -o artifacts/pkg
dotnet nuget push artifacts/pkg/*.nupkg --source github --api-key $GITHUB_TOKEN
```
