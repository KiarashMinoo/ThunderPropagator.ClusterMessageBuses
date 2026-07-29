using System.Runtime.CompilerServices;
using System.Runtime.Versioning;

[assembly: RequiresPreviewFeatures]

[assembly: InternalsVisibleTo("ThunderPropagator.ArchTests")]
[assembly: InternalsVisibleTo("ThunderPropagator.UnitTests")]

// Castle DynamicProxy (NSubstitute's proxy generator) needs this to create a substitute for this
// assembly's internal transport interface (e.g. IPulsarClusterTransport).
[assembly: InternalsVisibleTo("DynamicProxyGenAssembly2")]
