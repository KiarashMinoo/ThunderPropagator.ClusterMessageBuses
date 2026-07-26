using System.Net;
using System.Net.Http;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using ThunderPropagator.Application.Channels.Cluster;
using ThunderPropagator.Application.Channels.Cluster.Discovery;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;
using ThunderPropagator.ClusterMessageBuses.WebApi;

namespace ThunderPropagator.UnitTests.WebApi;

/// <summary>
/// Shared construction helpers for <see cref="WebApiClusterMessageBus"/> tests. Every test
/// constructs the bus through the same seams the production DI extension uses
/// (<c>IOptions&lt;WebApiClusterMessageBusOptions&gt;</c>, <c>ClusterConfiguration</c>,
/// <c>IClusterChannelResolver</c>, <c>IClusterNodeDiscovery</c>, an injectable listener factory, and
/// an injectable <see cref="IWebApiClusterHttpClient"/>) so no test ever binds a real socket or makes
/// a real HTTP call.
/// </summary>
internal static class WebApiClusterMessageBusTestHelpers
{
    internal static readonly Uri DefaultNodeEndpoint = new("https://node1:5000/");

    internal static IWebApiClusterHttpClient CreateHttpClientSubstitute(HttpStatusCode statusCode = HttpStatusCode.OK, string body = "{}")
    {
        var httpClient = Substitute.For<IWebApiClusterHttpClient>();
        httpClient.SendAsync(Arg.Any<HttpRequestMessage>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(new HttpResponseMessage(statusCode) { Content = new StringContent(body) }));

        return httpClient;
    }

    /// <summary>
    /// Builds a substitute <see cref="IWebApiClusterListener"/> whose <c>AcceptRequestsAsync</c>
    /// yields <paramref name="requestsToAccept"/> (if any) and then blocks, respecting cancellation,
    /// exactly like a real listener that simply has no further inbound requests.
    /// </summary>
    internal static IWebApiClusterListener CreateListenerSubstitute(IEnumerable<WebApiIncomingRequest>? requestsToAccept = null)
    {
        var requests = (requestsToAccept ?? []).ToArray();

        var listener = Substitute.For<IWebApiClusterListener>();
        listener.AcceptRequestsAsync(Arg.Any<CancellationToken>())
            .Returns(callInfo => AcceptAsync(requests, (CancellationToken)callInfo[0]));
        listener.DisposeAsync().Returns(ValueTask.CompletedTask);

        return listener;
    }

    private static async IAsyncEnumerable<WebApiIncomingRequest> AcceptAsync(
        WebApiIncomingRequest[] requests,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        foreach (var request in requests)
        {
            yield return request;
        }

        try
        {
            await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Expected once the bus is disposed and its lifetime token is cancelled.
        }
    }

    internal static Func<CancellationToken, Task<IWebApiClusterListener>> ListenerFactoryReturning(IWebApiClusterListener listener)
        => _ => Task.FromResult(listener);

    internal static WebApiIncomingRequest CreateIncomingRequest(string method, string path, string? query, string body, out List<(int StatusCode, string Body)> responses)
    {
        var capturedResponses = new List<(int, string)>();
        responses = capturedResponses;

        return new WebApiIncomingRequest
        {
            Method = method,
            Path = path,
            Query = query,
            Body = body,
            RespondAsync = (statusCode, responseBody) =>
            {
                capturedResponses.Add((statusCode, responseBody));
                return Task.CompletedTask;
            },
        };
    }

    internal static IClusterNodeDiscovery CreateDiscoverySubstitute(IReadOnlyList<ClusterNodeEntry> peers)
    {
        var discovery = Substitute.For<IClusterNodeDiscovery>();
        discovery.GetPeersAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(peers));
        return discovery;
    }

    internal static async Task<WebApiClusterMessageBus> CreateBusAsync(
        IEnumerable<WebApiIncomingRequest>? acceptedRequests = null,
        IClusterNodeDiscovery? discovery = null,
        IClusterChannelResolver? channelResolver = null,
        Uri? nodeEndpoint = null,
        TimeSpan? requestTimeout = null,
        IWebApiClusterHttpClient? httpClient = null)
    {
        discovery ??= CreateDiscoverySubstitute([]);
        channelResolver ??= Substitute.For<IClusterChannelResolver>();
        httpClient ??= CreateHttpClientSubstitute();

        var listener = CreateListenerSubstitute(acceptedRequests);

        var options = Options.Create(new WebApiClusterMessageBusOptions
        {
            RequestTimeout = requestTimeout ?? TimeSpan.FromSeconds(30),
        });

        var clusterConfiguration = new ClusterConfiguration { NodeEndpoint = nodeEndpoint ?? DefaultNodeEndpoint };

        var bus = new WebApiClusterMessageBus(
            options,
            clusterConfiguration,
            channelResolver,
            discovery,
            NullLoggerFactory.Instance,
            ListenerFactoryReturning(listener),
            httpClient);

        await bus.EnsureInitializedAsync(CancellationToken.None);

        return bus;
    }
}
