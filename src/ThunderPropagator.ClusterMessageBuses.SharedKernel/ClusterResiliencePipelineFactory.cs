using Polly;
using Polly.CircuitBreaker;
using Polly.Retry;

namespace ThunderPropagator.ClusterMessageBuses.SharedKernel
{
    /// <summary>
    /// Builds a transport-agnostic <see cref="ResiliencePipeline" /> (retry with exponential backoff,
    /// plus a circuit breaker) for cluster transport implementations to wrap per-peer operations in.
    /// Deliberately built directly on <c>Polly.Core</c> rather than
    /// <c>Microsoft.Extensions.Http.Resilience</c> / <c>Microsoft.Extensions.Http.Polly</c> — those
    /// packages are HttpClient-specific and don't apply to gRPC streams, ZeroMQ sockets, or broker
    /// connections. Every transport project should build its per-peer resilience pipeline through
    /// this factory instead of hand-rolling retry loops, so behavior stays consistent across
    /// transports.
    /// </summary>
    public static class ClusterResiliencePipelineFactory
    {
        /// <summary>
        /// Creates a pipeline combining a retry strategy (exponential backoff) with a circuit
        /// breaker, using <paramref name="options" /> or the defaults from
        /// <see cref="ClusterResilienceOptions" /> if omitted.
        /// </summary>
        public static ResiliencePipeline Create(ClusterResilienceOptions? options = null)
        {
            options ??= new ClusterResilienceOptions();

            return new ResiliencePipelineBuilder()
                .AddRetry(new RetryStrategyOptions
                {
                    ShouldHandle = new PredicateBuilder().Handle<Exception>(),
                    MaxRetryAttempts = options.MaxRetryAttempts,
                    Delay = options.RetryBaseDelay,
                    BackoffType = DelayBackoffType.Exponential,
                })
                .AddCircuitBreaker(new CircuitBreakerStrategyOptions
                {
                    ShouldHandle = new PredicateBuilder().Handle<Exception>(),
                    FailureRatio = options.CircuitBreakerFailureRatio,
                    SamplingDuration = options.CircuitBreakerSamplingDuration,
                    MinimumThroughput = options.CircuitBreakerMinimumThroughput,
                    BreakDuration = options.CircuitBreakerBreakDuration,
                })
                .Build();
        }
    }
}
