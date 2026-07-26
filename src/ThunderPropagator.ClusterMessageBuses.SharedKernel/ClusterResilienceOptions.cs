namespace ThunderPropagator.ClusterMessageBuses.SharedKernel
{
    /// <summary>
    /// Configures the retry and circuit-breaker behavior built by
    /// <see cref="ClusterResiliencePipelineFactory" />. Defaults are sensible for a peer-to-peer
    /// cluster connection — a few quick retries, then a short circuit-open window so a persistently
    /// unreachable peer doesn't get hammered.
    /// </summary>
    public sealed class ClusterResilienceOptions
    {
        /// <summary>Maximum number of retry attempts before giving up. Default: 3.</summary>
        public int MaxRetryAttempts { get; set; } = 3;

        /// <summary>Base delay for exponential backoff between retries. Default: 1 second.</summary>
        public TimeSpan RetryBaseDelay { get; set; } = TimeSpan.FromSeconds(1);

        /// <summary>
        /// Fraction of sampled operations that must fail within <see cref="CircuitBreakerSamplingDuration" />
        /// before the circuit opens. Default: 0.5 (50%).
        /// </summary>
        public double CircuitBreakerFailureRatio { get; set; } = 0.5;

        /// <summary>Rolling window over which the failure ratio is evaluated. Default: 30 seconds.</summary>
        public TimeSpan CircuitBreakerSamplingDuration { get; set; } = TimeSpan.FromSeconds(30);

        /// <summary>Minimum number of sampled operations before the circuit breaker can open. Default: 10.</summary>
        public int CircuitBreakerMinimumThroughput { get; set; } = 10;

        /// <summary>How long the circuit stays open before allowing a trial operation through. Default: 30 seconds.</summary>
        public TimeSpan CircuitBreakerBreakDuration { get; set; } = TimeSpan.FromSeconds(30);
    }
}
