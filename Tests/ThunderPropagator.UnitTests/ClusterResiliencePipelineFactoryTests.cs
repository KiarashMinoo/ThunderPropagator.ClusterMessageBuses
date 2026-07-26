using FluentAssertions;
using Polly.CircuitBreaker;
using ThunderPropagator.ClusterMessageBuses.SharedKernel;

namespace ThunderPropagator.UnitTests;

public class ClusterResiliencePipelineFactoryTests
{
    [Fact]
    public async Task Create_TransientFailureWithinRetryBudget_RetriesAndEventuallySucceeds()
    {
        // Arrange
        var pipeline = ClusterResiliencePipelineFactory.Create(new ClusterResilienceOptions
        {
            MaxRetryAttempts = 3,
            RetryBaseDelay = TimeSpan.FromMilliseconds(1),
            // High enough that the breaker never opens during this test's small failure count.
            CircuitBreakerMinimumThroughput = 100,
        });

        var attempt = 0;

        // Act
        var result = await pipeline.ExecuteAsync(async _ =>
        {
            attempt++;
            if (attempt < 3)
                throw new InvalidOperationException("transient");

            await Task.Yield();
            return "ok";
        });

        // Assert
        result.Should().Be("ok");
        attempt.Should().Be(3);
    }

    [Fact]
    public async Task Create_FailuresExceedRetryBudget_ThrowsUnderlyingException()
    {
        // Arrange
        var pipeline = ClusterResiliencePipelineFactory.Create(new ClusterResilienceOptions
        {
            MaxRetryAttempts = 2,
            RetryBaseDelay = TimeSpan.FromMilliseconds(1),
            CircuitBreakerMinimumThroughput = 100,
        });

        var attempts = 0;

        // Act
        var act = async () => await pipeline.ExecuteAsync<string>(_ =>
        {
            attempts++;
            throw new InvalidOperationException("always fails");
        });

        // Assert
        await act.Should().ThrowAsync<InvalidOperationException>();
        attempts.Should().Be(3); // 1 initial attempt + 2 retries
    }

    [Fact]
    public async Task Create_RepeatedFailuresReachMinimumThroughput_OpensCircuitAndStopsInvokingCallback()
    {
        // Arrange — MaxRetryAttempts: 0 isolates circuit-breaker behavior from the retry strategy.
        var pipeline = ClusterResiliencePipelineFactory.Create(new ClusterResilienceOptions
        {
            MaxRetryAttempts = 0,
            CircuitBreakerFailureRatio = 1.0,
            CircuitBreakerMinimumThroughput = 2,
            CircuitBreakerSamplingDuration = TimeSpan.FromSeconds(30),
            CircuitBreakerBreakDuration = TimeSpan.FromSeconds(30),
        });

        var invocations = 0;

        async Task InvokeAsync()
        {
            await pipeline.ExecuteAsync<string>(_ =>
            {
                invocations++;
                throw new InvalidOperationException("always fails");
            });
        }

        // Trip the breaker with MinimumThroughput failing calls.
        for (var i = 0; i < 2; i++)
        {
            var act = async () => await InvokeAsync();
            await act.Should().ThrowAsync<InvalidOperationException>();
        }

        var invocationsBeforeOpen = invocations;

        // Act — the circuit should now be open; the next call must fail fast without invoking the callback.
        var actAfterOpen = async () => await InvokeAsync();

        // Assert
        await actAfterOpen.Should().ThrowAsync<BrokenCircuitException>();
        invocations.Should().Be(invocationsBeforeOpen);
    }
}
