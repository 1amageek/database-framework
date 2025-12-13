// AdaptiveThrottlerTests.swift
// Tests for AdaptiveThrottler

import Testing
import Foundation
@testable import DatabaseEngine

@Suite("AdaptiveThrottler Tests")
struct AdaptiveThrottlerTests {

    // MARK: - Configuration Tests

    @Test func defaultConfiguration() {
        let config = ThrottleConfiguration.default

        #expect(config.initialBatchSize == 100)
        #expect(config.minBatchSize == 10)
        #expect(config.maxBatchSize == 1000)
        #expect(config.increaseRatio == 1.5)
        #expect(config.decreaseRatio == 0.5)
        #expect(config.successesBeforeIncrease == 3)
    }

    @Test func conservativeConfiguration() {
        let config = ThrottleConfiguration.conservative

        #expect(config.initialBatchSize == 50)
        #expect(config.maxBatchSize == 200)
        #expect(config.successesBeforeIncrease == 5)
    }

    @Test func aggressiveConfiguration() {
        let config = ThrottleConfiguration.aggressive

        #expect(config.initialBatchSize == 200)
        #expect(config.maxBatchSize == 2000)
        #expect(config.increaseRatio == 2.0)
    }

    @Test func nestedBatchSettingsConfiguration() {
        let batch = ThrottleConfiguration.BatchSettings(
            initial: 200,
            min: 50,
            max: 500,
            increaseRatio: 2.0,
            decreaseRatio: 0.3
        )
        let config = ThrottleConfiguration(batch: batch)

        #expect(config.batch.initial == 200)
        #expect(config.batch.min == 50)
        #expect(config.batch.max == 500)
        #expect(config.batch.increaseRatio == 2.0)
        #expect(config.batch.decreaseRatio == 0.3)

        // Convenience accessors
        #expect(config.initialBatchSize == 200)
        #expect(config.minBatchSize == 50)
        #expect(config.maxBatchSize == 500)
    }

    @Test func nestedDelaySettingsConfiguration() {
        let delay = ThrottleConfiguration.DelaySettings(
            min: 10,
            max: 2000,
            initial: 100,
            increaseRatio: 3.0,
            decreaseRatio: 0.8
        )
        let config = ThrottleConfiguration(delay: delay)

        #expect(config.delay.min == 10)
        #expect(config.delay.max == 2000)
        #expect(config.delay.initial == 100)
        #expect(config.delay.increaseRatio == 3.0)
        #expect(config.delay.decreaseRatio == 0.8)

        // Convenience accessors
        #expect(config.minDelayMs == 10)
        #expect(config.maxDelayMs == 2000)
        #expect(config.initialDelayMs == 100)
    }

    // MARK: - Initial State Tests

    @Test func initialState() {
        let throttler = AdaptiveThrottler()

        #expect(throttler.currentBatchSize == 100)
        #expect(throttler.currentDelayMs == 0)

        let stats = throttler.statistics
        #expect(stats.totalSuccesses == 0)
        #expect(stats.totalFailures == 0)
        #expect(stats.consecutiveSuccesses == 0)
        #expect(stats.consecutiveFailures == 0)
    }

    @Test func customConfiguration() {
        let config = ThrottleConfiguration(
            batch: .init(initial: 50, min: 5, max: 500)
        )
        let throttler = AdaptiveThrottler(configuration: config)

        #expect(throttler.currentBatchSize == 50)
    }

    // MARK: - Success Recording Tests

    @Test func recordSuccessIncrementsCounter() {
        let throttler = AdaptiveThrottler()

        throttler.recordSuccess(itemCount: 100, durationNs: 1_000_000)

        let stats = throttler.statistics
        #expect(stats.totalSuccesses == 1)
        #expect(stats.consecutiveSuccesses == 1)
        #expect(stats.totalItemsProcessed == 100)
    }

    @Test func batchSizeIncreasesAfterConsecutiveSuccesses() {
        let config = ThrottleConfiguration(
            batch: .init(initial: 100, min: 10, max: 1000, increaseRatio: 1.5),
            successesBeforeIncrease: 3
        )
        let throttler = AdaptiveThrottler(configuration: config)

        // Record 3 successes
        for _ in 0..<3 {
            throttler.recordSuccess(itemCount: 100, durationNs: 1_000_000)
        }

        // Batch size should increase by 1.5x
        #expect(throttler.currentBatchSize == 150)
    }

    @Test func batchSizeDoesNotExceedMax() {
        let config = ThrottleConfiguration(
            batch: .init(initial: 900, min: 10, max: 1000, increaseRatio: 1.5),
            successesBeforeIncrease: 1
        )
        let throttler = AdaptiveThrottler(configuration: config)

        throttler.recordSuccess(itemCount: 100, durationNs: 1_000_000)

        // Should be clamped to maxBatchSize
        #expect(throttler.currentBatchSize == 1000)
    }

    // MARK: - Failure Recording Tests

    @Test func recordFailureDecreasesBatchSize() {
        let config = ThrottleConfiguration(
            batch: .init(initial: 100, min: 10, max: 1000, decreaseRatio: 0.5)
        )
        let throttler = AdaptiveThrottler(configuration: config)

        throttler.recordFailure(error: ThrottlerTestError.generic)

        #expect(throttler.currentBatchSize == 50)

        let stats = throttler.statistics
        #expect(stats.totalFailures == 1)
        #expect(stats.consecutiveFailures == 1)
    }

    @Test func batchSizeDoesNotGoBelowMin() {
        let config = ThrottleConfiguration(
            batch: .init(initial: 15, min: 10, max: 1000, decreaseRatio: 0.5)
        )
        let throttler = AdaptiveThrottler(configuration: config)

        throttler.recordFailure(error: ThrottlerTestError.generic)

        // Should be clamped to minBatchSize (15 * 0.5 = 7.5 -> 10)
        #expect(throttler.currentBatchSize == 10)
    }

    @Test func failureResetsConsecutiveSuccesses() {
        let throttler = AdaptiveThrottler()

        throttler.recordSuccess(itemCount: 100, durationNs: 1_000_000)
        throttler.recordSuccess(itemCount: 100, durationNs: 1_000_000)

        #expect(throttler.statistics.consecutiveSuccesses == 2)

        throttler.recordFailure(error: ThrottlerTestError.generic)

        #expect(throttler.statistics.consecutiveSuccesses == 0)
        #expect(throttler.statistics.consecutiveFailures == 1)
    }

    @Test func successResetsConsecutiveFailures() {
        let throttler = AdaptiveThrottler()

        throttler.recordFailure(error: ThrottlerTestError.generic)
        throttler.recordFailure(error: ThrottlerTestError.generic)

        #expect(throttler.statistics.consecutiveFailures == 2)

        throttler.recordSuccess(itemCount: 100, durationNs: 1_000_000)

        #expect(throttler.statistics.consecutiveFailures == 0)
        #expect(throttler.statistics.consecutiveSuccesses == 1)
    }

    // MARK: - Delay Tests

    @Test func failureIncreasesDelay() {
        let config = ThrottleConfiguration(
            delay: .init(min: 0, max: 1000, initial: 10, increaseRatio: 2.0)
        )
        let throttler = AdaptiveThrottler(configuration: config)

        throttler.recordFailure(error: ThrottlerTestError.generic)

        #expect(throttler.currentDelayMs == 20)
    }

    @Test func successDecreasesDelay() {
        let config = ThrottleConfiguration(
            delay: .init(min: 0, max: 1000, initial: 100, decreaseRatio: 0.5)
        )
        let throttler = AdaptiveThrottler(configuration: config)

        throttler.recordSuccess(itemCount: 100, durationNs: 1_000_000)

        #expect(throttler.currentDelayMs == 50)
    }

    // MARK: - Retryable Error Tests

    @Test func isRetryableDetectsFDBRetryableErrors() {
        let throttler = AdaptiveThrottler()

        // Generic non-FDB errors are not retryable
        #expect(!throttler.isRetryable(ThrottlerTestError.generic))
        #expect(!throttler.isRetryable(ThrottlerTestError.permanent))
    }

    // MARK: - Reset Tests

    @Test func resetRestoresInitialState() {
        let config = ThrottleConfiguration(batch: .init(initial: 100))
        let throttler = AdaptiveThrottler(configuration: config)

        // Modify state
        throttler.recordSuccess(itemCount: 100, durationNs: 1_000_000)
        throttler.recordFailure(error: ThrottlerTestError.generic)

        // Reset
        throttler.reset()

        #expect(throttler.currentBatchSize == 100)
        #expect(throttler.statistics.consecutiveSuccesses == 0)
        #expect(throttler.statistics.consecutiveFailures == 0)
        #expect(throttler.statistics.totalSuccesses == 0)
        #expect(throttler.statistics.totalFailures == 0)
    }

    // MARK: - Statistics Tests

    @Test func statisticsSuccessRate() {
        let throttler = AdaptiveThrottler()

        throttler.recordSuccess(itemCount: 100, durationNs: 1_000_000)
        throttler.recordSuccess(itemCount: 100, durationNs: 1_000_000)
        throttler.recordSuccess(itemCount: 100, durationNs: 1_000_000)
        throttler.recordFailure(error: ThrottlerTestError.generic)

        let stats = throttler.statistics
        #expect(stats.successRate == 0.75)
    }

    @Test func statisticsAvgItemsPerSecond() {
        let throttler = AdaptiveThrottler()

        // 100 items in 100ms = 1000 items/second
        throttler.recordSuccess(itemCount: 100, durationNs: 100_000_000)

        let stats = throttler.statistics
        #expect(stats.avgItemsPerSecond == 1000)
    }

    // MARK: - Description Tests

    @Test func configurationDescription() {
        let defaultConfig = ThrottleConfiguration.default
        #expect(defaultConfig.description == "ThrottleConfiguration.default")

        let customConfig = ThrottleConfiguration(
            batch: .init(initial: 200),
            successesBeforeIncrease: 5
        )
        #expect(customConfig.description.contains("batch:"))
        #expect(customConfig.description.contains("successesBeforeIncrease: 5"))
    }

    @Test func batchSettingsDescription() {
        let batch = ThrottleConfiguration.BatchSettings(initial: 100, min: 10, max: 500)
        #expect(batch.description.contains("initial: 100"))
        #expect(batch.description.contains("min: 10"))
        #expect(batch.description.contains("max: 500"))
    }
}

// MARK: - Test Errors

private enum ThrottlerTestError: Error, CustomStringConvertible {
    case generic
    case permanent

    var description: String {
        switch self {
        case .generic: return "Generic error"
        case .permanent: return "Permanent error"
        }
    }
}
