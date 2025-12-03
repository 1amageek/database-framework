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
            initialBatchSize: 50,
            minBatchSize: 5,
            maxBatchSize: 500
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
            initialBatchSize: 100,
            minBatchSize: 10,
            maxBatchSize: 1000,
            increaseRatio: 1.5,
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
            initialBatchSize: 900,
            minBatchSize: 10,
            maxBatchSize: 1000,
            increaseRatio: 1.5,
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
            initialBatchSize: 100,
            minBatchSize: 10,
            maxBatchSize: 1000,
            decreaseRatio: 0.5
        )
        let throttler = AdaptiveThrottler(configuration: config)

        throttler.recordFailure(error: TestError.generic)

        #expect(throttler.currentBatchSize == 50)

        let stats = throttler.statistics
        #expect(stats.totalFailures == 1)
        #expect(stats.consecutiveFailures == 1)
    }

    @Test func batchSizeDoesNotGoBelowMin() {
        let config = ThrottleConfiguration(
            initialBatchSize: 15,
            minBatchSize: 10,
            maxBatchSize: 1000,
            decreaseRatio: 0.5
        )
        let throttler = AdaptiveThrottler(configuration: config)

        throttler.recordFailure(error: TestError.generic)

        // Should be clamped to minBatchSize (15 * 0.5 = 7.5 -> 10)
        #expect(throttler.currentBatchSize == 10)
    }

    @Test func failureResetsConsecutiveSuccesses() {
        let throttler = AdaptiveThrottler()

        throttler.recordSuccess(itemCount: 100, durationNs: 1_000_000)
        throttler.recordSuccess(itemCount: 100, durationNs: 1_000_000)

        #expect(throttler.statistics.consecutiveSuccesses == 2)

        throttler.recordFailure(error: TestError.generic)

        #expect(throttler.statistics.consecutiveSuccesses == 0)
        #expect(throttler.statistics.consecutiveFailures == 1)
    }

    @Test func successResetsConsecutiveFailures() {
        let throttler = AdaptiveThrottler()

        throttler.recordFailure(error: TestError.generic)
        throttler.recordFailure(error: TestError.generic)

        #expect(throttler.statistics.consecutiveFailures == 2)

        throttler.recordSuccess(itemCount: 100, durationNs: 1_000_000)

        #expect(throttler.statistics.consecutiveFailures == 0)
        #expect(throttler.statistics.consecutiveSuccesses == 1)
    }

    // MARK: - Delay Tests

    @Test func failureIncreasesDelay() {
        let config = ThrottleConfiguration(
            initialDelayMs: 10,
            delayIncreaseRatio: 2.0
        )
        let throttler = AdaptiveThrottler(configuration: config)

        throttler.recordFailure(error: TestError.generic)

        #expect(throttler.currentDelayMs == 20)
    }

    @Test func successDecreasesDelay() {
        let config = ThrottleConfiguration(
            initialDelayMs: 100,
            delayDecreaseRatio: 0.5
        )
        let throttler = AdaptiveThrottler(configuration: config)

        throttler.recordSuccess(itemCount: 100, durationNs: 1_000_000)

        #expect(throttler.currentDelayMs == 50)
    }

    // MARK: - Retryable Error Tests

    @Test func isRetryableDetectsRetryableErrors() {
        let throttler = AdaptiveThrottler()

        #expect(throttler.isRetryable(TestError.retryable))
        #expect(throttler.isRetryable(TestError.timeout))
        #expect(throttler.isRetryable(TestError.conflict))
        #expect(throttler.isRetryable(TestError.transactionTooOld))
    }

    @Test func isRetryableRejectsNonRetryableErrors() {
        let throttler = AdaptiveThrottler()

        #expect(!throttler.isRetryable(TestError.generic))
        #expect(!throttler.isRetryable(TestError.permanent))
    }

    // MARK: - Reset Tests

    @Test func resetRestoresInitialState() {
        let config = ThrottleConfiguration(initialBatchSize: 100)
        let throttler = AdaptiveThrottler(configuration: config)

        // Modify state
        throttler.recordSuccess(itemCount: 100, durationNs: 1_000_000)
        throttler.recordFailure(error: TestError.generic)

        // Reset
        throttler.reset()

        #expect(throttler.currentBatchSize == 100)
        #expect(throttler.statistics.consecutiveSuccesses == 0)
        #expect(throttler.statistics.consecutiveFailures == 0)
    }

    @Test func resetStatisticsKeepsThrottleSettings() {
        let config = ThrottleConfiguration(
            initialBatchSize: 100,
            successesBeforeIncrease: 1
        )
        let throttler = AdaptiveThrottler(configuration: config)

        throttler.recordSuccess(itemCount: 100, durationNs: 1_000_000)
        let newBatchSize = throttler.currentBatchSize

        throttler.resetStatistics()

        // Batch size should remain
        #expect(throttler.currentBatchSize == newBatchSize)
        // Statistics should be reset
        #expect(throttler.statistics.totalSuccesses == 0)
        #expect(throttler.statistics.totalItemsProcessed == 0)
    }

    // MARK: - Statistics Tests

    @Test func statisticsSuccessRate() {
        let throttler = AdaptiveThrottler()

        throttler.recordSuccess(itemCount: 100, durationNs: 1_000_000)
        throttler.recordSuccess(itemCount: 100, durationNs: 1_000_000)
        throttler.recordSuccess(itemCount: 100, durationNs: 1_000_000)
        throttler.recordFailure(error: TestError.generic)

        let stats = throttler.statistics
        #expect(stats.successRate == 0.75)
    }

    @Test func statisticsAverageItemsPerBatch() {
        let throttler = AdaptiveThrottler()

        throttler.recordSuccess(itemCount: 100, durationNs: 1_000_000)
        throttler.recordSuccess(itemCount: 200, durationNs: 1_000_000)

        let stats = throttler.statistics
        #expect(stats.averageItemsPerBatch == 150)
    }

    @Test func statisticsThroughput() {
        let throttler = AdaptiveThrottler()

        // 100 items in 100ms = 1000 items/second
        throttler.recordSuccess(itemCount: 100, durationNs: 100_000_000)

        let stats = throttler.statistics
        #expect(stats.throughputPerSecond == 1000)
    }
}

// MARK: - Test Errors

private enum TestError: Error, CustomStringConvertible {
    case generic
    case permanent
    case retryable
    case timeout
    case conflict
    case transactionTooOld

    var description: String {
        switch self {
        case .generic: return "Generic error"
        case .permanent: return "Permanent error"
        case .retryable: return "Please retry"
        case .timeout: return "Connection timeout"
        case .conflict: return "Write conflict"
        case .transactionTooOld: return "transaction_too_old"
        }
    }
}
