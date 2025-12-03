// TransactionConfigurationTests.swift
// DatabaseEngine Tests - TransactionConfiguration and related types

import Testing
import Foundation
import FoundationDB
@testable import DatabaseEngine

@Suite("TransactionConfiguration Tests")
struct TransactionConfigurationTests {

    // MARK: - Preset Tests

    @Test("Default configuration has no special options")
    func defaultConfiguration() {
        let config = TransactionConfiguration.default

        #expect(config.priority == nil)
        #expect(config.readPriority == nil)
        #expect(config.timeout == nil)
        #expect(config.retryLimit == nil)
        #expect(config.maxRetryDelay == nil)
        #expect(config.useGrvCache == false)
        #expect(config.snapshotRywDisable == false)
        #expect(config.debugTransactionIdentifier == nil)
        #expect(config.logTransaction == false)
        #expect(config.tags.isEmpty)
    }

    @Test("ReadOnly configuration uses GRV cache")
    func readOnlyConfiguration() {
        let config = TransactionConfiguration.readOnly

        #expect(config.useGrvCache == true)
        #expect(config.priority == nil)
    }

    @Test("Batch configuration has low priority and longer timeout")
    func batchConfiguration() {
        let config = TransactionConfiguration.batch

        #expect(config.priority == .priorityBatch)
        #expect(config.readPriority == .readPriorityLow)
        #expect(config.timeout == 30_000)
        #expect(config.retryLimit == 20)
        #expect(config.maxRetryDelay == 5_000)
    }

    @Test("System configuration has highest priority")
    func systemConfiguration() {
        let config = TransactionConfiguration.system

        #expect(config.priority == .prioritySystemImmediate)
        #expect(config.readPriority == .readPriorityHigh)
        #expect(config.timeout == 2_000)
        #expect(config.retryLimit == 5)
        #expect(config.maxRetryDelay == 100)
    }

    @Test("Interactive configuration has short timeout")
    func interactiveConfiguration() {
        let config = TransactionConfiguration.interactive

        #expect(config.timeout == 1_000)
        #expect(config.retryLimit == 3)
        #expect(config.maxRetryDelay == 50)
        #expect(config.priority == nil) // Default priority
    }

    // MARK: - Builder Pattern Tests

    @Test("withTimeout creates copy with new timeout")
    func withTimeout() {
        let original = TransactionConfiguration.batch
        let modified = original.withTimeout(10_000)

        #expect(modified.timeout == 10_000)
        #expect(modified.priority == original.priority)
        #expect(modified.retryLimit == original.retryLimit)
    }

    @Test("withDebugIdentifier sets identifier and enables logging")
    func withDebugIdentifier() {
        let config = TransactionConfiguration.default.withDebugIdentifier("test-tx-123")

        #expect(config.debugTransactionIdentifier == "test-tx-123")
        #expect(config.logTransaction == true)
    }

    @Test("withTags adds tags to existing")
    func withTags() {
        let config = TransactionConfiguration(tags: ["existing"])
            .withTags(["new1", "new2"])

        #expect(config.tags == ["existing", "new1", "new2"])
    }

    @Test("withRetryLimit creates copy with new limit")
    func withRetryLimit() {
        let config = TransactionConfiguration.interactive.withRetryLimit(10)

        #expect(config.retryLimit == 10)
        #expect(config.timeout == 1_000) // Preserved from original
    }

    // MARK: - Custom Configuration Tests

    @Test("Custom configuration with all options")
    func customConfiguration() {
        let config = TransactionConfiguration(
            priority: .priorityBatch,
            readPriority: .readPriorityHigh,
            timeout: 5_000,
            retryLimit: 10,
            maxRetryDelay: 1_000,
            useGrvCache: true,
            snapshotRywDisable: true,
            debugTransactionIdentifier: "custom-tx",
            logTransaction: true,
            tags: ["tag1", "tag2"]
        )

        #expect(config.priority == .priorityBatch)
        #expect(config.readPriority == .readPriorityHigh)
        #expect(config.timeout == 5_000)
        #expect(config.retryLimit == 10)
        #expect(config.maxRetryDelay == 1_000)
        #expect(config.useGrvCache == true)
        #expect(config.snapshotRywDisable == true)
        #expect(config.debugTransactionIdentifier == "custom-tx")
        #expect(config.logTransaction == true)
        #expect(config.tags == ["tag1", "tag2"])
    }

    // MARK: - Equatable Tests

    @Test("Equal configurations are equal")
    func equalConfigurations() {
        let config1 = TransactionConfiguration.batch
        let config2 = TransactionConfiguration.batch

        #expect(config1 == config2)
    }

    @Test("Different configurations are not equal")
    func differentConfigurations() {
        let config1 = TransactionConfiguration.batch
        let config2 = TransactionConfiguration.interactive

        #expect(config1 != config2)
    }
}

// MARK: - TransactionTiming Tests

@Suite("TransactionTiming Tests")
struct TransactionTimingTests {

    @Test("Total duration calculated correctly")
    func totalDuration() {
        let start = Date()
        let end = start.addingTimeInterval(2.5)

        let timing = TransactionTiming(
            startTime: start,
            endTime: end,
            succeeded: true
        )

        #expect(abs(timing.totalDuration - 2.5) < 0.001)
    }

    @Test("Timing captures all phases")
    func timingPhases() {
        let start = Date()
        let end = start.addingTimeInterval(1.0)

        let timing = TransactionTiming(
            startTime: start,
            endTime: end,
            getReadVersionDuration: 0.1,
            userCodeDuration: 0.5,
            commitDuration: 0.2,
            retryCount: 2,
            succeeded: true
        )

        #expect(timing.getReadVersionDuration == 0.1)
        #expect(timing.userCodeDuration == 0.5)
        #expect(timing.commitDuration == 0.2)
        #expect(timing.retryCount == 2)
        #expect(timing.succeeded == true)
        #expect(timing.error == nil)
    }

    @Test("Failed timing captures error")
    func failedTiming() {
        let error = NSError(domain: "test", code: 1)
        let timing = TransactionTiming(
            startTime: Date(),
            endTime: Date(),
            succeeded: false,
            error: error
        )

        #expect(timing.succeeded == false)
        #expect(timing.error != nil)
    }
}

// MARK: - TransactionStatistics Tests

@Suite("TransactionStatistics Tests")
struct TransactionStatisticsTests {

    @Test("Initial statistics are zero")
    func initialStatistics() {
        let stats = TransactionStatistics()

        #expect(stats.totalTransactions == 0)
        #expect(stats.successfulTransactions == 0)
        #expect(stats.failedTransactions == 0)
        #expect(stats.totalRetries == 0)
        #expect(stats.averageDurationSeconds == 0)
        #expect(stats.maxDurationSeconds == 0)
        #expect(stats.successRate == 0)
        #expect(stats.averageRetries == 0)
    }

    @Test("Recording successful transaction")
    func recordSuccessful() {
        var stats = TransactionStatistics()

        let timing = TransactionTiming(
            startTime: Date(),
            endTime: Date().addingTimeInterval(0.5),
            retryCount: 1,
            succeeded: true
        )

        stats.record(timing, priority: .priorityBatch)

        #expect(stats.totalTransactions == 1)
        #expect(stats.successfulTransactions == 1)
        #expect(stats.failedTransactions == 0)
        #expect(stats.totalRetries == 1)
        #expect(stats.transactionsByPriority[.priorityBatch] == 1)
        #expect(stats.successRate == 1.0)
    }

    @Test("Recording failed transaction")
    func recordFailed() {
        var stats = TransactionStatistics()

        let timing = TransactionTiming(
            startTime: Date(),
            endTime: Date().addingTimeInterval(0.1),
            retryCount: 3,
            succeeded: false,
            error: NSError(domain: "test", code: 1)
        )

        stats.record(timing, priority: nil)

        #expect(stats.totalTransactions == 1)
        #expect(stats.successfulTransactions == 0)
        #expect(stats.failedTransactions == 1)
        #expect(stats.totalRetries == 3)
        #expect(stats.successRate == 0)
    }

    @Test("Average duration calculated correctly")
    func averageDuration() {
        var stats = TransactionStatistics()

        // Record 3 transactions: 1s, 2s, 3s -> average = 2s
        for duration in [1.0, 2.0, 3.0] {
            let timing = TransactionTiming(
                startTime: Date(),
                endTime: Date().addingTimeInterval(duration),
                succeeded: true
            )
            stats.record(timing, priority: nil)
        }

        #expect(abs(stats.averageDurationSeconds - 2.0) < 0.001)
        #expect(stats.maxDurationSeconds == 3.0)
    }

    @Test("Success rate calculation")
    func successRateCalculation() {
        var stats = TransactionStatistics()

        // 3 successes, 1 failure -> 75% success rate
        for i in 0..<4 {
            let timing = TransactionTiming(
                startTime: Date(),
                endTime: Date(),
                succeeded: i < 3
            )
            stats.record(timing, priority: nil)
        }

        #expect(abs(stats.successRate - 0.75) < 0.001)
    }
}

// MARK: - PriorityRateLimiter Tests

@Suite("PriorityRateLimiter Tests")
struct PriorityRateLimiterTests {

    @Test("Initial tokens are at maximum")
    func initialTokens() {
        let limiter = PriorityRateLimiter(
            maxBatchTokens: 10,
            maxDefaultTokens: 100,
            maxSystemTokens: 1000
        )

        let counts = limiter.tokenCounts
        #expect(counts.batch == 10)
        #expect(counts.default == 100)
        #expect(counts.system == 1000)
    }

    @Test("Acquiring batch token decrements batch tokens")
    func acquireBatchToken() {
        let limiter = PriorityRateLimiter(maxBatchTokens: 10)

        let acquired = limiter.tryAcquire(priority: .priorityBatch)

        #expect(acquired == true)
        #expect(limiter.tokenCounts.batch == 9)
    }

    @Test("Acquiring system token decrements system tokens")
    func acquireSystemToken() {
        let limiter = PriorityRateLimiter(maxSystemTokens: 100)

        let acquired = limiter.tryAcquire(priority: .prioritySystemImmediate)

        #expect(acquired == true)
        #expect(limiter.tokenCounts.system == 99)
    }

    @Test("Acquiring default token decrements default tokens")
    func acquireDefaultToken() {
        let limiter = PriorityRateLimiter(maxDefaultTokens: 50)

        let acquired = limiter.tryAcquire(priority: nil)

        #expect(acquired == true)
        #expect(limiter.tokenCounts.default == 49)
    }

    @Test("Cannot acquire when tokens exhausted")
    func exhaustedTokens() {
        let limiter = PriorityRateLimiter(maxBatchTokens: 2)

        _ = limiter.tryAcquire(priority: .priorityBatch)
        _ = limiter.tryAcquire(priority: .priorityBatch)
        let thirdAcquire = limiter.tryAcquire(priority: .priorityBatch)

        #expect(thirdAcquire == false)
        #expect(limiter.tokenCounts.batch == 0)
    }

    @Test("Tokens are independent per priority")
    func independentTokens() {
        let limiter = PriorityRateLimiter(
            maxBatchTokens: 1,
            maxDefaultTokens: 1,
            maxSystemTokens: 1
        )

        // Exhaust batch tokens
        _ = limiter.tryAcquire(priority: .priorityBatch)
        let batchFailed = limiter.tryAcquire(priority: .priorityBatch)

        // Default and system should still work
        let defaultOk = limiter.tryAcquire(priority: nil)
        let systemOk = limiter.tryAcquire(priority: .prioritySystemImmediate)

        #expect(batchFailed == false)
        #expect(defaultOk == true)
        #expect(systemOk == true)
    }
}
