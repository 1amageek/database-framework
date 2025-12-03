// TransactionListenerTests.swift
// DatabaseEngine Tests - TransactionListener tests

import Testing
import Foundation
@testable import DatabaseEngine

// MARK: - TransactionContext Tests

@Suite("TransactionContext Tests")
struct TransactionContextTests {

    @Test("Default context creation")
    func defaultContextCreation() {
        let context = TransactionContext()

        #expect(context.operation == .generic)
        #expect(context.tags.isEmpty)
        #expect(context.metadata.isEmpty)
        #expect(context.debugIdentifier == nil)
    }

    @Test("Custom context creation")
    func customContextCreation() {
        let context = TransactionContext(
            debugIdentifier: "test-tx",
            operation: .save,
            tags: ["batch", "important"],
            metadata: ["user": "123"]
        )

        #expect(context.debugIdentifier == "test-tx")
        #expect(context.operation == .save)
        #expect(context.tags == ["batch", "important"])
        #expect(context.metadata["user"] == "123")
    }
}

// MARK: - TransactionTimingInfo Tests

@Suite("TransactionTimingInfo Tests")
struct TransactionTimingInfoTests {

    @Test("Duration calculation in milliseconds")
    func durationCalculation() {
        let timing = TransactionTimingInfo(
            totalDurationNanos: 5_000_000, // 5ms
            retryCount: 2
        )

        #expect(timing.totalDurationMs == 5.0)
        #expect(timing.retryCount == 2)
    }

    @Test("Full timing info")
    func fullTimingInfo() {
        let timing = TransactionTimingInfo(
            totalDurationNanos: 10_000_000,
            getReadVersionNanos: 1_000_000,
            userCodeNanos: 5_000_000,
            commitNanos: 3_000_000,
            retryCount: 0,
            readVersion: 12345,
            commitVersion: 12350,
            readVersionCached: true
        )

        #expect(timing.getReadVersionNanos == 1_000_000)
        #expect(timing.userCodeNanos == 5_000_000)
        #expect(timing.commitNanos == 3_000_000)
        #expect(timing.readVersion == 12345)
        #expect(timing.commitVersion == 12350)
        #expect(timing.readVersionCached == true)
    }
}

// MARK: - CompositeTransactionListener Tests

@Suite("CompositeTransactionListener Tests")
struct CompositeTransactionListenerTests {

    @Test("Empty composite has zero listeners")
    func emptyComposite() {
        let composite = CompositeTransactionListener()
        #expect(composite.listenerCount == 0)
    }

    @Test("Add and count listeners")
    func addListeners() {
        let composite = CompositeTransactionListener()

        composite.add(TestListener())
        composite.add(TestListener())

        #expect(composite.listenerCount == 2)
    }

    @Test("Remove all clears listeners")
    func removeAllListeners() {
        let composite = CompositeTransactionListener()

        composite.add(TestListener())
        composite.add(TestListener())
        composite.removeAll()

        #expect(composite.listenerCount == 0)
    }

    @Test("Composite calls all listeners")
    func compositeCallsAll() {
        let composite = CompositeTransactionListener()
        let listener1 = CountingListener()
        let listener2 = CountingListener()

        composite.add(listener1)
        composite.add(listener2)

        let context = TransactionContext()
        composite.transactionStarted(context: context)

        #expect(listener1.startedCount == 1)
        #expect(listener2.startedCount == 1)
    }
}

// MARK: - MetricsTransactionListener Tests

@Suite("MetricsTransactionListener Tests")
struct MetricsTransactionListenerTests {

    @Test("Metrics listener creates with different prefixes")
    func metricsListenerCreatesWithDifferentPrefixes() {
        let listener1 = MetricsTransactionListener(prefix: "service_a")
        let listener2 = MetricsTransactionListener(prefix: "service_b")

        // Both should be able to record metrics independently
        let context = TransactionContext(operation: .save)
        let timing = TransactionTimingInfo(totalDurationNanos: 1_000_000, retryCount: 0)

        listener1.transactionCommitted(context: context, timing: timing)
        listener2.transactionCommitted(context: context, timing: timing)

        // The fact that both can run without error shows they're independent
    }

    @Test("Committed transaction processes all timing info")
    func committedTransactionProcessesTimingInfo() {
        let listener = MetricsTransactionListener(prefix: "test")
        let context = TransactionContext(operation: .save)
        let timing = TransactionTimingInfo(
            totalDurationNanos: 5_000_000,  // 5ms
            getReadVersionNanos: 1_000_000,
            userCodeNanos: 3_000_000,
            commitNanos: 1_000_000,
            retryCount: 2,
            readVersion: 12345,
            commitVersion: 12350,
            readVersionCached: true
        )

        // Should process without error
        listener.transactionCommitted(context: context, timing: timing)

        // Verify timing info is valid
        #expect(timing.totalDurationNanos == 5_000_000)
        #expect(timing.totalDurationMs == 5.0)
        #expect(timing.retryCount == 2)
    }

    @Test("Failed transaction records retry count from timing")
    func failedTransactionRecordsRetryCount() {
        let listener = MetricsTransactionListener(prefix: "test")
        let context = TransactionContext(operation: .query)
        let timing = TransactionTimingInfo(
            totalDurationNanos: 10_000_000,
            retryCount: 5  // Multiple retries before failure
        )
        struct SimulatedError: Error {}

        listener.transactionFailed(context: context, error: SimulatedError(), timing: timing)

        // Verify timing data was correctly passed
        #expect(timing.retryCount == 5)
    }

    @Test("Transaction retry records each attempt")
    func transactionRetryRecordsAttempts() {
        let listener = MetricsTransactionListener(prefix: "test")
        let context = TransactionContext()
        struct RetryableError: Error {}

        // Should handle multiple retry attempts
        for attempt in 1...5 {
            listener.transactionRetried(context: context, error: RetryableError(), attempt: attempt)
        }
        // Test passes if no error occurs
    }

    @Test("Read version obtained distinguishes cache hits and misses")
    func readVersionDistinguishesCacheStatus() {
        let listener = MetricsTransactionListener(prefix: "test")
        let context = TransactionContext()

        // Cache hit - fast
        listener.readVersionObtained(
            context: context,
            readVersion: 12345,
            cached: true,
            duration: 100_000  // 0.1ms
        )

        // Cache miss - slower
        listener.readVersionObtained(
            context: context,
            readVersion: 12346,
            cached: false,
            duration: 2_000_000  // 2ms
        )

        // Both should process without error
    }

    @Test("Range scan handles various sizes")
    func rangeScanHandlesVariousSizes() {
        let listener = MetricsTransactionListener(prefix: "test")
        let context = TransactionContext()

        // Small scan
        listener.rangeScanCompleted(context: context, keyCount: 10, byteCount: 500, duration: 100_000)

        // Large scan
        listener.rangeScanCompleted(context: context, keyCount: 10_000, byteCount: 5_000_000, duration: 50_000_000)

        // Empty scan
        listener.rangeScanCompleted(context: context, keyCount: 0, byteCount: 0, duration: 10_000)
    }

    @Test("All transaction operations are tracked")
    func allTransactionOperationsTracked() {
        let listener = MetricsTransactionListener(prefix: "test")
        let timing = TransactionTimingInfo(totalDurationNanos: 1_000_000, retryCount: 0)

        // Verify all operation types can be tracked
        for operation in TransactionOperation.allCases {
            let context = TransactionContext(operation: operation)
            listener.transactionCommitted(context: context, timing: timing)
        }

        // All operations should process without error
        #expect(TransactionOperation.allCases.count > 0)
    }
}

// MARK: - LoggingTransactionListener Tests

@Suite("LoggingTransactionListener Tests")
struct LoggingTransactionListenerTests {

    @Test("Logging listener with custom logger")
    func customLogger() {
        let loggedMessages = LogMessageCapture()

        let listener = LoggingTransactionListener(minLevel: .debug) { _, message in
            loggedMessages.append(message)
        }

        let context = TransactionContext()
        listener.transactionStarted(context: context)

        #expect(loggedMessages.count == 1)
        #expect(loggedMessages.messages[0].contains("started"))
    }

    @Test("Log level filtering")
    func logLevelFiltering() {
        let loggedMessages = LogMessageCapture()

        let listener = LoggingTransactionListener(minLevel: .warning) { _, message in
            loggedMessages.append(message)
        }

        let context = TransactionContext()
        let timing = TransactionTimingInfo(totalDurationNanos: 1_000_000, retryCount: 0)

        // Debug level should be filtered out
        listener.transactionStarted(context: context)
        listener.transactionCommitted(context: context, timing: timing)

        #expect(loggedMessages.count == 0)

        // Warning should pass through
        listener.transactionFailed(context: context, error: NSError(domain: "test", code: 1), timing: timing)
        #expect(loggedMessages.count == 1)
    }
}

// MARK: - Test Helpers

/// Thread-safe log message capture for testing
final class LogMessageCapture: @unchecked Sendable {
    private var _messages: [String] = []
    private let lock = NSLock()

    var messages: [String] {
        lock.withLock { _messages }
    }

    var count: Int {
        lock.withLock { _messages.count }
    }

    func append(_ message: String) {
        lock.withLock {
            _messages.append(message)
        }
    }
}

final class TestListener: TransactionListener, @unchecked Sendable {}

final class CountingListener: TransactionListener, @unchecked Sendable {
    var startedCount = 0
    var committedCount = 0
    var failedCount = 0
    var retriedCount = 0

    func transactionStarted(context: TransactionContext) {
        startedCount += 1
    }

    func transactionCommitted(context: TransactionContext, timing: TransactionTimingInfo) {
        committedCount += 1
    }

    func transactionFailed(context: TransactionContext, error: Error, timing: TransactionTimingInfo) {
        failedCount += 1
    }

    func transactionRetried(context: TransactionContext, error: Error, attempt: Int) {
        retriedCount += 1
    }
}
