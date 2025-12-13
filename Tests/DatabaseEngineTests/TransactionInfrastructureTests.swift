// TransactionInfrastructureTests.swift
// DatabaseEngine - Tests for transaction infrastructure features
//
// **Coverage**:
// - CommitCheck protocol and registry
// - PostCommit protocol and registry
// - TransactionListener and lifecycle tracking
// - TransactionConfiguration (ID, logging, tags)

import Testing
import Foundation
import FoundationDB
import Synchronization
@testable import DatabaseEngine
@testable import Core

// MARK: - Thread-safe Test Helpers

/// Thread-safe counter for testing
final class AtomicCounter: Sendable {
    private let value: Mutex<Int>

    init(_ initial: Int = 0) {
        self.value = Mutex(initial)
    }

    func increment() {
        value.withLock { $0 += 1 }
    }

    var current: Int {
        value.withLock { $0 }
    }
}

/// Thread-safe boolean for testing
final class AtomicBool: Sendable {
    private let value: Mutex<Bool>

    init(_ initial: Bool = false) {
        self.value = Mutex(initial)
    }

    func set(_ newValue: Bool) {
        value.withLock { $0 = newValue }
    }

    var current: Bool {
        value.withLock { $0 }
    }
}

/// Thread-safe array for testing
final class AtomicArray<T: Sendable>: Sendable {
    private let storage: Mutex<[T]>

    init(_ initial: [T] = []) {
        self.storage = Mutex(initial)
    }

    func append(_ element: T) {
        storage.withLock { $0.append(element) }
    }

    var current: [T] {
        storage.withLock { Array($0) }
    }

    var count: Int {
        storage.withLock { $0.count }
    }
}

// MARK: - CommitCheck Tests

@Suite("CommitCheck Tests", .serialized)
struct CommitCheckTests {

    // MARK: - CommitCheckRegistry Basic Tests

    @Test("CommitCheckRegistry starts empty")
    func registryStartsEmpty() {
        let registry = CommitCheckRegistry()
        #expect(registry.count == 0)
    }

    @Test("CommitCheckRegistry.add increases count")
    func addIncreasesCount() {
        let registry = CommitCheckRegistry()
        registry.add(PassingCommitCheck(), name: "test1")
        #expect(registry.count == 1)

        registry.add(PassingCommitCheck(), name: "test2")
        #expect(registry.count == 2)
    }

    @Test("CommitCheckRegistry.clear removes all checks")
    func clearRemovesAllChecks() {
        let registry = CommitCheckRegistry()
        registry.add(PassingCommitCheck(), name: "test1")
        registry.add(PassingCommitCheck(), name: "test2")
        #expect(registry.count == 2)

        registry.clear()
        #expect(registry.count == 0)
    }

    @Test("CommitCheckRegistry.add with closure works")
    func addWithClosureWorks() async throws {
        let registry = CommitCheckRegistry()
        let executed = AtomicBool(false)

        registry.add(name: "closure-check") { _ in
            executed.set(true)
        }

        #expect(registry.count == 1)

        // Execute to verify closure works
        try await FDBTestEnvironment.shared.ensureInitialized()
        let database = try FDBClient.openDatabase()
        try await database.withTransaction { tx in
            try await registry.executeAll(transaction: tx)
        }
        #expect(executed.current == true)
    }

    // MARK: - CommitCheck Execution Tests

    @Test("CommitCheckRegistry.executeAll runs all passing checks")
    func executeAllRunsPassingChecks() async throws {
        try await FDBTestEnvironment.shared.ensureInitialized()
        let database = try FDBClient.openDatabase()

        let registry = CommitCheckRegistry()
        let executionOrder = AtomicArray<String>()

        registry.add(name: "check1", priority: 100) { _ in
            executionOrder.append("check1")
        }
        registry.add(name: "check2", priority: 50) { _ in
            executionOrder.append("check2")
        }
        registry.add(name: "check3", priority: 200) { _ in
            executionOrder.append("check3")
        }

        try await database.withTransaction { tx in
            try await registry.executeAll(transaction: tx)
        }

        // Should execute in priority order (lower first)
        #expect(executionOrder.current == ["check2", "check1", "check3"])
    }

    @Test("CommitCheckRegistry.executeAll throws on first failure")
    func executeAllThrowsOnFailure() async throws {
        try await FDBTestEnvironment.shared.ensureInitialized()
        let database = try FDBClient.openDatabase()

        let registry = CommitCheckRegistry()
        let executionOrder = AtomicArray<String>()

        registry.add(name: "check1", priority: 100) { _ in
            executionOrder.append("check1")
        }
        registry.add(name: "check2-fails", priority: 200) { _ in
            executionOrder.append("check2")
            throw TestCommitCheckError.validationFailed("test failure")
        }
        registry.add(name: "check3-should-not-run", priority: 300) { _ in
            executionOrder.append("check3")
        }

        do {
            try await database.withTransaction { tx in
                try await registry.executeAll(transaction: tx)
            }
            Issue.record("Expected error to be thrown")
        } catch {
            // Check that check3 was not executed
            #expect(executionOrder.current == ["check1", "check2"])
        }
    }

    // MARK: - CompositeCommitCheck Tests

    @Test("CompositeCommitCheck failFast=true stops on first failure")
    func compositeFailFastStopsOnFirstFailure() async throws {
        try await FDBTestEnvironment.shared.ensureInitialized()
        let database = try FDBClient.openDatabase()

        let executed = AtomicArray<Int>()

        let composite = CompositeCommitCheck(checks: [
            TrackingCommitCheck(id: 1, tracker: executed),
            FailingCommitCheck(id: 2, tracker: executed),
            TrackingCommitCheck(id: 3, tracker: executed)
        ], failFast: true)

        do {
            try await database.withTransaction { tx in
                try await composite.check(transaction: tx)
            }
            Issue.record("Expected error")
        } catch {
            #expect(executed.current == [1, 2])
        }
    }

    @Test("CompositeCommitCheck failFast=false collects all failures")
    func compositeNoFailFastCollectsAllFailures() async throws {
        try await FDBTestEnvironment.shared.ensureInitialized()
        let database = try FDBClient.openDatabase()

        let executed = AtomicArray<Int>()

        let composite = CompositeCommitCheck(checks: [
            FailingCommitCheck(id: 1, tracker: executed),
            TrackingCommitCheck(id: 2, tracker: executed),
            FailingCommitCheck(id: 3, tracker: executed)
        ], failFast: false)

        do {
            try await database.withTransaction { tx in
                try await composite.check(transaction: tx)
            }
            Issue.record("Expected error")
        } catch let error as CommitCheckError {
            // All checks should have executed
            #expect(executed.current == [1, 2, 3])

            // Error should be multipleFailures
            if case .multipleFailures(let failures) = error {
                #expect(failures.count == 2)
            } else {
                Issue.record("Expected multipleFailures error")
            }
        }
    }

    // MARK: - ConditionalCommitCheck Tests

    @Test("ConditionalCommitCheck executes when condition is true")
    func conditionalExecutesWhenTrue() async throws {
        try await FDBTestEnvironment.shared.ensureInitialized()
        let database = try FDBClient.openDatabase()

        let innerExecuted = AtomicBool(false)
        let inner = SettingCommitCheck(flag: innerExecuted)
        let conditional = ConditionalCommitCheck(if: { true }, then: inner)

        try await database.withTransaction { tx in
            try await conditional.check(transaction: tx)
        }

        #expect(innerExecuted.current == true)
    }

    @Test("ConditionalCommitCheck skips when condition is false")
    func conditionalSkipsWhenFalse() async throws {
        try await FDBTestEnvironment.shared.ensureInitialized()
        let database = try FDBClient.openDatabase()

        let innerExecuted = AtomicBool(false)
        let inner = SettingCommitCheck(flag: innerExecuted)
        let conditional = ConditionalCommitCheck(if: { false }, then: inner)

        try await database.withTransaction { tx in
            try await conditional.check(transaction: tx)
        }

        #expect(innerExecuted.current == false)
    }

    // MARK: - CommitCheckError Tests

    @Test("CommitCheckError.validationFailed has correct description")
    func validationFailedDescription() {
        let error = CommitCheckError.validationFailed(checkName: "email-unique", reason: "duplicate email")
        #expect(error.description.contains("email-unique"))
        #expect(error.description.contains("duplicate email"))
    }

    @Test("CommitCheckError.multipleFailures has correct description")
    func multipleFailuresDescription() {
        let error = CommitCheckError.multipleFailures(failures: [
            ("check1", "reason1"),
            ("check2", "reason2")
        ])
        #expect(error.description.contains("check1"))
        #expect(error.description.contains("check2"))
    }
}

// MARK: - PostCommit Tests

@Suite("PostCommit Tests", .serialized)
struct PostCommitTests {

    // MARK: - PostCommitRegistry Basic Tests

    @Test("PostCommitRegistry starts empty")
    func registryStartsEmpty() {
        let registry = PostCommitRegistry()
        #expect(registry.count == 0)
    }

    @Test("PostCommitRegistry.add increases count")
    func addIncreasesCount() {
        let registry = PostCommitRegistry()
        registry.add(NoOpPostCommit(), name: "test1")
        #expect(registry.count == 1)

        registry.add(NoOpPostCommit(), name: "test2")
        #expect(registry.count == 2)
    }

    @Test("PostCommitRegistry.clear removes all hooks")
    func clearRemovesAllHooks() {
        let registry = PostCommitRegistry()
        registry.add(NoOpPostCommit(), name: "test1")
        registry.add(NoOpPostCommit(), name: "test2")
        #expect(registry.count == 2)

        registry.clear()
        #expect(registry.count == 0)
    }

    // MARK: - PostCommit Execution Tests

    @Test("PostCommitRegistry.executeAll runs hooks in priority order")
    func executeAllRunsInPriorityOrder() async {
        let registry = PostCommitRegistry()
        let executionOrder = AtomicArray<String>()

        registry.add(name: "hook1", priority: 100, runConcurrently: false) {
            executionOrder.append("hook1")
        }
        registry.add(name: "hook2", priority: 50, runConcurrently: false) {
            executionOrder.append("hook2")
        }
        registry.add(name: "hook3", priority: 200, runConcurrently: false) {
            executionOrder.append("hook3")
        }

        let results = await registry.executeAll()

        // Sequential hooks should run in priority order
        #expect(executionOrder.current == ["hook2", "hook1", "hook3"])
        #expect(results.count == 3)
        #expect(results.allSatisfy { $0.success })
    }

    @Test("PostCommitRegistry.executeAll returns results with duration")
    func executeAllReturnsDuration() async {
        let registry = PostCommitRegistry()

        registry.add(name: "slow-hook", priority: 100) {
            try await Task.sleep(nanoseconds: 50_000_000) // 50ms
        }

        let results = await registry.executeAll()

        #expect(results.count == 1)
        #expect(results[0].success == true)
        #expect(results[0].duration >= 0.05) // At least 50ms
    }

    @Test("PostCommitRegistry.executeAll captures failures")
    func executeAllCapturesFailures() async {
        let registry = PostCommitRegistry()

        registry.add(name: "failing-hook") {
            throw TestPostCommitError.intentionalFailure
        }
        registry.add(name: "passing-hook") {
            // Success
        }

        let results = await registry.executeAll()

        let failingResult = results.first { $0.name == "failing-hook" }
        let passingResult = results.first { $0.name == "passing-hook" }

        #expect(failingResult?.success == false)
        #expect(failingResult?.error != nil)
        #expect(passingResult?.success == true)
    }

    @Test("PostCommitRegistry runs concurrent hooks in parallel")
    func concurrentHooksRunInParallel() async {
        let registry = PostCommitRegistry()
        let startTime = Date()

        // Add 3 concurrent hooks that each take 50ms
        for i in 0..<3 {
            registry.add(name: "concurrent-\(i)", priority: 100, runConcurrently: true) {
                try await Task.sleep(nanoseconds: 50_000_000) // 50ms
            }
        }

        _ = await registry.executeAll()
        let totalDuration = Date().timeIntervalSince(startTime)

        // If truly parallel, total time should be ~50ms, not 150ms
        // Allow some slack for scheduling
        #expect(totalDuration < 0.15) // Should complete in less than 150ms
    }

    // MARK: - RetryingPostCommit Tests

    @Test("RetryingPostCommit retries on failure")
    func retryingPostCommitRetries() async throws {
        let attempts = AtomicCounter(0)

        let inner = CountingPostCommit(counter: attempts, failUntil: 3)
        let retrying = RetryingPostCommit(inner, maxAttempts: 5, backoffMs: 10)

        try await retrying.run()

        #expect(attempts.current == 3)
    }

    @Test("RetryingPostCommit gives up after maxAttempts")
    func retryingPostCommitGivesUp() async {
        let attempts = AtomicCounter(0)

        let inner = AlwaysFailingPostCommit(counter: attempts)
        let retrying = RetryingPostCommit(inner, maxAttempts: 3, backoffMs: 5)

        do {
            try await retrying.run()
            Issue.record("Expected error")
        } catch {
            #expect(attempts.current == 3)
        }
    }

    // MARK: - DelayedPostCommit Tests

    @Test("DelayedPostCommit waits before executing")
    func delayedPostCommitWaits() async throws {
        let executed = AtomicBool(false)
        let inner = SettingPostCommit(flag: executed)
        let delayed = DelayedPostCommit(inner, delay: 0.05) // 50ms

        let startTime = Date()
        try await delayed.run()
        let duration = Date().timeIntervalSince(startTime)

        #expect(executed.current == true)
        #expect(duration >= 0.05)
    }

    // MARK: - FireAndForgetPostCommit Tests

    @Test("FireAndForgetPostCommit ignores errors")
    func fireAndForgetIgnoresErrors() async throws {
        let failing = AlwaysFailingPostCommit(counter: AtomicCounter())
        let fireAndForget = FireAndForgetPostCommit(failing)

        // Should not throw
        try await fireAndForget.run()
    }

    // MARK: - CompositePostCommit Tests

    @Test("CompositePostCommit runs all hooks sequentially")
    func compositeRunsSequentially() async throws {
        let order = AtomicArray<Int>()

        let composite = CompositePostCommit(hooks: [
            AppendingPostCommit(array: order, value: 1),
            AppendingPostCommit(array: order, value: 2),
            AppendingPostCommit(array: order, value: 3)
        ], runConcurrently: false)

        try await composite.run()

        #expect(order.current == [1, 2, 3])
    }

    @Test("CompositePostCommit runs hooks concurrently")
    func compositeRunsConcurrently() async throws {
        let startTime = Date()

        let composite = CompositePostCommit(hooks: [
            SleepingPostCommit(milliseconds: 50),
            SleepingPostCommit(milliseconds: 50),
            SleepingPostCommit(milliseconds: 50)
        ], runConcurrently: true)

        try await composite.run()
        let duration = Date().timeIntervalSince(startTime)

        // Should complete in ~50ms if parallel, not 150ms
        #expect(duration < 0.15)
    }
}

// MARK: - TransactionListener Tests

@Suite("TransactionListener Tests", .serialized)
struct TransactionListenerTests {

    // MARK: - TransactionListenerRegistry Basic Tests

    @Test("TransactionListenerRegistry starts empty")
    func registryStartsEmpty() {
        let registry = TransactionListenerRegistry()
        #expect(registry.count == 0)
    }

    @Test("TransactionListenerRegistry.add increases count")
    func addIncreasesCount() {
        let registry = TransactionListenerRegistry()
        registry.add(CollectingTransactionListener())
        #expect(registry.count == 1)

        registry.add(CollectingTransactionListener())
        #expect(registry.count == 2)
    }

    @Test("TransactionListenerRegistry.clear removes all listeners")
    func clearRemovesAllListeners() {
        let registry = TransactionListenerRegistry()
        registry.add(CollectingTransactionListener())
        registry.add(CollectingTransactionListener())
        #expect(registry.count == 2)

        registry.clear()
        #expect(registry.count == 0)
    }

    @Test("TransactionListenerRegistry.add with closure works")
    func addWithClosureWorks() {
        let registry = TransactionListenerRegistry()
        let events = AtomicArray<TransactionEvent>()

        registry.add { event in
            events.append(event)
        }

        registry.notify(.created(id: "test-123", timestamp: Date()))

        #expect(events.count == 1)
        if case .created(let id, _) = events.current[0] {
            #expect(id == "test-123")
        } else {
            Issue.record("Expected created event")
        }
    }

    // MARK: - TransactionEvent Tests

    @Test("TransactionEvent.transactionID returns correct ID")
    func eventTransactionIdReturnsCorrectId() {
        let events: [TransactionEvent] = [
            .created(id: "id1", timestamp: Date()),
            .committing(id: "id2", timestamp: Date()),
            .committed(id: "id3", timestamp: Date(), duration: 0.1, version: 123),
            .failed(id: "id4", timestamp: Date(), duration: 0.1, error: TransactionTestError.test),
            .cancelled(id: "id5", timestamp: Date(), duration: 0.1),
            .closed(id: "id6", timestamp: Date(), totalDuration: 0.1)
        ]

        let expectedIds = ["id1", "id2", "id3", "id4", "id5", "id6"]

        for (event, expectedId) in zip(events, expectedIds) {
            #expect(event.transactionID == expectedId)
        }
    }

    @Test("TransactionEvent.timestamp returns correct timestamp")
    func eventTimestampReturnsCorrectTimestamp() {
        let now = Date()
        let event = TransactionEvent.created(id: "test", timestamp: now)
        #expect(event.timestamp == now)
    }

    @Test("TransactionEvent.description contains transaction ID")
    func eventDescriptionContainsId() {
        let event = TransactionEvent.committed(
            id: "tx-12345",
            timestamp: Date(),
            duration: 0.123,
            version: 456
        )
        #expect(event.description.contains("tx-12345"))
        #expect(event.description.contains("committed"))
    }

    // MARK: - TransactionLifecycleTracker Tests

    @Test("TransactionLifecycleTracker emits created event on init")
    func trackerEmitsCreatedEvent() {
        let registry = TransactionListenerRegistry()
        let events = AtomicArray<TransactionEvent>()

        registry.add { event in
            events.append(event)
        }

        _ = TransactionLifecycleTracker(id: "test-tx", registry: registry)

        #expect(events.count == 1)
        if case .created(let id, _) = events.current[0] {
            #expect(id == "test-tx")
        } else {
            Issue.record("Expected created event")
        }
    }

    @Test("TransactionLifecycleTracker.markCommitting emits committing event")
    func trackerEmitsCommittingEvent() {
        let registry = TransactionListenerRegistry()
        let events = AtomicArray<TransactionEvent>()

        registry.add { event in
            events.append(event)
        }

        let tracker = TransactionLifecycleTracker(id: "test-tx", registry: registry)
        tracker.markCommitting()

        #expect(events.count == 2)
        if case .committing(let id, _) = events.current[1] {
            #expect(id == "test-tx")
        } else {
            Issue.record("Expected committing event")
        }
    }

    @Test("TransactionLifecycleTracker.markCommitted emits committed event")
    func trackerEmitsCommittedEvent() {
        let registry = TransactionListenerRegistry()
        let events = AtomicArray<TransactionEvent>()

        registry.add { event in
            events.append(event)
        }

        let tracker = TransactionLifecycleTracker(id: "test-tx", registry: registry)
        tracker.markCommitted(version: 12345)

        #expect(events.count == 2)
        if case .committed(let id, _, _, let version) = events.current[1] {
            #expect(id == "test-tx")
            #expect(version == 12345)
        } else {
            Issue.record("Expected committed event")
        }
    }

    @Test("TransactionLifecycleTracker prevents duplicate terminal events")
    func trackerPreventsDuplicateTerminalEvents() {
        let registry = TransactionListenerRegistry()
        let events = AtomicArray<TransactionEvent>()

        registry.add { event in
            events.append(event)
        }

        let tracker = TransactionLifecycleTracker(id: "test-tx", registry: registry)

        // First commit
        tracker.markCommitted(version: 123)

        // These should be ignored (already committed)
        tracker.markFailed(error: TransactionTestError.test)
        tracker.markCancelled()
        tracker.markCommitted(version: 456)

        // Should only have created + committed
        #expect(events.count == 2)
    }

    @Test("TransactionLifecycleTracker.markClosed can be called after commit")
    func trackerClosedAfterCommit() {
        let registry = TransactionListenerRegistry()
        let events = AtomicArray<TransactionEvent>()

        registry.add { event in
            events.append(event)
        }

        let tracker = TransactionLifecycleTracker(id: "test-tx", registry: registry)
        tracker.markCommitted(version: 123)
        tracker.markClosed()

        #expect(events.count == 3)
        if case .closed(let id, _, _) = events.current[2] {
            #expect(id == "test-tx")
        } else {
            Issue.record("Expected closed event")
        }
    }

    @Test("TransactionLifecycleTracker.elapsed returns positive duration")
    func trackerElapsedReturnsPositive() async throws {
        let tracker = TransactionLifecycleTracker(id: "test", registry: nil)
        try await Task.sleep(nanoseconds: 10_000_000) // 10ms

        #expect(tracker.elapsed >= 0.01)
    }

    // MARK: - MetricsTransactionListener Tests

    @Test("MetricsTransactionListener tracks total transactions")
    func metricsTracksTotal() {
        let listener = MetricsTransactionListener()

        listener.onEvent(.created(id: "tx1", timestamp: Date()))
        listener.onEvent(.created(id: "tx2", timestamp: Date()))
        listener.onEvent(.created(id: "tx3", timestamp: Date()))

        #expect(listener.metrics.totalTransactions == 3)
    }

    @Test("MetricsTransactionListener tracks committed transactions")
    func metricsTracksCommitted() {
        let listener = MetricsTransactionListener()

        listener.onEvent(.created(id: "tx1", timestamp: Date()))
        listener.onEvent(.committed(id: "tx1", timestamp: Date(), duration: 0.05, version: 123))

        listener.onEvent(.created(id: "tx2", timestamp: Date()))
        listener.onEvent(.committed(id: "tx2", timestamp: Date(), duration: 0.1, version: 456))

        #expect(listener.metrics.committedTransactions == 2)
        #expect(listener.metrics.avgDurationMs >= 75) // Average of 50ms and 100ms
    }

    @Test("MetricsTransactionListener tracks failed transactions")
    func metricsTracksFailed() {
        let listener = MetricsTransactionListener()

        listener.onEvent(.created(id: "tx1", timestamp: Date()))
        listener.onEvent(.failed(id: "tx1", timestamp: Date(), duration: 0.1, error: TransactionTestError.test))

        #expect(listener.metrics.failedTransactions == 1)
    }

    @Test("MetricsTransactionListener tracks cancelled transactions")
    func metricsTracksCancelled() {
        let listener = MetricsTransactionListener()

        listener.onEvent(.created(id: "tx1", timestamp: Date()))
        listener.onEvent(.cancelled(id: "tx1", timestamp: Date(), duration: 0.1))

        #expect(listener.metrics.cancelledTransactions == 1)
    }

    @Test("MetricsTransactionListener.successRate calculates correctly")
    func metricsSuccessRate() {
        let listener = MetricsTransactionListener()

        // 2 committed, 1 failed, 1 cancelled = 50% success
        listener.onEvent(.created(id: nil, timestamp: Date()))
        listener.onEvent(.committed(id: nil, timestamp: Date(), duration: 0.1, version: nil))
        listener.onEvent(.created(id: nil, timestamp: Date()))
        listener.onEvent(.committed(id: nil, timestamp: Date(), duration: 0.1, version: nil))
        listener.onEvent(.created(id: nil, timestamp: Date()))
        listener.onEvent(.failed(id: nil, timestamp: Date(), duration: 0.1, error: TransactionTestError.test))
        listener.onEvent(.created(id: nil, timestamp: Date()))
        listener.onEvent(.cancelled(id: nil, timestamp: Date(), duration: 0.1))

        #expect(listener.metrics.totalTransactions == 4)
        #expect(listener.metrics.successRate == 0.5)
    }

    @Test("MetricsTransactionListener.reset clears all metrics")
    func metricsReset() {
        let listener = MetricsTransactionListener()

        listener.onEvent(.created(id: nil, timestamp: Date()))
        listener.onEvent(.committed(id: nil, timestamp: Date(), duration: 0.1, version: nil))

        #expect(listener.metrics.totalTransactions == 1)

        listener.reset()

        #expect(listener.metrics.totalTransactions == 0)
        #expect(listener.metrics.committedTransactions == 0)
    }

    // MARK: - FilteringTransactionListener Tests

    @Test("FilteringTransactionListener only forwards matching events")
    func filteringOnlyForwardsMatching() {
        let events = AtomicArray<TransactionEvent>()
        let inner = ClosureTransactionListener { event in
            events.append(event)
        }

        // Only forward committed events
        let filtering = FilteringTransactionListener(inner) { event in
            if case .committed = event { return true }
            return false
        }

        filtering.onEvent(.created(id: nil, timestamp: Date()))
        filtering.onEvent(.committed(id: nil, timestamp: Date(), duration: 0.1, version: nil))
        filtering.onEvent(.failed(id: nil, timestamp: Date(), duration: 0.1, error: TransactionTestError.test))
        filtering.onEvent(.committed(id: nil, timestamp: Date(), duration: 0.2, version: nil))

        #expect(events.count == 2)
    }

    // MARK: - LoggingTransactionListener Tests

    @Test("LoggingTransactionListener uses provided logger")
    func loggingUsesProvidedLogger() {
        let mockLogger = TestLogger()
        let listener = LoggingTransactionListener(logger: mockLogger, level: .info)

        listener.onEvent(.created(id: "test", timestamp: Date()))

        #expect(mockLogger.infoMessages.count == 1)
        #expect(mockLogger.infoMessages.current[0].contains("test"))
    }
}

// MARK: - TransactionConfiguration Tests

@Suite("TransactionConfiguration Extended Tests", .serialized)
struct TransactionConfigurationExtendedTests {

    // MARK: - Tracing Tests

    @Test("TransactionConfiguration.Tracing stores transactionID")
    func tracingStoresTransactionID() {
        let tracing = TransactionConfiguration.Tracing(transactionID: "user-request-12345")
        #expect(tracing.transactionID == "user-request-12345")
    }

    @Test("TransactionConfiguration with tracing has transactionID via convenience accessor")
    func configWithTracingHasTransactionID() {
        let config = TransactionConfiguration(
            tracing: .init(transactionID: "user-request-12345")
        )
        #expect(config.transactionID == "user-request-12345")
        #expect(config.tracing.transactionID == "user-request-12345")
    }

    @Test("TransactionConfiguration.default has nil transactionID")
    func defaultHasNilTransactionID() {
        let config = TransactionConfiguration.default
        #expect(config.transactionID == nil)
        #expect(config.tracing == .disabled)
    }

    // MARK: - Logging Tests

    @Test("TransactionConfiguration.Tracing stores logTransaction")
    func tracingStoresLogTransaction() {
        let tracing = TransactionConfiguration.Tracing(logTransaction: true)
        #expect(tracing.logTransaction == true)
    }

    @Test("TransactionConfiguration.default has logTransaction=false")
    func defaultHasLogTransactionFalse() {
        let config = TransactionConfiguration.default
        #expect(config.logTransaction == false)
    }

    @Test("TransactionConfiguration.Tracing stores serverRequestTracing")
    func tracingStoresServerRequestTracing() {
        let tracing = TransactionConfiguration.Tracing(serverRequestTracing: true)
        #expect(tracing.serverRequestTracing == true)
    }

    // MARK: - Tags Tests

    @Test("TransactionConfiguration.Tracing stores tags")
    func tracingStoresTags() {
        let tracing = TransactionConfiguration.Tracing(tags: ["api", "user-service", "v2"])
        #expect(tracing.tags.count == 3)
        #expect(tracing.tags.contains("api"))
        #expect(tracing.tags.contains("user-service"))
        #expect(tracing.tags.contains("v2"))
    }

    @Test("TransactionConfiguration.default has empty tags")
    func defaultHasEmptyTags() {
        let config = TransactionConfiguration.default
        #expect(config.tags.isEmpty)
    }

    // MARK: - Combined Configuration Tests

    @Test("TransactionConfiguration with full tracing options")
    func fullTracingOptions() {
        let config = TransactionConfiguration(
            tracing: .init(
                transactionID: "tx-12345",
                logTransaction: true,
                serverRequestTracing: true,
                tags: ["debug", "performance"]
            )
        )

        // Via convenience accessors
        #expect(config.transactionID == "tx-12345")
        #expect(config.logTransaction == true)
        #expect(config.serverRequestTracing == true)
        #expect(config.tags == ["debug", "performance"])

        // Via tracing property
        #expect(config.tracing.transactionID == "tx-12345")
        #expect(config.tracing.logTransaction == true)
        #expect(config.tracing.serverRequestTracing == true)
        #expect(config.tracing.tags == ["debug", "performance"])
    }

    // MARK: - Tracing.disabled Tests

    @Test("Tracing.disabled has all defaults")
    func tracingDisabledHasDefaults() {
        let tracing = TransactionConfiguration.Tracing.disabled
        #expect(tracing.transactionID == nil)
        #expect(tracing.logTransaction == false)
        #expect(tracing.serverRequestTracing == false)
        #expect(tracing.tags.isEmpty)
    }

    // MARK: - Preset Configurations Tests

    @Test("All presets have tracing disabled by default")
    func presetsHaveTracingDisabled() {
        let presets: [TransactionConfiguration] = [
            .default,
            .batch,
            .system,
            .interactive,
            .longRunning
        ]

        for preset in presets {
            // All presets should have tracing disabled
            #expect(preset.tracing == .disabled)
            // Convenience accessors should reflect disabled state
            #expect(preset.transactionID == nil)
            #expect(preset.logTransaction == false)
            #expect(preset.serverRequestTracing == false)
            #expect(preset.tags.isEmpty)
        }
    }

    // MARK: - Init Parameter Count Test

    @Test("TransactionConfiguration init has reasonable parameter count")
    func initHasReasonableParameterCount() {
        // The init now has 8 parameters instead of 11
        // Core: timeout, retryLimit, maxRetryDelay, priority, readPriority, disableReadCache, weakReadSemantics
        // Grouped: tracing

        let config = TransactionConfiguration(
            timeout: 5000,
            retryLimit: 3,
            maxRetryDelay: 500,
            priority: .batch,
            readPriority: .low,
            disableReadCache: true,
            weakReadSemantics: .relaxed,
            tracing: .init(transactionID: "test")
        )

        #expect(config.timeout == 5000)
        #expect(config.retryLimit == 3)
        #expect(config.maxRetryDelay == 500)
        #expect(config.priority == .batch)
        #expect(config.readPriority == .low)
        #expect(config.disableReadCache == true)
        #expect(config.weakReadSemantics == .relaxed)
        #expect(config.transactionID == "test")
    }
}

// MARK: - Test Helpers (Sendable-compliant)

struct PassingCommitCheck: CommitCheck {
    func check(transaction: any TransactionProtocol) async throws {
        // Always passes
    }
}

struct TrackingCommitCheck: CommitCheck {
    let id: Int
    let tracker: AtomicArray<Int>

    func check(transaction: any TransactionProtocol) async throws {
        tracker.append(id)
    }
}

struct FailingCommitCheck: CommitCheck {
    let id: Int
    let tracker: AtomicArray<Int>

    func check(transaction: any TransactionProtocol) async throws {
        tracker.append(id)
        throw TestCommitCheckError.validationFailed("test")
    }
}

struct SettingCommitCheck: CommitCheck {
    let flag: AtomicBool

    func check(transaction: any TransactionProtocol) async throws {
        flag.set(true)
    }
}

enum TestCommitCheckError: Error {
    case validationFailed(String)
}

struct NoOpPostCommit: PostCommit {
    func run() async throws {
        // No-op
    }
}

struct SettingPostCommit: PostCommit {
    let flag: AtomicBool

    func run() async throws {
        flag.set(true)
    }
}

struct CountingPostCommit: PostCommit {
    let counter: AtomicCounter
    let failUntil: Int

    func run() async throws {
        counter.increment()
        if counter.current < failUntil {
            throw TestPostCommitError.intentionalFailure
        }
    }
}

struct AlwaysFailingPostCommit: PostCommit {
    let counter: AtomicCounter

    func run() async throws {
        counter.increment()
        throw TestPostCommitError.intentionalFailure
    }
}

struct AppendingPostCommit: PostCommit {
    let array: AtomicArray<Int>
    let value: Int

    func run() async throws {
        array.append(value)
    }
}

struct SleepingPostCommit: PostCommit {
    let milliseconds: UInt64

    func run() async throws {
        try await Task.sleep(nanoseconds: milliseconds * 1_000_000)
    }
}

enum TestPostCommitError: Error {
    case intentionalFailure
}

final class CollectingTransactionListener: TransactionListener {
    private let events: Mutex<[TransactionEvent]>

    init() {
        self.events = Mutex([])
    }

    func onEvent(_ event: TransactionEvent) {
        events.withLock { $0.append(event) }
    }

    var collectedEvents: [TransactionEvent] {
        events.withLock { $0 }
    }
}

enum TransactionTestError: Error {
    case test
}

final class TestLogger: LoggerProtocol, Sendable {
    let debugMessages = AtomicArray<String>()
    let infoMessages = AtomicArray<String>()
    let warningMessages = AtomicArray<String>()
    let errorMessages = AtomicArray<String>()

    func debug(_ message: String) {
        debugMessages.append(message)
    }

    func info(_ message: String) {
        infoMessages.append(message)
    }

    func warning(_ message: String) {
        warningMessages.append(message)
    }

    func error(_ message: String) {
        errorMessages.append(message)
    }
}
