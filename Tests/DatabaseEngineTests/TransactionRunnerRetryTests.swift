#if !os(WASI)
import DatabaseTypes
import Testing
import StorageKit
import StorageKitSystemClock
import Synchronization
@testable import DatabaseEngine

@Suite("TransactionRunner Retry Tests", .serialized, .heartbeat)
struct TransactionRunnerRetryTests {
    @Test("Batch configuration reaches the body on a portable backend")
    func batchConfigurationRunsOnInMemoryBackend() async throws {
        let engine = InMemoryEngine()
        let key: ByteString = [0xA0]
        let value: ByteString = [0x01]

        try await StorageTransactionExecutor(engine: engine).withTransaction(
            configuration: .batch,
            clock: SystemStorageClock()
        ) { transaction in
            try transaction.setValue(value, for: key)
        }

        let verification = try engine.createTransaction()
        #expect(try await verification.getValue(for: key) == value)
        try await verification.cancel()
    }

    @Test("Configuration resolution exposes unsupported advisory hints")
    func configurationResolutionExposesUnsupportedHints() async throws {
        let engine = InMemoryEngine()
        let transaction = try engine.createTransaction()

        let resolution = try TransactionConfiguration.batch.apply(to: transaction)

        #expect(!resolution.backendTimeoutApplied)
        #expect(resolution.executionTimeoutMilliseconds == 30_000)
        #expect(
            resolution.unsupportedHints == [
                .schedulingPriority,
                .readPriority,
                .readCacheControl,
            ]
        )
        try await transaction.cancel()
    }

    @Test("Portable timeout cancels once and never dispatches commit")
    func portableTimeoutCancelsOnceWithoutCommit() async throws {
        let engine = RecordingTransactionEngine(commitBehavior: .success)
        let runner = TransactionRunner(transactionExecutor: StorageTransactionExecutor(engine: engine), clock: SystemStorageClock())

        do {
            let _: Void = try await runner.run(
                configuration: TransactionConfiguration(
                    timeout: 10,
                    maximumAttempts: 1
                )
            ) { _ in
                try await Task.sleep(for: .seconds(30))
            }
            Issue.record("Expected portable timeout")
        } catch let error as TransactionExecutionDeadlineExceeded {
            #expect(error.timeoutMilliseconds == 10)
            #expect(error.source == .transactionConfiguration)
        }

        let snapshot = engine.snapshot
        #expect(snapshot.created == 1)
        #expect(snapshot.commits == 0)
        #expect(snapshot.cancellations == 1)
    }

    @Test("Equal operation and cancellation errors remain a cleanup failure")
    func equalCancellationFailureIsNotDiscarded() async throws {
        let failure = StorageError.transactionConflict
        let engine = RecordingTransactionEngine(
            commitBehavior: .success,
            cancellationError: failure
        )
        let runner = TransactionRunner(transactionExecutor: StorageTransactionExecutor(engine: engine), clock: SystemStorageClock())

        do {
            let _: Void = try await runner.run(
                configuration: TransactionConfiguration(maximumAttempts: 1)
            ) { _ in
                throw failure
            }
            Issue.record("Expected transaction cleanup failure")
        } catch let cleanup as StorageTransactionCleanupError {
            #expect((cleanup.operationError as? StorageError) == failure)
            #expect(cleanup.cancellationErrors.count == 1)
            #expect(
                (cleanup.cancellationErrors[0] as? StorageError) == failure
            )
        }

        #expect(engine.snapshot.cancellations == 1)
    }

    @Test("Cancellation preserves a backend-specific typed error")
    func cancellationPreservesTypedError() async throws {
        let engine = RecordingTransactionEngine(
            commitBehavior: .success,
            cancellationError: RecordingCancellationFailure.rejected
        )
        let runner = TransactionRunner(transactionExecutor: StorageTransactionExecutor(engine: engine), clock: SystemStorageClock())

        do {
            let _: Void = try await runner.run(
                configuration: TransactionConfiguration(maximumAttempts: 1)
            ) { _ in
                throw StorageError.invalidOperation("Rejected body")
            }
            Issue.record("Expected transaction cleanup failure")
        } catch let cleanup as StorageTransactionCleanupError {
            #expect(cleanup.cancellationErrors.count == 1)
            #expect(
                cleanup.cancellationErrors[0]
                    is RecordingCancellationFailure
            )
        }
    }

    @Test("Deadline cancellation failure is represented exactly once")
    func deadlineCancellationFailureIsNotDuplicated() async throws {
        let engine = RecordingTransactionEngine(
            commitBehavior: .success,
            cancellationError: RecordingCancellationFailure.rejected
        )
        let runner = TransactionRunner(transactionExecutor: StorageTransactionExecutor(engine: engine), clock: SystemStorageClock())

        do {
            let _: Void = try await runner.run(
                configuration: TransactionConfiguration(
                    timeout: 10,
                    maximumAttempts: 1
                )
            ) { _ in
                try await Task.sleep(for: .seconds(30))
            }
            Issue.record("Expected transaction cleanup failure")
        } catch let cleanup as StorageTransactionCleanupError {
            let deadline = cleanup.operationError
                as? TransactionExecutionDeadlineExceeded
            #expect(deadline?.timeoutMilliseconds == 10)
            #expect(deadline?.source == .transactionConfiguration)
            #expect(cleanup.cancellationErrors.count == 1)
            #expect(
                cleanup.cancellationErrors[0]
                    is RecordingCancellationFailure
            )
        }

        #expect(engine.snapshot.cancellations == 1)
        #expect(engine.snapshot.commits == 0)
    }

    @Test("Commit unknown is never cancelled or retried")
    func commitUnknownIsNeverCancelledOrRetried() async throws {
        let unknown = StorageError(
            code: .commitUnknownResult,
            operation: .commit,
            message: "Unknown commit result"
        )
        let engine = RecordingTransactionEngine(
            commitBehavior: .storageFailure(unknown)
        )
        let runner = TransactionRunner(transactionExecutor: StorageTransactionExecutor(engine: engine), clock: SystemStorageClock())
        let bodyAttempts = AttemptCounter()

        do {
            let _: Void = try await runner.run(
                configuration: TransactionConfiguration(maximumAttempts: 3)
            ) { _ in
                _ = bodyAttempts.increment()
            }
            Issue.record("Expected commitUnknownResult")
        } catch let error as StorageError {
            #expect(error.code == .commitUnknownResult)
        }

        let snapshot = engine.snapshot
        #expect(snapshot.created == 1)
        #expect(snapshot.commits == 1)
        #expect(snapshot.cancellations == 0)
        #expect(bodyAttempts.value == 1)
    }

    @Test("Bare commit cancellation is normalized to an unknown result")
    func bareCommitCancellationBecomesCommitUnknown() async throws {
        let engine = RecordingTransactionEngine(commitBehavior: .cancellation)
        let runner = TransactionRunner(transactionExecutor: StorageTransactionExecutor(engine: engine), clock: SystemStorageClock())

        do {
            let _: Void = try await runner.run(
                configuration: TransactionConfiguration(maximumAttempts: 3)
            ) { _ in }
            Issue.record("Expected commitUnknownResult")
        } catch let error as StorageError {
            #expect(error.code == .commitUnknownResult)
        }

        let snapshot = engine.snapshot
        #expect(snapshot.created == 1)
        #expect(snapshot.commits == 1)
        #expect(snapshot.cancellations == 0)
    }

    @Test("Caller cancellation after commit dispatch waits for success")
    func callerCancellationAfterCommitDispatchPreservesSuccess() async throws {
        let engine = RecordingTransactionEngine(
            commitBehavior: .delayedSuccess(.milliseconds(30))
        )
        let runner = TransactionRunner(transactionExecutor: StorageTransactionExecutor(engine: engine), clock: SystemStorageClock())
        let task = Task {
            try await runner.run(configuration: .default) { _ in "committed" }
        }

        while engine.snapshot.commits == 0 {
            await Task.yield()
        }
        task.cancel()

        #expect(try await task.value == "committed")
        #expect(engine.snapshot.commits == 1)
        #expect(engine.snapshot.cancellations == 0)
    }

    @Test("Portable deadline cannot discard an authoritative commit success")
    func portableDeadlineAfterCommitDispatchPreservesSuccess() async throws {
        let engine = RecordingTransactionEngine(
            commitBehavior: .delayedSuccess(.milliseconds(30))
        )
        let runner = TransactionRunner(transactionExecutor: StorageTransactionExecutor(engine: engine), clock: SystemStorageClock())

        let result = try await runner.run(
            configuration: TransactionConfiguration(
                timeout: 10,
                maximumAttempts: 1
            )
        ) { _ in
            "committed"
        }

        #expect(result == "committed")
        #expect(engine.snapshot.commits == 1)
        #expect(engine.snapshot.cancellations == 0)
    }

    @Test("Inherited deadline bounds a transaction without a relative timeout")
    func inheritedDeadlineBoundsUnconfiguredTimeout() async throws {
        let engine = RecordingTransactionEngine(commitBehavior: .success)
        let clock = SystemStorageClock()
        let runner = TransactionRunner(transactionExecutor: StorageTransactionExecutor(engine: engine), clock: clock)
        let deadline = TransactionExecutionDeadline(
            instant: clock.now.advanced(by: .milliseconds(10)),
            timeoutMilliseconds: 10
        )

        do {
            let _: Void = try await runner.run(
                configuration: TransactionConfiguration(maximumAttempts: 1),
                executionDeadline: deadline
            ) { _ in
                try await Task.sleep(for: .seconds(30))
            }
            Issue.record("Expected inherited transaction deadline")
        } catch let error as TransactionExecutionDeadlineExceeded {
            #expect(error.timeoutMilliseconds == 10)
            #expect(error.source == .inheritedOperation)
        }

        #expect(engine.snapshot.created == 1)
        #expect(engine.snapshot.commits == 0)
        #expect(engine.snapshot.cancellations == 1)
    }

    @Test("Expired inherited deadline fails before transaction creation")
    func expiredInheritedDeadlinePrecedesTransactionCreation() async throws {
        let engine = RecordingTransactionEngine(commitBehavior: .success)
        let clock = SystemStorageClock()
        let runner = TransactionRunner(transactionExecutor: StorageTransactionExecutor(engine: engine), clock: clock)
        let deadline = TransactionExecutionDeadline(
            instant: clock.now.advanced(by: .milliseconds(-1)),
            timeoutMilliseconds: .max
        )

        do {
            let _: Void = try await runner.run(
                configuration: TransactionConfiguration(maximumAttempts: 1),
                executionDeadline: deadline
            ) { _ in }
            Issue.record("Expected expired inherited transaction deadline")
        } catch let error as TransactionExecutionDeadlineExceeded {
            #expect(error.timeoutMilliseconds == UInt64(UInt32.max))
            #expect(error.source == .inheritedOperation)
        }

        #expect(engine.snapshot.created == 0)
    }

    @Test("Inherited deadline is shared by every retry attempt")
    func inheritedDeadlineSpansRetries() async throws {
        let engine = RecordingTransactionEngine(commitBehavior: .success)
        let clock = SystemStorageClock()
        let runner = TransactionRunner(transactionExecutor: StorageTransactionExecutor(engine: engine), clock: clock)
        let attempts = AttemptCounter()
        let deadline = TransactionExecutionDeadline(
            instant: clock.now.advanced(by: .milliseconds(100)),
            timeoutMilliseconds: 100
        )

        do {
            let _: Void = try await runner.run(
                configuration: TransactionConfiguration(
                    maximumAttempts: 2,
                    maxRetryDelay: 0,
                    initialRetryDelay: 0
                ),
                executionDeadline: deadline
            ) { _ in
                if attempts.increment() == 1 {
                    throw StorageError.transactionConflict
                }
                try await Task.sleep(for: .seconds(30))
            }
            Issue.record("Expected inherited deadline to span retries")
        } catch let error as TransactionExecutionDeadlineExceeded {
            #expect(error.timeoutMilliseconds == 100)
            #expect(error.source == .inheritedOperation)
        }

        #expect(attempts.value == 2)
    }

    @Test("Portable deadline is shared by every retry attempt")
    func portableDeadlineSpansRetries() async throws {
        let runner = TransactionRunner(transactionExecutor: StorageTransactionExecutor(engine: InMemoryEngine()), clock: SystemStorageClock())
        let attempts = AttemptCounter()

        do {
            let _: Void = try await runner.run(
                configuration: TransactionConfiguration(
                    timeout: 100,
                    maximumAttempts: 2,
                    maxRetryDelay: 0,
                    initialRetryDelay: 0
                )
            ) { _ in
                if attempts.increment() == 1 {
                    throw StorageError.transactionConflict
                }
                try await Task.sleep(for: .seconds(30))
            }
            Issue.record("Expected the shared portable deadline to expire")
        } catch let error as TransactionExecutionDeadlineExceeded {
            #expect(error.timeoutMilliseconds == 100)
            #expect(error.source == .transactionConfiguration)
        }

        #expect(attempts.value == 2)
    }

    @Test("Mutation aggregate overflow cancels without publishing writes")
    func mutationAggregateOverflowRollsBack() async throws {
        let engine = InMemoryEngine()
        let runner = TransactionRunner(transactionExecutor: StorageTransactionExecutor(engine: engine), clock: SystemStorageClock())
        let key: ByteString = [0xA0]

        do {
            let _: Void = try await runner.run(
                configuration: TransactionConfiguration(
                    maximumAttempts: 1,
                    maximumMutationAggregateBytes: 18
                )
            ) { transaction in
                try transaction.setValue([0x01], for: key)
            }
            Issue.record("Expected mutation aggregate byte rejection")
        } catch let error as TransactionMutationByteLimitError {
            #expect(
                error == .exceeded(actual: 19, maximum: 18)
            )
        }

        let verification = try engine.createTransaction()
        #expect(try await verification.getValue(for: key) == nil)
        try await verification.cancel()
    }

    @Test("Mutation aggregate meter is fresh for every retry attempt")
    func mutationAggregateMeterIsAttemptScoped() async throws {
        let engine = InMemoryEngine()
        let runner = TransactionRunner(transactionExecutor: StorageTransactionExecutor(engine: engine), clock: SystemStorageClock())
        let attempts = AttemptCounter()
        let key: ByteString = [0xA1]

        try await runner.run(
            configuration: TransactionConfiguration(
                maximumAttempts: 2,
                maxRetryDelay: 0,
                initialRetryDelay: 0,
                maximumMutationAggregateBytes: 19
            )
        ) { transaction in
            try transaction.setValue([0x02], for: key)
            guard attempts.increment() > 1 else {
                throw StorageError.transactionConflict
            }
        }

        #expect(attempts.value == 2)
        let verification = try engine.createTransaction()
        #expect(try await verification.getValue(for: key) == [0x02])
        try await verification.cancel()
    }

    @Test("Mutation admission remains attached across detached tasks")
    func detachedTaskCannotBypassMutationAdmission() async throws {
        let engine = InMemoryEngine()
        let runner = TransactionRunner(transactionExecutor: StorageTransactionExecutor(engine: engine), clock: SystemStorageClock())
        let key: ByteString = [0xA2]

        do {
            let _: Void = try await runner.run(
                configuration: TransactionConfiguration(
                    maximumAttempts: 1,
                    maximumMutationAggregateBytes: 18
                )
            ) { transaction in
                try await Task.detached {
                    try transaction.setValue([0x03], for: key)
                }.value
            }
            Issue.record("Expected detached mutation admission failure")
        } catch let error as TransactionMutationByteLimitError {
            #expect(error == .exceeded(actual: 19, maximum: 18))
        }

        let verification = try engine.createTransaction()
        #expect(try await verification.getValue(for: key) == nil)
        try await verification.cancel()
    }

    @Test("Nested transaction runners fail before creating a child attempt")
    func nestedTransactionRunnerIsRejected() async throws {
        let engine = RecordingTransactionEngine(commitBehavior: .success)
        let outer = TransactionRunner(transactionExecutor: StorageTransactionExecutor(engine: engine), clock: SystemStorageClock())
        let inner = TransactionRunner(transactionExecutor: StorageTransactionExecutor(engine: engine), clock: SystemStorageClock())

        try await outer.run(configuration: .default) { _ in
            do {
                let _: Void = try await inner.run(configuration: .default) { _ in }
                Issue.record("Expected nested runner rejection")
            } catch let error as StorageError {
                #expect(error.code == .invalidOperation)
            }
        }

        #expect(engine.snapshot.created == 1)
        #expect(engine.snapshot.commits == 1)
    }

    @Test("Invalid execution policy fails before transaction creation")
    func invalidPolicyFailsBeforeCreatingTransaction() async throws {
        let engine = RecordingTransactionEngine(commitBehavior: .success)
        let runner = TransactionRunner(transactionExecutor: StorageTransactionExecutor(engine: engine), clock: SystemStorageClock())

        do {
            let _: Void = try await runner.run(
                configuration: TransactionConfiguration(maximumAttempts: 0)
            ) { _ in }
            Issue.record("Expected invalid configuration")
        } catch let error as StorageError {
            #expect(error.code == .invalidOperation)
        }
        #expect(engine.snapshot.created == 0)
    }

    @Test("Read versions are captured before commit and version zero is valid")
    func readVersionCachePublishesZeroAfterCommit() async throws {
        let engine = RecordingTransactionEngine(commitBehavior: .success)
        let runner = TransactionRunner(transactionExecutor: StorageTransactionExecutor(engine: engine), clock: SystemStorageClock())
        let cache = ReadVersionCache(monotonicClock: SystemStorageClock())
        cache.updateFromCommit(version: 999)

        let result = try await runner.run(
            configuration: TransactionConfiguration(
                maximumAttempts: 1,
                cachePolicy: .cached
            ),
            readVersionCache: cache
        ) { _ in "ok" }

        #expect(result == "ok")
        #expect(engine.snapshot.readVersions == 1)
        #expect(cache.getCachedVersion(policy: .cached) == 0)
    }

    @Test("Retries retryable storage errors and then succeeds")
    func retriesRetryableStorageErrorsAndSucceeds() async throws {
        let engine = InMemoryEngine()
        let runner = TransactionRunner(transactionExecutor: StorageTransactionExecutor(engine: engine), clock: SystemStorageClock())
        let attempts = AttemptCounter()

        let result = try await runner.run(
            configuration: TransactionConfiguration(maximumAttempts: 3, maxRetryDelay: 1),
            operationDescription: "test retry success"
        ) { _ in
            let attempt = attempts.increment()
            if attempt < 3 {
                throw StorageError.transactionConflict
            }
            return "ok"
        }

        #expect(result == "ok")
        #expect(attempts.value == 3)
    }

    @Test("Retries retryable createTransaction errors and then succeeds")
    func retriesRetryableCreateTransactionErrorsAndSucceeds() async throws {
        let engine = FlakyCreateTransactionEngine(failuresBeforeSuccess: 2)
        let runner = TransactionRunner(transactionExecutor: StorageTransactionExecutor(engine: engine), clock: SystemStorageClock())
        let bodyAttempts = AttemptCounter()

        let result = try await runner.run(
            configuration: TransactionConfiguration(maximumAttempts: 3, maxRetryDelay: 1),
            operationDescription: "test create transaction retry"
        ) { transaction in
            _ = bodyAttempts.increment()
            try transaction.setValue([0x01], for: [0xA1])
            return "ok"
        }

        #expect(result == "ok")
        #expect(engine.createAttempts == 3)
        #expect(bodyAttempts.value == 1)
    }

    @Test("Preserves the last storage error after the attempt limit")
    func preservesLastStorageErrorAfterAttemptLimit() async throws {
        let engine = InMemoryEngine()
        let runner = TransactionRunner(transactionExecutor: StorageTransactionExecutor(engine: engine), clock: SystemStorageClock())
        let attempts = AttemptCounter()

        do {
            let _: Void = try await runner.run(
                configuration: TransactionConfiguration(maximumAttempts: 2, maxRetryDelay: 1),
                operationDescription: "test retry exhaustion"
            ) { _ in
                _ = attempts.increment()
                throw StorageError.transactionConflict
            }
            Issue.record("Expected retry exhaustion")
        } catch let error as StorageError {
            #expect(error.code == .transactionConflict)
        }

        #expect(attempts.value == 2)
    }

    @Test("Cancellation during backoff is not retried")
    func cancellationDuringBackoffIsNotRetried() async throws {
        let engine = InMemoryEngine()
        let runner = TransactionRunner(transactionExecutor: StorageTransactionExecutor(engine: engine), clock: SystemStorageClock())
        let attempts = AttemptCounter()

        let task = Task {
            let _: Void = try await runner.run(
                configuration: TransactionConfiguration(maximumAttempts: 5, maxRetryDelay: 50),
                operationDescription: "test cancellation"
            ) { _ in
                _ = attempts.increment()
                throw StorageError.transactionConflict
            }
        }

        while attempts.value == 0 {
            await Task.yield()
        }
        task.cancel()

        do {
            try await task.value
            Issue.record("Expected cancellation")
        } catch is CancellationError {
            #expect(attempts.value == 1)
        }
    }

    @Test("Instrumented transactions use the shared runner retry policy")
    func instrumentedTransactionsUseSharedRunnerRetryPolicy() async throws {
        let engine = InMemoryEngine()
        let attempts = AttemptCounter()
        let clock = SystemStorageClock()
        let timer = StoreTimer(monotonicClock: clock)
        let executor = StorageTransactionExecutor(engine: engine)

        let (result, metrics) = try await executor.withInstrumentedTransaction(
            timer: timer,
            clock: clock
        ) { tx in
            let attempt = attempts.increment()
            if attempt == 1 {
                throw StorageError.transactionConflict
            }
            try tx.setValue([0x01], for: [0xA0])
            return "committed"
        }

        #expect(result == "committed")
        #expect(attempts.value == 2)
        #expect(metrics.retryCount == 1)
        #expect(metrics.committed == true)
        #expect(metrics.writeCount == 1)
        #expect(timer.getCount(.retries) == 1)
    }
}

private final class AttemptCounter: Sendable {
    private let state = Mutex<Int>(0)

    var value: Int {
        state.withLock { $0 }
    }

    func increment() -> Int {
        state.withLock {
            $0 += 1
            return $0
        }
    }
}

private final class FlakyCreateTransactionEngine: StorageEngine, Sendable {
    struct Configuration: Sendable {
        let failuresBeforeSuccess: Int
    }

    typealias TransactionType = InMemoryTransaction

    private let engine = InMemoryEngine()
    private let remainingFailures: Mutex<Int>
    private let attempts = Mutex<Int>(0)

    var createAttempts: Int {
        attempts.withLock { $0 }
    }

    init(failuresBeforeSuccess: Int) {
        self.remainingFailures = Mutex(failuresBeforeSuccess)
    }

    init(configuration: Configuration) {
        self.remainingFailures = Mutex(configuration.failuresBeforeSuccess)
    }

    func createTransaction() throws -> InMemoryTransaction {
        attempts.withLock { $0 += 1 }
        let shouldFail = remainingFailures.withLock { remaining in
            guard remaining > 0 else { return false }
            remaining -= 1
            return true
        }
        if shouldFail {
            throw StorageError.transactionConflict
        }
        return try engine.createTransaction()
    }

    func withTransaction<T: Sendable>(
        _ operation: (any TransactionAccess) async throws -> T
    ) async throws -> T {
        let transaction = try createTransaction()
        do {
            let result = try await operation(transaction)
            try await transaction.commit()
            return result
        } catch {
            let operationError = error
            do {
                try await transaction.cancel()
            } catch {
                throw StorageTransactionCleanupError(
                    operationError: operationError,
                    cancellationError: error
                )
            }
            throw operationError
        }
    }

    var namespaceResolver: any NamespaceResolver {
        engine.namespaceResolver
    }

    var namespaceCatalog: (any NamespaceCatalog)? {
        engine.namespaceCatalog
    }
}

private enum RecordingCommitBehavior: Sendable {
    case success
    case delayedSuccess(Duration)
    case storageFailure(StorageError)
    case cancellation
}

private enum RecordingCancellationFailure: Error, Sendable {
    case rejected
}

private final class RecordingTransactionState: Sendable {
    struct Snapshot: Sendable {
        let created: Int
        let commits: Int
        let cancellations: Int
        let readVersions: Int
    }

    private struct Values: Sendable {
        var created = 0
        var commits = 0
        var cancellations = 0
        var readVersions = 0
    }

    private let values = Mutex(Values())

    var snapshot: Snapshot {
        values.withLock {
            Snapshot(
                created: $0.created,
                commits: $0.commits,
                cancellations: $0.cancellations,
                readVersions: $0.readVersions
            )
        }
    }

    func recordCreation() {
        values.withLock { $0.created += 1 }
    }

    func recordCommit() {
        values.withLock { $0.commits += 1 }
    }

    func recordCancellation() {
        values.withLock { $0.cancellations += 1 }
    }

    func recordReadVersion() {
        values.withLock { $0.readVersions += 1 }
    }
}

private final class RecordingTransactionEngine: StorageEngine, Sendable {
    struct Configuration: Sendable {
        let commitBehavior: RecordingCommitBehavior
    }

    typealias TransactionType = RecordingTransaction

    private let underlying = InMemoryEngine()
    private let state = RecordingTransactionState()
    private let commitBehavior: RecordingCommitBehavior
    private let cancellationError: (any Error)?

    var snapshot: RecordingTransactionState.Snapshot { state.snapshot }

    init(
        commitBehavior: RecordingCommitBehavior,
        cancellationError: (any Error)? = nil
    ) {
        self.commitBehavior = commitBehavior
        self.cancellationError = cancellationError
    }

    init(configuration: Configuration) async throws {
        self.commitBehavior = configuration.commitBehavior
        self.cancellationError = nil
    }

    func createTransaction() throws -> RecordingTransaction {
        state.recordCreation()
        return RecordingTransaction(
            underlying: try underlying.createTransaction(),
            state: state,
            commitBehavior: commitBehavior,
            cancellationError: cancellationError
        )
    }

    var namespaceResolver: any NamespaceResolver {
        underlying.namespaceResolver
    }

    var namespaceCatalog: (any NamespaceCatalog)? {
        underlying.namespaceCatalog
    }
}

private final class RecordingTransaction: Transaction, Sendable {
    typealias RangeResult = KeyValueRangeResult

    private let underlying: InMemoryTransaction
    private let state: RecordingTransactionState
    private let commitBehavior: RecordingCommitBehavior
    private let cancellationError: (any Error)?

    var capabilities: TransactionCapabilities { underlying.capabilities }
    var storageFailure: StorageError? { underlying.storageFailure }
    var mutationByteLimit: Int? { underlying.mutationByteLimit }
    var transactionDomain: StorageTransactionDomain {
        underlying.transactionDomain
    }

    init(
        underlying: InMemoryTransaction,
        state: RecordingTransactionState,
        commitBehavior: RecordingCommitBehavior,
        cancellationError: (any Error)?
    ) {
        self.underlying = underlying
        self.state = state
        self.commitBehavior = commitBehavior
        self.cancellationError = cancellationError
    }

    func configureMutationByteLimit(maximumBytes: Int?) throws {
        try underlying.configureMutationByteLimit(maximumBytes: maximumBytes)
    }

    func getValue(for key: ByteString, snapshot: Bool) async throws -> ByteString? {
        try await underlying.getValue(for: key, snapshot: snapshot)
    }

    func getValue(for key: ByteString) async throws -> ByteString? {
        try await underlying.getValue(for: key)
    }

    func getKey(
        selector: KeySelector,
        snapshot: Bool
    ) async throws -> ByteString? {
        try await underlying.getKey(selector: selector, snapshot: snapshot)
    }

    func rangeCursor(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        streamingMode: StreamingMode
    ) -> KeyValueCursor {
        underlying.rangeCursor(
            from: begin,
            to: end,
            limit: limit,
            reverse: reverse,
            snapshot: snapshot,
            streamingMode: streamingMode
        )
    }

    func setValue(_ value: ByteString, for key: ByteString) throws {
        try underlying.setValue(value, for: key)
    }

    func clear(key: ByteString) throws {
        try underlying.clear(key: key)
    }

    func clearRange(beginKey: ByteString, endKey: ByteString) throws {
        try underlying.clearRange(beginKey: beginKey, endKey: endKey)
    }

    func atomicOp(
        key: ByteString,
        param: ByteString,
        mutationType: MutationType
    ) throws {
        try underlying.atomicOp(key: key, param: param, mutationType: mutationType)
    }

    func setReadVersion(_ version: Int64) throws {
        try underlying.setReadVersion(version)
    }

    func getReadVersion() async throws -> Int64 {
        state.recordReadVersion()
        return try await underlying.getReadVersion()
    }

    func getCommittedVersion() throws -> Int64 {
        try underlying.getCommittedVersion()
    }

    func requestVersionstamp() -> any PendingTransactionVersionstamp {
        underlying.requestVersionstamp()
    }

    func commit() async throws {
        state.recordCommit()
        switch commitBehavior {
        case .success:
            try await underlying.commit()
        case .delayedSuccess(let delay):
            try await Task.sleep(for: delay)
            try await underlying.commit()
        case .storageFailure(let error):
            throw error
        case .cancellation:
            throw CancellationError()
        }
    }

    func cancel() async throws {
        state.recordCancellation()
        if let cancellationError {
            throw cancellationError
        }
        try await underlying.cancel()
    }
}

#endif
