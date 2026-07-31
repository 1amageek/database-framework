import DatabaseTypes
import StorageKit
import StorageKitSystemClock
import Synchronization
import Testing
@testable import DatabaseEngine

@Suite("Transaction deadline and cleanup behavior")
struct TransactionDeadlineAndCleanupTests {
    @Test("Portable deadline cancels once before commit")
    func portableDeadlineCancelsOnce() async throws {
        let engine = DeadlineControlledEngine()
        let runner = makeTransactionRunner(engine: engine)

        do {
            let _: Void = try await runner.run(
                configuration: TransactionConfiguration(
                    timeout: 10,
                    maximumAttempts: 1
                )
            ) { _ in
                try await Task.sleep(for: .seconds(30))
            }
            Issue.record("Expected deadline failure")
        } catch let error as TransactionExecutionDeadlineExceeded {
            #expect(error.timeoutMilliseconds == 10)
            #expect(error.source == .transactionConfiguration)
        }

        #expect(engine.snapshot.created == 1)
        #expect(engine.snapshot.cancellations == 1)
        #expect(engine.snapshot.commits == 0)
    }

    @Test("Equal operation and cancellation errors are both retained")
    func equalErrorsRemainCleanupFailure() async throws {
        let failure = StorageError.transactionConflict
        let engine = DeadlineControlledEngine(cancellationError: failure)
        let runner = makeTransactionRunner(engine: engine)

        do {
            let _: Void = try await runner.run(
                configuration: TransactionConfiguration(maximumAttempts: 1)
            ) { _ in
                throw failure
            }
            Issue.record("Expected cleanup failure")
        } catch let cleanup as StorageTransactionCleanupError {
            #expect((cleanup.operationError as? StorageError) == failure)
            #expect(cleanup.cancellationErrors.count == 1)
            #expect(
                cleanup.cancellationErrors.first.flatMap {
                    $0 as? StorageError
                } == failure
            )
        }

        #expect(engine.snapshot.cancellations == 1)
    }

    @Test("Cancellation preserves its concrete typed error")
    func cancellationPreservesConcreteError() async throws {
        let engine = DeadlineControlledEngine(
            cancellationError: DeadlineCancellationError.rejected
        )
        let runner = makeTransactionRunner(engine: engine)

        do {
            let _: Void = try await runner.run(
                configuration: TransactionConfiguration(maximumAttempts: 1)
            ) { _ in
                throw StorageError.invalidOperation("Rejected operation")
            }
            Issue.record("Expected cleanup failure")
        } catch let cleanup as StorageTransactionCleanupError {
            #expect(cleanup.cancellationErrors.count == 1)
            #expect(
                cleanup.cancellationErrors.first.map {
                    $0 is DeadlineCancellationError
                } == true
            )
        }
    }

    @Test("Deadline cancellation failure appears exactly once")
    func deadlineCancellationFailureIsNotDuplicated() async throws {
        let engine = DeadlineControlledEngine(
            cancellationError: DeadlineCancellationError.rejected
        )
        let runner = makeTransactionRunner(engine: engine)

        do {
            let _: Void = try await runner.run(
                configuration: TransactionConfiguration(
                    timeout: 10,
                    maximumAttempts: 1
                )
            ) { _ in
                try await Task.sleep(for: .seconds(30))
            }
            Issue.record("Expected cleanup failure")
        } catch let cleanup as StorageTransactionCleanupError {
            let deadline = cleanup.operationError
                as? TransactionExecutionDeadlineExceeded
            #expect(deadline?.timeoutMilliseconds == 10)
            #expect(deadline?.source == .transactionConfiguration)
            #expect(cleanup.cancellationErrors.count == 1)
            #expect(
                cleanup.cancellationErrors.first.map {
                    $0 is DeadlineCancellationError
                } == true
            )
        }

        #expect(engine.snapshot.cancellations == 1)
        #expect(engine.snapshot.commits == 0)
    }

    @Test("Expired inherited deadline rejects before transaction creation")
    func expiredInheritedDeadlineRejectsAdmission() async throws {
        let engine = DeadlineControlledEngine()
        let clock = FixedStorageClock(
            now: StorageInstant(durationSinceReference: .milliseconds(1))
        )
        let runner = makeTransactionRunner(engine: engine, clock: clock)
        let deadline = TransactionExecutionDeadline(
            instant: StorageInstant(durationSinceReference: .zero),
            timeoutMilliseconds: .max
        )

        do {
            let _: Void = try await runner.run(
                configuration: TransactionConfiguration(maximumAttempts: 1),
                executionDeadline: deadline
            ) { _ in }
            Issue.record("Expected inherited deadline failure")
        } catch let error as TransactionExecutionDeadlineExceeded {
            #expect(error.timeoutMilliseconds == UInt64(UInt32.max))
            #expect(error.source == .inheritedOperation)
        }

        #expect(engine.snapshot.created == 0)
    }
}

private func makeTransactionRunner(
    engine: DeadlineControlledEngine,
    clock: any StorageMonotonicClock = SystemStorageClock()
) -> TransactionRunner {
    TransactionRunner(
        transactionExecutor: StorageTransactionExecutor(engine: engine),
        clock: clock
    )
}

private struct FixedStorageClock: StorageMonotonicClock {
    let now: StorageInstant

    func sleep(
        until deadline: StorageInstant
    ) async throws(StorageClockError) {}
}

private enum DeadlineCancellationError: Error, Sendable {
    case rejected
}

private final class DeadlineObservationState: Sendable {
    struct Snapshot: Sendable {
        let created: Int
        let commits: Int
        let cancellations: Int
    }

    private struct Values: Sendable {
        var created = 0
        var commits = 0
        var cancellations = 0
    }

    private let values = Mutex(Values())

    var snapshot: Snapshot {
        values.withLock {
            Snapshot(
                created: $0.created,
                commits: $0.commits,
                cancellations: $0.cancellations
            )
        }
    }

    func recordCreation() { values.withLock { $0.created += 1 } }
    func recordCommit() { values.withLock { $0.commits += 1 } }
    func recordCancellation() { values.withLock { $0.cancellations += 1 } }
}

private final class DeadlineControlledEngine: StorageEngine, Sendable {
    struct Configuration: Sendable {}

    typealias TransactionType = DeadlineControlledTransaction

    private let underlying = InMemoryEngine()
    private let state = DeadlineObservationState()
    private let cancellationError: (any Error)?

    var snapshot: DeadlineObservationState.Snapshot { state.snapshot }

    init(cancellationError: (any Error)? = nil) {
        self.cancellationError = cancellationError
    }

    init(configuration: Configuration) async throws {
        self.cancellationError = nil
    }

    func createTransaction() throws -> DeadlineControlledTransaction {
        state.recordCreation()
        return DeadlineControlledTransaction(
            underlying: try underlying.createTransaction(),
            state: state,
            cancellationError: cancellationError
        )
    }

    var namespaceResolver: any NamespaceResolver {
        underlying.namespaceResolver
    }

    var namespaceCatalog: (any NamespaceCatalog)? {
        underlying.namespaceCatalog
    }

    func requestShutdown() {
        underlying.requestShutdown()
    }

    func waitUntilShutdown() async {
        await underlying.waitUntilShutdown()
    }
}

private final class DeadlineControlledTransaction: Transaction, Sendable {
    private let underlying: InMemoryTransaction
    private let state: DeadlineObservationState
    private let cancellationError: (any Error)?

    var capabilities: TransactionCapabilities { underlying.capabilities }
    var compaction: StorageCompactionAccess? { underlying.compaction }
    var storageFailure: StorageError? { underlying.storageFailure }
    var mutationByteLimit: Int? { underlying.mutationByteLimit }
    var transactionDomain: StorageTransactionDomain {
        underlying.transactionDomain
    }

    init(
        underlying: InMemoryTransaction,
        state: DeadlineObservationState,
        cancellationError: (any Error)?
    ) {
        self.underlying = underlying
        self.state = state
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

    func setOption(forOption option: TransactionOption) throws {
        try underlying.setOption(forOption: option)
    }

    func setOption(
        to value: ByteString?,
        forOption option: TransactionOption
    ) throws {
        try underlying.setOption(to: value, forOption: option)
    }

    func setOption(
        to value: Int,
        forOption option: TransactionOption
    ) throws {
        try underlying.setOption(to: value, forOption: option)
    }

    func addConflictRange(
        beginKey: ByteString,
        endKey: ByteString,
        type: ConflictRangeType
    ) throws {
        try underlying.addConflictRange(
            beginKey: beginKey,
            endKey: endKey,
            type: type
        )
    }

    func getEstimatedRangeSizeBytes(
        beginKey: ByteString,
        endKey: ByteString
    ) async throws -> Int {
        try await underlying.getEstimatedRangeSizeBytes(
            beginKey: beginKey,
            endKey: endKey
        )
    }

    func getRangeSplitPoints(
        beginKey: ByteString,
        endKey: ByteString,
        chunkSize: Int
    ) async throws -> [ByteString] {
        try await underlying.getRangeSplitPoints(
            beginKey: beginKey,
            endKey: endKey,
            chunkSize: chunkSize
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
        try underlying.atomicOp(
            key: key,
            param: param,
            mutationType: mutationType
        )
    }

    func setReadVersion(_ version: Int64) throws {
        try underlying.setReadVersion(version)
    }

    func getReadVersion() async throws -> Int64 {
        try await underlying.getReadVersion()
    }

    func getCommittedVersion() throws -> Int64 {
        try underlying.getCommittedVersion()
    }

    func requestVersionstamp() -> any PendingTransactionVersionstamp {
        underlying.requestVersionstamp()
    }

    func commit() async throws {
        state.recordCommit()
        try await underlying.commit()
    }

    func cancel() async throws {
        state.recordCancellation()
        if let cancellationError {
            throw cancellationError
        }
        try await underlying.cancel()
    }
}
