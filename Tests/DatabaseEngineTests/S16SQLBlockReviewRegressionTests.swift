import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import Synchronization
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("S16 SQL block review regressions", .timeLimit(.minutes(1)))
struct S16SQLBlockReviewRegressionTests {
    @Test("EXECUTE access follows live definitions through named and ALL deallocation")
    func livePreparedAccess() async throws {
        for deallocation: String? in ["reused", nil] {
            let engine = InMemoryEngine()
            let container = try await makeContainer(engine)
            defer { await container.shutdown() }
            let insert = SQLPrepare(name: "reused", statement: .insert(InsertQuery(
                target: TableRef(EntityMutationFixture.persistableType), columns: ["id", "title"],
                source: .values([[.string("retained"), .string("value")]]))))
            let select = SQLPrepare(name: "reused", statement: .select(SelectQuery(
                projection: [.expression(.int(1))], source: .values([[]]))))
            let result = try await SQLBlockExecutor(context: container.testBaseContext()).execute(SQLBlock(steps: [
                .prepare(insert), .execute(SQLExecute(name: "reused")), .deallocate(deallocation),
                .prepare(select), .execute(SQLExecute(name: "reused"))
            ]), options: options())
            guard case .command(.insert, let affected) = result.results[1],
                  case .rows(let rows) = result.results[4] else { Issue.record("Expected both live definitions"); continue }
            #expect(affected == 1)
            #expect(rows.rows.count == 1)
            #expect(try await container.testBaseContext().model(for: "retained", as: EntityMutationFixture.self)?.title == "value")
        }
    }

    @Test("Caller cancellation preserves a suspended statement-timeout cancellation failure")
    func cancellationJoinsGateFailure() async throws {
        let engine = S16CheckpointEngine()
        let cancellation = engine.control.holdCancellation(failure: S16CancellationFailure.rejected)
        let runner = TransactionRunner(transactionExecutor: StorageTransactionExecutor(engine: engine), clock: TestProcessMonotonicClock())
        let task = Task {
            try await runner.run(configuration: TransactionConfiguration(maximumAttempts: 1)) { _ in
                try await SQLBlockStatementDeadline.run(milliseconds: 1, clock: TestProcessMonotonicClock()) {
                    try await Task.sleep(for: .seconds(30))
                }
            }
        }
        let monitor = try await cancellation.waitUntilEntered(beforeCompletionOf: task)
        task.cancel()
        cancellation.release()
        do { try await task.value; Issue.record("Expected cancellation cleanup failure") }
        catch let error as StorageTransactionCleanupError {
            #expect((error.operationError as? SQLBlockExecutionError) == .statementTimeout)
            #expect(error.cancellationErrors.count == 1)
            #expect(error.cancellationErrors.first is S16CancellationFailure)
        }
        await monitor.value
        #expect(engine.control.cancellationCount == 1)
        await engine.shutdown()
    }

    @Test("Statement timeout covers every held physical checkpoint transition")
    func checkpointStatementDeadlines() async throws {
        for kind in S16CheckpointKind.allCases {
            let engine = S16CheckpointEngine()
            let container = try await makeContainer(engine)
            defer { await container.shutdown() }
            let checkpoint = engine.control.holdCheckpoint(kind)
            var steps: [SQLBlockStep] = [
                .setting(.set(name: .statementTimeout, value: .string("100ms"), scope: .local))
            ]
            switch kind {
            case .create: steps.append(.savepoint("held"))
            case .rollback: steps += [.savepoint("held"), .rollbackTo("held")]
            case .release: steps += [.savepoint("held"), .release("held")]
            }
            let block = try SQLBlock(steps: steps)
            let task = Task {
                try await SQLBlockExecutor(context: container.testBaseContext()).execute(block, options: options())
            }
            let monitor = try await checkpoint.waitUntilEntered(beforeCompletionOf: task)
            // The real physical transition remains suspended across the configured deadline.
            try await Task.sleep(for: .milliseconds(300))
            checkpoint.release()
            await #expect(throws: SQLBlockExecutionError.statementTimeout) { try await task.value }
            await monitor.value
            #expect(engine.control.cancellationCount >= 1)
        }
    }

    private func makeContainer(_ engine: any StorageEngine) async throws -> DBContainer {
        try await DBContainer.open(for: Schema(entities: [EntityMutationFixture.schemaEntity]),
            configuration: .testing(storageEngine: engine), runtimeConfiguration: DatabaseFrameworkRuntime.configuration(
                executionIdentity: .init(identifier: "s16-block-review", revision: 1),
                entityRuntimes: [DatabaseFrameworkRuntime.entity(EntityMutationFixture.self)]), security: .testingDisabled)
    }
    private func options() throws -> SQLBlockExecutionOptions {
        SQLBlockExecutionOptions(checkpointLimits: try TransactionCheckpointLimits(maximumEntries: 10_000,
            maximumDepth: 32, maximumRetainedBytes: 4 * 1_024 * 1_024,
            maximumWorkUnits: 1_000_000, maximumReadPageRows: 128))
    }
}

private enum S16CheckpointKind: Sendable, CaseIterable { case create, rollback, release }
private enum S16CancellationFailure: Error { case rejected }

private final class S16CheckpointControl: Sendable {
    private struct State: Sendable {
        var checkpoint: (S16CheckpointKind, StorageOperationBarrier)?
        var cancellation: (StorageOperationBarrier, any Error)?
        var cancellations = 0
    }
    private let state = Mutex(State())
    var cancellationCount: Int { state.withLock { $0.cancellations } }
    func holdCheckpoint(_ kind: S16CheckpointKind) -> StorageOperationBarrier {
        let barrier = StorageOperationBarrier()
        state.withLock { $0.checkpoint = (kind, barrier) }
        return barrier
    }
    func holdCancellation(failure: any Error) -> StorageOperationBarrier {
        let barrier = StorageOperationBarrier()
        state.withLock { $0.cancellation = (barrier, failure) }
        return barrier
    }
    func transition(_ kind: S16CheckpointKind) async {
        let barrier = state.withLock { state -> StorageOperationBarrier? in
            guard state.checkpoint?.0 == kind else { return nil }
            defer { state.checkpoint = nil }
            return state.checkpoint?.1
        }
        if let barrier { await barrier.enterAndWait() }
    }
    func cancel() async throws {
        let held = state.withLock { state in
            state.cancellations += 1
            defer { state.cancellation = nil }
            return state.cancellation
        }
        if let held { await held.0.enterAndWait(); throw held.1 }
    }
}

/// Forwards actual storage and checkpoint behavior while holding selected lifecycle boundaries.
private final class S16CheckpointEngine: StorageEngine, Sendable {
    typealias Configuration = InMemoryEngine.Configuration
    typealias TransactionType = S16CheckpointTransaction
    private let base: InMemoryEngine
    let control = S16CheckpointControl()
    init() { base = InMemoryEngine() }
    init(configuration: Configuration) async throws { base = InMemoryEngine(configuration: configuration) }
    var transactionDomain: StorageTransactionDomain { base.transactionDomain }
    var directoryAccess: any DirectoryAccess { base.directoryAccess }
    func createTransaction() throws -> S16CheckpointTransaction {
        S16CheckpointTransaction(base: try base.createTransaction(), control: control)
    }
    func requestShutdown() { base.requestShutdown() }
    func waitUntilShutdown() async { await base.waitUntilShutdown() }
}

private final class S16CheckpointTransaction: Transaction, TransactionCheckpointAccess, Sendable {
    typealias RangeResult = KeyValueRangeResult
    private let base: InMemoryTransaction
    private let control: S16CheckpointControl
    init(base: InMemoryTransaction, control: S16CheckpointControl) { self.base = base; self.control = control }
    var transactionDomain: StorageTransactionDomain { base.transactionDomain }
    var capabilities: TransactionCapabilities { base.capabilities }
    var storageFailure: StorageError? { base.storageFailure }
    var mutationByteLimit: Int? { base.mutationByteLimit }
    var checkpoints: (any TransactionCheckpointAccess)? { base.checkpoints == nil ? nil : self }
    func configureCheckpoints(limits: TransactionCheckpointLimits) throws { try base.configureCheckpoints(limits: limits) }
    func performOperation(_ operation: @escaping @Sendable () async throws -> Void) async throws { try await base.performOperation(operation) }
    func configureMutationByteLimit(maximumBytes: Int?) throws { try base.configureMutationByteLimit(maximumBytes: maximumBytes) }
    func createCheckpoint() async throws -> TransactionCheckpoint {
        await control.transition(.create)
        guard let checkpoints = base.checkpoints else { throw DatabaseTransactionCheckpointError.unavailable }
        return try await checkpoints.createCheckpoint()
    }
    func rollback(to checkpoint: TransactionCheckpoint) async throws {
        await control.transition(.rollback)
        guard let checkpoints = base.checkpoints else { throw DatabaseTransactionCheckpointError.unavailable }
        try await checkpoints.rollback(to: checkpoint)
    }
    func release(_ checkpoint: TransactionCheckpoint) async throws {
        await control.transition(.release)
        guard let checkpoints = base.checkpoints else { throw DatabaseTransactionCheckpointError.unavailable }
        try await checkpoints.release(checkpoint)
    }
    func getValue(for key: ByteString) async throws -> ByteString? { try await base.getValue(for: key) }
    func getValue(for key: ByteString, snapshot: Bool) async throws -> ByteString? { try await base.getValue(for: key, snapshot: snapshot) }
    func getKey(selector: KeySelector, snapshot: Bool) async throws -> ByteString? { try await base.getKey(selector: selector, snapshot: snapshot) }
    func rangeCursor(from begin: KeySelector, to end: KeySelector, limit: Int, reverse: Bool, snapshot: Bool, streamingMode: StreamingMode) -> KeyValueCursor {
        base.rangeCursor(from: begin, to: end, limit: limit, reverse: reverse, snapshot: snapshot, streamingMode: streamingMode)
    }
    func setValue(_ value: ByteString, for key: ByteString) throws { try base.setValue(value, for: key) }
    func clear(key: ByteString) throws { try base.clear(key: key) }
    func clearRange(beginKey: ByteString, endKey: ByteString) throws { try base.clearRange(beginKey: beginKey, endKey: endKey) }
    func atomicOp(key: ByteString, param: ByteString, mutationType: MutationType) throws { try base.atomicOp(key: key, param: param, mutationType: mutationType) }
    func setReadVersion(_ version: Int64) throws { try base.setReadVersion(version) }
    func getReadVersion() async throws -> Int64 { try await base.getReadVersion() }
    func setOption(forOption option: TransactionOption) throws { try base.setOption(forOption: option) }
    func setOption(to value: ByteString?, forOption option: TransactionOption) throws { try base.setOption(to: value, forOption: option) }
    func setOption(to value: Int, forOption option: TransactionOption) throws { try base.setOption(to: value, forOption: option) }
    func addConflictRange(beginKey: ByteString, endKey: ByteString, type: ConflictRangeType) throws { try base.addConflictRange(beginKey: beginKey, endKey: endKey, type: type) }
    func requestVersionstamp() -> any PendingTransactionVersionstamp { base.requestVersionstamp() }
    func commit() async throws { try await base.commit() }
    func cancel() async throws { try await base.cancel(); try await control.cancel() }
    func getCommittedVersion() throws -> Int64 { try base.getCommittedVersion() }
}
