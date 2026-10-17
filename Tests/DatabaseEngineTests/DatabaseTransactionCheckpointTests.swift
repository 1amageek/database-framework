import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import Synchronization
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("Database transaction checkpoints", .timeLimit(.minutes(1)))
struct DatabaseTransactionCheckpointTests {
    @Test("Nested restore preserves data, net effects and retained claims")
    func logicalRestoreRetainsEffectsAndClaims() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let meter = makeMeter(container)
        try await context.withCheckpointTransaction(checkpointLimits: limits(), workMeter: meter) { transaction in
            try await transaction.save(EntityMutationFixture(id: "same", title: "first"))
            let originalRows = meter.retainedIntermediateRows
            let first = try await transaction.createCheckpoint()
            try await transaction.save(EntityMutationFixture(id: "same", title: "second"))
            let second = try await transaction.createCheckpoint()
            try await transaction.delete(EntityMutationFixture(id: "same", title: "second"))
            try await transaction.rollback(to: second)
            let restoredSecond = try await transaction.fetch(EntityMutationFixture.self, identifiedBy: "same")
            #expect(restoredSecond?.title == "second")
            try await transaction.rollback(to: first)
            let restoredFirst = try await transaction.fetch(EntityMutationFixture.self, identifiedBy: "same")
            #expect(restoredFirst?.title == "first")
            let effects = try await transaction.persistedMutationEffects()
            #expect(effects.count == 1)
            #expect(effects.first?.kind == .insert)
            #expect(meter.retainedIntermediateRows == originalRows)
            try await transaction.release(first)
        }
        let persisted = try await context.model(for: "same", as: EntityMutationFixture.self)
        #expect(persisted?.title == "first")
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
        try await context.withCheckpointTransaction(checkpointLimits: limits(), workMeter: meter) { transaction in
            let marker = try await transaction.createCheckpoint()
            try await transaction.rollback(to: marker)
        }
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Ordinary failures close; checkpoint failures admit only rollback recovery")
    func recoverableFailureRequiresRollback() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let model = EntityMutationFixture(id: "present", title: "before")
        try await context.withTransaction { transaction in try await transaction.save(model) }
        await #expect(throws: DatabaseTransactionError.closed) {
            try await context.withTransaction { transaction in
                do {
                    try await transaction.save(model, precondition: .notExists)
                    Issue.record("Expected an existing row to reject creation")
                } catch let error as DatabaseContextError {
                    guard case .preconditionFailed = error else { throw error }
                }
                _ = try await transaction.fetch(EntityMutationFixture.self, identifiedBy: model.id)
            }
        }
        let meter = makeMeter(container)
        try await context.withCheckpointTransaction(checkpointLimits: limits(), workMeter: meter) { transaction in
            let marker = try await transaction.createCheckpoint()
            do {
                try await transaction.save(model, precondition: .notExists)
                Issue.record("Expected an existing row to reject creation")
            } catch let error as DatabaseContextError {
                guard case .preconditionFailed = error else { throw error }
            }
            await #expect(throws: DatabaseTransactionCheckpointError.failed) {
                _ = try await transaction.fetch(EntityMutationFixture.self, identifiedBy: model.id)
            }
            try await transaction.rollback(to: marker)
            try await transaction.save(EntityMutationFixture(id: model.id, title: "after"), precondition: .exists)
            let restored = try await transaction.fetch(EntityMutationFixture.self, identifiedBy: model.id)
            #expect(restored?.title == "after")
        }
        try await context.withCheckpointTransaction(requiredAccess: .read,
            checkpointLimits: limits(), workMeter: meter) { transaction in
            #expect(transaction.executionStorageAccess.checkpoints == nil)
            let marker = try await transaction.createCheckpoint()
            let before = try await transaction.fetch(EntityMutationFixture.self, identifiedBy: model.id)
            #expect(before?.title == "after")
            await #expect(throws: DatabaseReadTransactionError.mutationRequiresWriteAccess) {
                try await transaction.save(EntityMutationFixture(id: model.id, title: "must-not-write"))
            }
            try await transaction.rollback(to: marker)
            let after = try await transaction.fetch(EntityMutationFixture.self, identifiedBy: model.id)
            #expect(after?.title == "after")
            try await transaction.release(marker)
        }
        let unchanged = try await context.model(for: model.id, as: EntityMutationFixture.self)
        #expect(unchanged?.title == "after")
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Statement mutation failures preserve explicit checkpoint recovery")
    func statementMutationFailureAllowsRollback() async throws {
        let maintainer = CheckpointFailingMutationMaintainer()
        let schema = try Schema(entities: [EntityMutationFixture.schemaEntity])
        let container = try await DBContainer.open(for: schema,
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: DatabaseRuntimeConfiguration(
                executionIdentity: .init(identifier: "s12-statement-recovery", revision: 1),
                persistableMutationMaintainers: [maintainer],
                entityRuntimes: [DatabaseFrameworkRuntime.entity(EntityMutationFixture.self)]),
            security: .testingDisabled)
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        try await context.withTransaction { transaction in
            try await transaction.save(EntityMutationFixture(id: "statement", title: "before"))
        }
        let executor = DatabaseEntityStatementMutationExecutor(container: container,
            limits: try DatabaseEntityMutationLimits(maximumChanges: 16, maximumPreconditions: 16))
        let statement = QueryStatement.update(UpdateQuery(
            target: TableRef(EntityMutationFixture.persistableType),
            assignments: [Assignment(column: "title", value: .string("after"))],
            filter: .equal(.col("id"), .string("statement"))))
        let meter = makeMeter(container)
        try await context.withCheckpointTransaction(checkpointLimits: limits(), workMeter: meter) { transaction in
            let marker = try await transaction.createCheckpoint()
            await #expect(throws: CheckpointFailingMutationMaintainer.Failure.expected) {
                _ = try await executor.execute(statement, transaction: transaction, workMeter: meter)
            }
            #expect(maintainer.failureCount == 1)
            await #expect(throws: DatabaseTransactionCheckpointError.failed) {
                _ = try await transaction.fetch(EntityMutationFixture.self, identifiedBy: "statement")
            }
            try await transaction.rollback(to: marker)
            let restored = try await transaction.fetch(EntityMutationFixture.self, identifiedBy: "statement")
            #expect(restored?.title == "before")
            let restoredEffects = try await transaction.persistedMutationEffects()
            #expect(restoredEffects.isEmpty)
            let effects = try await executor.execute(statement, transaction: transaction, workMeter: meter)
            #expect(effects.count == 1)
            #expect(effects.first?.kind == .update)
        }
        let persisted = try await context.model(for: "statement", as: EntityMutationFixture.self)
        #expect(persisted?.title == "after")
        #expect(maintainer.failureCount == 1)
        #expect(meter.retainedIntermediateBytes == 0)
        #expect(meter.retainedIntermediateRows == 0)
    }

    @Test("A checkpoint drains admitted bound reads and revokes their cursors")
    func checkpointDrainsBoundScopes() async throws {
        let engine = ControlledStorageEngine(base: InMemoryEngine())
        let container = try await makeContainer(engine: engine)
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let meter = makeMeter(container)
        try await context.withCheckpointTransaction(checkpointLimits: limits(), workMeter: meter) { transaction in
            _ = try await transaction.withStatementScope {
                await #expect(throws: DatabaseTransactionCheckpointError.barrierInsideOperation) {
                    _ = try await transaction.createCheckpoint()
                }
            }
            let key = container.metadataSubspace.pack(Tuple("checkpoint-scope"))
            let suspension = engine.control.suspendNextValueRead(for: key)
            let read = Task {
                try await transaction.withStatementScope {
                    try await context.indexQueryContext.withSession(workMeter: meter) { session in
                        var cursor = session.transaction.rangeCursor(from: .firstGreaterOrEqual(key),
                            to: .firstGreaterOrEqual(key.appending(0xFF)), limit: 1,
                            reverse: false, snapshot: false, streamingMode: .small)
                        _ = try await session.transaction.getValue(for: key, snapshot: false)
                        try await cursor.finish()
                    }
                }
            }
            await suspension.waitUntilEntered()
            let completed = Mutex(false)
            let checkpoint = Task {
                let token = try await transaction.createCheckpoint()
                completed.withLock { $0 = true }
                return token
            }
            while !(await transaction.hasPendingCheckpointBarrier) { await Task.yield() }
            #expect(completed.withLock { $0 } == false)
            await #expect(throws: DatabaseTransactionError.concurrentOperation) {
                _ = try await transaction.fetch(EntityMutationFixture.self, identifiedBy: "absent")
            }
            suspension.release()
            try await read.value
            let token = try await checkpoint.value
            #expect(completed.withLock { $0 })
            try await transaction.rollback(to: token)
            #expect(engine.control.finishedRangeCursorCount > 0)
        }
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Retries replace attempt tokens and commit outcomes gate result return")
    func retryAndCommitOutcomeOwnPublication() async throws {
        let engine = ControlledStorageEngine(base: InMemoryEngine())
        let container = try await makeContainer(engine: engine)
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let meter = makeMeter(container)
        let captured = DatabaseTransactionAttemptCapture()
        engine.control.failNextMutatingCommit(with: .transactionConflict)
        try await context.withCheckpointTransaction(configuration: TransactionConfiguration(
            maximumAttempts: 2, maxRetryDelay: 0, initialRetryDelay: 0),
            checkpointLimits: limits(), workMeter: meter) { transaction in
            let token = try await transaction.createCheckpoint()
            captured.append(transaction, checkpoint: token)
            try await transaction.save(EntityMutationFixture(id: "retry", title: "committed"))
        }
        let attempts = captured.attempts
        #expect(attempts.count == 2)
        if attempts.count == 2 {
            #expect(ObjectIdentifier(attempts[0].0) != ObjectIdentifier(attempts[1].0))
            #expect(attempts[0].1 != attempts[1].1)
        }
        for attempt in attempts {
            await #expect(throws: DatabaseTransactionError.closed) {
                try await attempt.0.rollback(to: attempt.1)
            }
        }
        let commit = engine.control.suspendNextMutatingCommit()
        let operation = Task {
            try await context.withCheckpointTransaction(checkpointLimits: limits(), workMeter: meter) { transaction in
                try await transaction.save(EntityMutationFixture(id: "held", title: "closed"))
            }
            captured.markReturned()
        }
        await commit.waitUntilEntered()
        #expect(captured.didReturn == false)
        commit.release()
        try await operation.value
        #expect(captured.didReturn)
        #expect(meter.retainedIntermediateBytes == 0)

        let unknownEngine = CommitOutcomeUnknownEngine()
        let unknownContainer = try await makeContainer(engine: unknownEngine)
        defer { await unknownContainer.shutdown() }
        let unknownContext = unknownContainer.testBaseContext()
        unknownEngine.reportNextCommitAsUnknown()
        do {
            try await unknownContext.withCheckpointTransaction(checkpointLimits: limits(),
                workMeter: makeMeter(unknownContainer)) { transaction in
                captured.recordUnknownAttempt()
                try await transaction.save(EntityMutationFixture(id: "unknown", title: "uncertain"))
            }
            Issue.record("An unknown commit must not return a successful callback result")
        } catch let error as StorageError {
            #expect(error.code == .commitUnknownResult)
        }
        #expect(captured.unknownAttemptCount == 1)
        await #expect(throws: DatabaseContextError.commitOutcomeUnknown) {
            try await unknownContext.withTransaction { _ in () }
        }
    }

    private func limits() throws -> TransactionCheckpointLimits {
        try TransactionCheckpointLimits(maximumEntries: 10_000, maximumDepth: 16,
            maximumRetainedBytes: 4 * 1_024 * 1_024, maximumWorkUnits: 100_000,
            maximumReadPageRows: 128)
    }

    private func makeMeter(_ container: DBContainer) -> DatabaseWorkMeter {
        DatabaseWorkMeter(budget: ExecutionBudget(maximumIntermediateBytes: 4 * 1_024 * 1_024),
            monotonicClock: container.monotonicClock)
    }

    private func makeContainer(engine: any StorageEngine = InMemoryEngine()) async throws -> DBContainer {
        let schema = try Schema(entities: [EntityMutationFixture.schemaEntity])
        return try await DBContainer.open(for: schema, configuration: .testing(storageEngine: engine),
            runtimeConfiguration: DatabaseFrameworkRuntime.configuration(
                executionIdentity: .init(identifier: "s12-checkpoint", revision: 1),
                entityRuntimes: [DatabaseFrameworkRuntime.entity(EntityMutationFixture.self)]),
            security: .testingDisabled)
    }
}
