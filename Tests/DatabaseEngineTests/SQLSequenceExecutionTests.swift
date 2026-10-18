import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import Synchronization
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("S13 request sequence effects", .timeLimit(.minutes(1)))
struct SQLSequenceExecutionTests {
    @Test("NEXTVAL, CURRVAL and LASTVAL retain request order through savepoint rollback")
    func requestHistoryAndSavepoint() async throws {
        let container = try await open()
        defer { await container.shutdown() }
        let result = try await run(container, [.sequence(.create(sequence())),
            call("NEXTVAL", [.string("counter")]), .savepoint("s"),
            call("NEXTVAL", [.string("counter")]), .rollbackTo("s"),
            call("CURRVAL", [.string("counter")]), call("LASTVAL", []),
            call("NEXTVAL", [.string("counter")])])
        #expect(try value(result, at: 1) == 1)
        #expect(try value(result, at: 3) == 2)
        #expect(try value(result, at: 5) == 2)
        #expect(try value(result, at: 6) == 2)
        #expect(try value(result, at: 7) == 3)
        let next = try await run(container, [call("NEXTVAL", [.string("counter")])])
        #expect(try value(next, at: 0) == 4)
        let quotedUppercase = QueryStatement.select(SelectQuery(projection: [.expression(.function(
            FunctionCall(name: "NEXTVAL", arguments: [.string("counter")])))], source: .values([[]])))
        let effectMeter = DatabaseWorkMeter(budget: ExecutionBudget(), monotonicClock: container.monotonicClock)
        #expect(try !SQLSequenceEffectAnalysis.containsSequence(quotedUppercase, workMeter: effectMeter))
        do {
            _ = try await run(container, [.statement(quotedUppercase)])
            Issue.record("The exact uppercase semantic function is not a builtin alias")
        } catch let error as CanonicalReadError {
            guard case .expressionEvaluation(.unsupportedFunction("NEXTVAL"), _) = error else {
                Issue.record("Expected the original exact function binding error"); return
            }
        }
        await #expect(throws: SQLSequenceExecutionError.self) {
            try await run(container, [call("CURRVAL", [.string("counter")])])
        }
    }

    @Test("A later statement error rolls data back but keeps consumed sequence values")
    func wholeRequestFailureKeepsConsumption() async throws {
        let container = try await open()
        defer { await container.shutdown() }
        _ = try await run(container, [.sequence(.create(sequence()))])
        await #expect(throws: SQLBlockExecutionError.unknownCheckpoint("missing")) {
            try await run(container, [call("NEXTVAL", [.string("counter")]), .rollbackTo("missing")])
        }
        let next = try await run(container, [call("NEXTVAL", [.string("counter")])])
        #expect(try value(next, at: 0) == 2)
    }

    @Test("SETVAL false rewinds consumption without rewriting the initialized request history")
    func setvalFalseAndHistory() async throws {
        let container = try await open()
        defer { await container.shutdown() }
        let result = try await run(container, [.sequence(.create(sequence())),
            call("NEXTVAL", [.string("counter")]),
            call("SETVAL", [.string("counter"), .int(42), .literal(.bool(false))]),
            call("CURRVAL", [.string("counter")]), call("LASTVAL", []),
            call("NEXTVAL", [.string("counter")])])
        #expect(try value(result, at: 1) == 1)
        #expect(try value(result, at: 2) == 42)
        #expect(try value(result, at: 3) == 1)
        #expect(try value(result, at: 4) == 1)
        #expect(try value(result, at: 5) == 42)
    }

    @Test("SETVAL true initializes CURRVAL but cannot choose LASTVAL's sequence")
    func setvalTrueDoesNotInitializeLastval() async throws {
        let container = try await open()
        defer { await container.shutdown() }
        _ = try await run(container, [.sequence(.create(sequence()))])
        let current = try await run(container, [call("SETVAL", [.string("counter"), .int(7)]),
            call("CURRVAL", [.string("counter")])])
        #expect(try value(current, at: 1) == 7)
        await #expect(throws: SQLSequenceExecutionError.self) {
            try await run(container, [call("SETVAL", [.string("counter"), .int(9)]), call("LASTVAL", [])])
        }
        let next = try await run(container, [call("NEXTVAL", [.string("counter")])])
        #expect(try value(next, at: 0) == 10)
    }

    @Test("Transactional RESTART rolls back its incarnation while ordinary allocation stays consumed")
    func restartCheckpointRollback() async throws {
        let container = try await open()
        defer { await container.shutdown() }
        _ = try await run(container, [.sequence(.create(sequence())), call("NEXTVAL", [.string("counter")])])
        let result = try await run(container, [.savepoint("s"), .sequence(.alter(sequence(), restart: 80)),
            call("NEXTVAL", [.string("counter")]), .rollbackTo("s"),
            call("NEXTVAL", [.string("counter")])])
        #expect(try value(result, at: 2) == 80)
        #expect(try value(result, at: 4) == 2)
        let next = try await run(container, [call("NEXTVAL", [.string("counter")])])
        #expect(try value(next, at: 0) == 3)
    }

    @Test("A rolled-back CREATE never publishes its orphan allocation cell")
    func createRollbackAndRecreate() async throws {
        let container = try await open()
        defer { await container.shutdown() }
        await #expect(throws: SQLBlockExecutionError.unknownCheckpoint("missing")) {
            try await run(container, [.sequence(.create(sequence())), call("NEXTVAL", [.string("counter")]),
                .rollbackTo("missing")])
        }
        #expect(container.sqlCatalog.sequences.isEmpty)
        let result = try await run(container, [.sequence(.create(sequence())), call("NEXTVAL", [.string("counter")])])
        #expect(try value(result, at: 1) == 1)
    }

    @Test("Exhaustion and signed overflow never wrap; explicit CYCLE chooses the declared bound")
    func numericBoundaries() async throws {
        let container = try await open()
        defer { await container.shutdown() }
        let edge = try SQLSequence(id: "edge", namespaceID: "public", name: "edge",
            start: .max, increment: .max, minimum: .min, maximum: .max)
        _ = try await run(container, [.sequence(.create(edge)), call("NEXTVAL", [.string("edge")])])
        await #expect(throws: SQLSequenceExecutionError.self) {
            try await run(container, [call("NEXTVAL", [.string("edge")])])
        }
        let cycle = try SQLSequence(id: "cycle", namespaceID: "public", name: "cycle",
            start: 2, increment: 1, minimum: 1, maximum: 2, cycle: true)
        let result = try await run(container, [.sequence(.create(cycle)),
            call("NEXTVAL", [.string("cycle")]), call("NEXTVAL", [.string("cycle")])])
        #expect(try value(result, at: 1) == 2)
        #expect(try value(result, at: 2) == 1)
    }

    @Test("Drop is transactional and leaves no catalog-visible sequence after commit")
    func dropAndRollback() async throws {
        let container = try await open()
        defer { await container.shutdown() }
        _ = try await run(container, [.sequence(.create(sequence()))])
        _ = try await run(container, [.savepoint("s"), .sequence(.drop(id: "counter", cascade: false)), .rollbackTo("s")])
        #expect(container.sqlCatalog.sequences.count == 1)
        _ = try await run(container, [.sequence(.drop(id: "counter", cascade: false))])
        #expect(container.sqlCatalog.sequences.isEmpty)
        await #expect(throws: SQLSequenceExecutionError.self) {
            try await run(container, [call("NEXTVAL", [.string("counter")])])
        }
    }

    @Test("DROP RESTRICT and CASCADE follow constant default dependencies without deleting the column")
    func defaultDependencyDrop() async throws {
        let container = try await openDynamic()
        defer { await container.shutdown() }
        _ = try await run(container, [.sequence(.create(sequence()))])
        let catalog = container.sqlCatalog
        let relation = try #require(catalog.relations.first)
        let columns = try relation.columns.map { column in
            if column.name == "title" {
                return try SQLColumn(id: column.id, name: column.name, backing: column.backing,
                    policy: .input(defaultExpression: .cast(.function(FunctionCall(name: "nextval",
                        arguments: [.string("counter")])), targetType: .text)))
            }
            return column
        }
        let dependent = try SQLRelation(id: relation.id, namespaceID: relation.namespaceID,
            name: relation.name, entityName: relation.entityName, columns: columns,
            primaryKeyColumnIDs: relation.primaryKeyColumnIDs,
            primaryKeyConstraint: relation.primaryKeyConstraint, constraints: relation.constraints)
        _ = try await container.publishSQLCatalog(SQLCatalog(namespaces: catalog.namespaces,
            relations: [dependent], sequences: catalog.sequences), expectedGeneration: container.schemaGeneration,
            idempotencyKey: "sequence-default-dependency", authorization: TestBaseEnvironment.authorization)
        let publishedColumns = try #require(container.sqlCatalog.relations.first?.columns)
        let boundColumn = try #require(publishedColumns.first { $0.name == "title" })
        #expect(boundColumn.sequenceBindings == [try SQLSequenceBinding(originalName: "counter", sequenceID: "counter")])
        await #expect(throws: SQLSequenceExecutionError.dependentIdentity("counter")) {
            try await run(container, [.sequence(.drop(id: "counter", cascade: false))])
        }
        _ = try await run(container, [.savepoint("s"), .sequence(.drop(id: "counter", cascade: true)), .rollbackTo("s")])
        #expect(container.sqlCatalog.sequences.count == 1)
        #expect(container.sqlCatalog.relations.first?.columns == publishedColumns)
        _ = try await run(container, [.sequence(.drop(id: "counter", cascade: true))])
        #expect(container.sqlCatalog.sequences.isEmpty)
        let retained = try #require(container.sqlCatalog.relations.first?.columns.first(where: { $0.name == "title" }))
        #expect(retained.backing == columns.first(where: { $0.name == "title" })?.backing)
        #expect(retained.policy == .input(defaultExpression: nil))
    }

    @Test("Logical sequence rename preserves allocator identity and checkpoint rollback")
    func renamePreservesIdentity() async throws {
        let container = try await open()
        defer { await container.shutdown() }
        _ = try await run(container, [.sequence(.create(sequence()))])
        let renamed = try SQLSequence(id: "counter", namespaceID: "public", name: "renamed",
            start: 1, increment: 1, minimum: 1, maximum: 100)
        _ = try await run(container, [.savepoint("before"), .sequence(.alter(renamed, restart: nil)), .rollbackTo("before")])
        #expect(container.sqlCatalog.sequences.first?.name == "counter")
        let first = try await run(container, [call("NEXTVAL", [.string("counter")])])
        #expect(try value(first, at: 0) == 1)
        _ = try await run(container, [.sequence(.alter(renamed, restart: nil))])
        await #expect(throws: SQLSequenceExecutionError.self) {
            try await run(container, [call("NEXTVAL", [.string("counter")])])
        }
        let next = try await run(container, [call("NEXTVAL", [.string("renamed")])])
        #expect(try value(next, at: 0) == 2)
        #expect(container.sqlCatalog.sequences.first?.id == "counter")
    }

    @Test("Allocation replay is separate from the storage retry maximum")
    func demandDoesNotSpendRetryAttempts() async throws {
        let container = try await open()
        defer { await container.shutdown() }
        let configuration = TransactionConfiguration(maximumAttempts: 1)
        let result = try await SQLBlockExecutor(context: container.testBaseContext()).execute(
            SQLBlock(steps: [.sequence(.create(sequence())), call("NEXTVAL", [.string("counter")]),
                call("NEXTVAL", [.string("counter")]), call("NEXTVAL", [.string("counter")])]),
            options: options(configuration: configuration))
        #expect(try value(result, at: 3) == 3)
    }

    @Test("Queued cancellation and deadline leave the active request running and release no foreign permit")
    func queuedCancellationAndDeadline() async throws {
        let engine = ControlledStorageEngine(base: InMemoryEngine())
        let container = try await open(engine: engine)
        defer { await container.shutdown() }
        let barrier = engine.control.suspendNextReadOnlyCommit()
        let active = Task { try await run(container, [.statement(.select(SelectQuery(
            projection: [.expression(.int(1))], source: .values([[]]))))]) }
        let monitor = try await barrier.waitUntilEntered(beforeCompletionOf: active)
        let cancelled = Task { try await run(container, [.savepoint("queued")]) }
        cancelled.cancel()
        await #expect(throws: CancellationError.self) { try await cancelled.value }
        let timedOut = Task {
            try await SQLBlockExecutor(context: container.testBaseContext()).execute(
                SQLBlock(steps: [.savepoint("deadline")]), options: options(budget: ExecutionBudget(timeoutMilliseconds: 100)))
        }
        await #expect(throws: TransactionExecutionDeadlineExceeded.self) { try await timedOut.value }
        barrier.release()
        _ = try await active.value
        await monitor.value
        _ = try await run(container, [.savepoint("after")])
    }

    @Test("Recovery-close commit prevents output, including cancellation while cleanup is suspended")
    func recoveryCloseGatesReady() async throws {
        let engine = ControlledStorageEngine(base: InMemoryEngine())
        let container = try await open(engine: engine)
        defer { await container.shutdown() }
        _ = try await run(container, [.sequence(.create(sequence()))])
        let dataCommit = engine.control.suspendNextReadOnlyCommit()
        let returned = SQLSequenceCompletionProbe()
        let task = Task {
            let value = try await run(container, [call("NEXTVAL", [.string("counter")])])
            returned.mark()
            return value
        }
        let dataMonitor = try await dataCommit.waitUntilEntered(beforeCompletionOf: task)
        let recoveryCommit = engine.control.suspendNextMutatingCommit()
        dataCommit.release()
        let recoveryMonitor = try await recoveryCommit.waitUntilEntered(beforeCompletionOf: task)
        #expect(!returned.didReturn)
        task.cancel()
        recoveryCommit.release()
        await #expect(throws: CancellationError.self) { try await task.value }
        await dataMonitor.value
        await recoveryMonitor.value
        #expect(!returned.didReturn)
        let next = try await run(container, [call("NEXTVAL", [.string("counter")])])
        #expect(try value(next, at: 0) == 2)
    }

    @Test("An allocator commit-unknown never returns a result or reuses its committed value")
    func allocatorCommitUnknown() async throws {
        let engine = CommitOutcomeUnknownEngine()
        let container = try await open(engine: engine)
        defer { await container.shutdown() }
        _ = try await run(container, [.sequence(.create(sequence()))])
        engine.reportNextCommitAsUnknown()
        do {
            _ = try await run(container, [call("NEXTVAL", [.string("counter")])])
            Issue.record("Allocator commit uncertainty must remain a failure")
        } catch let error as StorageError {
            #expect(error.code == .commitUnknownResult)
        }
        let next = try await run(container, [call("NEXTVAL", [.string("counter")])])
        #expect(try value(next, at: 0) == 2)
    }

    @Test("Allocator storage retry retains one consumed value")
    func allocatorRetry() async throws {
        let engine = ControlledStorageEngine(base: InMemoryEngine())
        let container = try await open(engine: engine)
        defer { await container.shutdown() }
        _ = try await run(container, [.sequence(.create(sequence()))])
        engine.control.failNextMutatingCommit(with: .transactionConflict)
        let first = try await run(container, [call("NEXTVAL", [.string("counter")])])
        #expect(try value(first, at: 0) == 1)
        let second = try await run(container, [call("NEXTVAL", [.string("counter")])])
        #expect(try value(second, at: 0) == 2)
    }

    private func openDynamic() async throws -> DBContainer {
        let schema = try Schema(entities: [EntityMutationFixture.schemaEntity])
        return try await DBContainer.open(testing: schema,
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: DatabaseFrameworkRuntime.configuration(
                executionIdentity: .init(identifier: "s13-sequence-dynamic", revision: 1), schema: schema),
            security: .testingDisabled)
    }

    private func open(engine: any StorageEngine = InMemoryEngine()) async throws -> DBContainer {
        try await DBContainer.open(for: Schema(entities: [EntityMutationFixture.schemaEntity]),
            configuration: .testing(storageEngine: engine),
            runtimeConfiguration: DatabaseFrameworkRuntime.configuration(
                executionIdentity: .init(identifier: "s13-sequences", revision: 1),
                entityRuntimes: [DatabaseFrameworkRuntime.entity(EntityMutationFixture.self)]), security: .testingDisabled)
    }

    private func sequence() throws -> SQLSequence {
        try SQLSequence(id: "counter", namespaceID: "public", name: "counter",
            start: 1, increment: 1, minimum: 1, maximum: 100)
    }

    private func run(_ container: DBContainer, _ steps: [SQLBlockStep]) async throws -> SQLBlockResult {
        try await SQLBlockExecutor(context: container.testBaseContext()).execute(SQLBlock(steps: steps), options: options())
    }

    private func options(budget: ExecutionBudget = ExecutionBudget(),
                         configuration: TransactionConfiguration = .default) throws -> SQLBlockExecutionOptions {
        SQLBlockExecutionOptions(checkpointLimits: try TransactionCheckpointLimits(maximumEntries: 10_000,
            maximumDepth: 32, maximumRetainedBytes: 4 * 1_024 * 1_024,
            maximumWorkUnits: 1_000_000, maximumReadPageRows: 128), budget: budget, configuration: configuration)
    }

    private func call(_ name: String, _ arguments: [Expression]) -> SQLBlockStep {
        .statement(.select(SelectQuery(projection: [.expression(.function(FunctionCall(name: name.lowercased(), arguments: arguments)))],
            source: .values([[]]))))
    }

    private func value(_ result: SQLBlockResult, at index: Int) throws -> Int64 {
        guard case .rows(let response) = result.results[index],
              case .primitive(.int64(let value)) = response.rows.first?.values.first else {
            throw SQLSequenceExecutionError.invalidPersistedState
        }
        return value
    }
}

private final class SQLSequenceCompletionProbe: Sendable {
    private let returned = Mutex(false)
    var didReturn: Bool { returned.withLock { $0 } }
    func mark() { returned.withLock { $0 = true } }
}
