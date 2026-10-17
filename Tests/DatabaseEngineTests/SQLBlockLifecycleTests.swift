import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import Synchronization
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("SQL block producing lifecycle", .timeLimit(.minutes(1)))
struct SQLBlockLifecycleTests {
    @Test("Public results remain unavailable until the producing commit closes")
    func commitGatesResults() async throws {
        let engine = ControlledStorageEngine(base: InMemoryEngine())
        let container = try await makeContainer(engine: engine)
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let returned = SQLBlockCompletionProbe()
        let barrier = engine.control.suspendNextMutatingCommit()
        let task = Task {
            let result = try await SQLBlockExecutor(context: context).execute(
                SQLBlock(steps: [.statement(insert("held")), .statement(select())]), options: options())
            returned.markReturned()
            return result
        }
        let monitor = try await barrier.waitUntilEntered(beforeCompletionOf: task)
        #expect(returned.didReturn == false)
        barrier.release()
        let result = try await task.value
        await monitor.value
        #expect(returned.didReturn)
        #expect(result.results.count == 2)
        #expect(try await context.model(for: "held", as: EntityMutationFixture.self)?.title == "value")
        #expect(engine.control.openedRangeCursorCount == engine.control.finishedRangeCursorCount)
    }

    @Test("Cancellation at commit exposes no block result after authoritative cleanup")
    func cancellationAfterCommitPreventsResult() async throws {
        let engine = ControlledStorageEngine(base: InMemoryEngine())
        let container = try await makeContainer(engine: engine)
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let returned = SQLBlockCompletionProbe()
        let barrier = engine.control.suspendNextMutatingCommit()
        let task = Task {
            let result = try await SQLBlockExecutor(context: context).execute(
                SQLBlock(steps: [.statement(insert("cancelled")), .statement(select())]), options: options())
            returned.markReturned()
            return result
        }
        let monitor = try await barrier.waitUntilEntered(beforeCompletionOf: task)
        task.cancel()
        barrier.release()
        await #expect(throws: CancellationError.self) { try await task.value }
        await monitor.value
        #expect(returned.didReturn == false)
        #expect(engine.control.openedRangeCursorCount == engine.control.finishedRangeCursorCount)
        let independent = container.testBaseContext()
        #expect(try await independent.model(for: "cancelled", as: EntityMutationFixture.self)?.title == "value")
    }

    @Test("Retry discards prior names and results without refunding consumed row budget")
    func retryUsesFreshStateAndCumulativeBudget() async throws {
        let engine = ControlledStorageEngine(base: InMemoryEngine())
        let container = try await makeContainer(engine: engine)
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let configuration = TransactionConfiguration(maximumAttempts: 2, maxRetryDelay: 0, initialRetryDelay: 0)
        func block(_ id: String) throws -> SQLBlock {
            try SQLBlock(steps: [.statement(insert(id)), .savepoint("same"),
                .statement(update(id)), .rollbackTo("same"), .statement(select(id))])
        }
        engine.control.failNextMutatingCommit(with: .transactionConflict)
        let result = try await SQLBlockExecutor(context: context).execute(block("retry"),
            options: options(configuration: configuration))
        #expect(result.results.count == 5)
        guard case .rows(let rows) = result.results[4] else { Issue.record("Expected retained rows"); return }
        #expect(rows.rows.count == 1)
        #expect(rows.rows[0].values == [.primitive(.string("value"))])
        engine.control.failNextMutatingCommit(with: .transactionConflict)
        await #expect(throws: DatabaseWorkLimitError.self) {
            try await SQLBlockExecutor(context: context).execute(block("limited-retry"),
                options: options(budget: ExecutionBudget(maximumRows: 4), configuration: configuration))
        }
        #expect(try await context.model(for: "limited-retry", as: EntityMutationFixture.self) == nil)
        #expect(try await context.model(for: "retry", as: EntityMutationFixture.self)?.title == "value")
    }

    @Test("Commit unknown remains a typed uncertain failure and never publishes results")
    func commitUnknownDoesNotPublish() async throws {
        let engine = CommitOutcomeUnknownEngine()
        let container = try await makeContainer(engine: engine)
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        engine.reportNextCommitAsUnknown()
        do {
            _ = try await SQLBlockExecutor(context: context).execute(
                SQLBlock(steps: [.statement(insert("unknown")), .statement(select())]), options: options())
            Issue.record("An uncertain commit must not return block results")
        } catch let error as StorageError {
            #expect(error.code == .commitUnknownResult)
        }
        await #expect(throws: DatabaseContextError.commitOutcomeUnknown) {
            try await SQLBlockExecutor(context: context).execute(
                SQLBlock(steps: [.savepoint("rejected")]), options: options())
        }
    }

    @Test("DML preserves its explicitly admitted statement instant across retry and keeps the live clock")
    func explicitDMLTemporalEnvironmentSurvivesRetry() async throws {
        let engine = ControlledStorageEngine(base: InMemoryEngine())
        let clock = SQLBlockAdvancingClock()
        let container = try await makeContainer(engine: engine)
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let environment = SQLTemporalEnvironment(
            transactionInstant: Timestamp(secondsSinceUnixEpoch: 10),
            statementInstant: Timestamp(secondsSinceUnixEpoch: 20), wallClock: clock)
        let meter = DatabaseWorkMeter(budget: ExecutionBudget(), monotonicClock: container.monotonicClock)
        let executor = DatabaseEntityStatementMutationExecutor(container: container,
            limits: try DatabaseEntityMutationLimits(maximumChanges: 8, maximumPreconditions: 1))
        let title = Expression.concatenate(
            .concatenate(.cast(.function(FunctionCall(name: "statement_timestamp", arguments: [])), targetType: .text), .string("|")),
            .cast(.function(FunctionCall(name: "clock_timestamp", arguments: [])), targetType: .text))
        let statement = QueryStatement.insert(InsertQuery(target: TableRef(EntityMutationFixture.persistableType),
            columns: ["id", "title"], source: .values([[.string("temporal-retry"), title]])))
        engine.control.failNextMutatingCommit(with: .transactionConflict)
        try await context.withSQLTemporalStatement(environment: environment) {
            try await context.withCheckpointTransaction(configuration: TransactionConfiguration(
                maximumAttempts: 2, maxRetryDelay: 0, initialRetryDelay: 0),
                checkpointLimits: options().checkpointLimits, workMeter: meter) { transaction in
                try await transaction.withStatementScope {
                    _ = try await executor.execute(statement, transaction: transaction,
                        workMeter: meter, temporalEnvironment: environment)
                }
            }
        }
        #expect(clock.sampleCount == 2)
        let liveInstant = try #require(clock.lastSample)
        let expected = try await context.query(SelectQuery(projection: [ProjectionItem(.concatenate(
            .concatenate(.cast(.literal(.timestamp(environment.statementInstant)), targetType: .text), .string("|")),
            .cast(.literal(.timestamp(liveInstant)), targetType: .text)))], source: .values([[]])))
        let model = try #require(try await context.model(for: "temporal-retry", as: EntityMutationFixture.self))
        #expect(expected.rows.first?.values == [.primitive(.string(model.title))])
        #expect(meter.consumedRows == 2)
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    private func makeContainer(engine: any StorageEngine) async throws -> DBContainer {
        let schema = try Schema(entities: [EntityMutationFixture.schemaEntity])
        return try await DBContainer.open(for: schema, configuration: .testing(storageEngine: engine),
            runtimeConfiguration: DatabaseFrameworkRuntime.configuration(
                executionIdentity: .init(identifier: "sql-block-lifecycle", revision: 1),
                entityRuntimes: [DatabaseFrameworkRuntime.entity(EntityMutationFixture.self)]), security: .testingDisabled)
    }

    private func options(budget: ExecutionBudget = ExecutionBudget(),
        configuration: TransactionConfiguration = .default) throws -> SQLBlockExecutionOptions {
        SQLBlockExecutionOptions(checkpointLimits: try TransactionCheckpointLimits(maximumEntries: 10_000,
            maximumDepth: 32, maximumRetainedBytes: 4 * 1_024 * 1_024,
            maximumWorkUnits: 1_000_000, maximumReadPageRows: 128), budget: budget, configuration: configuration)
    }

    private func insert(_ id: String) -> QueryStatement {
        .insert(InsertQuery(target: TableRef(EntityMutationFixture.persistableType),
            columns: ["id", "title"], source: .values([[.string(id), .string("value")]])))
    }
    private func update(_ id: String) -> QueryStatement {
        .update(UpdateQuery(target: TableRef(EntityMutationFixture.persistableType),
            assignments: [Assignment(column: "title", value: .string("discarded"))],
            filter: .equal(.col("id"), .string(id))))
    }
    private func select(_ id: String? = nil) -> QueryStatement {
        .select(SelectQuery(projection: [.column("title")], source: .table(TableRef(EntityMutationFixture.persistableType)),
            filter: id.map { .equal(.col("id"), .string($0)) }))
    }
}

private final class SQLBlockCompletionProbe: Sendable {
    private let returned = Mutex(false)
    func markReturned() { returned.withLock { $0 = true } }
    var didReturn: Bool { returned.withLock { $0 } }
}

private final class SQLBlockAdvancingClock: WallClock, Sendable {
    private struct State: Sendable {
        var next: Int64 = 1_700_000_000
        var count: Int = 0
        var last: Timestamp?
    }
    private let state = Mutex(State())
    var now: Timestamp {
        state.withLock { state in
            let instant = Timestamp(secondsSinceUnixEpoch: state.next)
            state.next += 1
            state.count += 1
            state.last = instant
            return instant
        }
    }
    var sampleCount: Int { state.withLock { $0.count } }
    var lastSample: Timestamp? { state.withLock { $0.last } }
}
