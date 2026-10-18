import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import Synchronization
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("S16 SQL request controls", .timeLimit(.minutes(1)))
struct S16SQLBlockTests {
    @Test("Explicit rollback retains observations and discards real mutations")
    func rollbackKeepsObservations() async throws {
        let engine = ControlledStorageEngine(base: InMemoryEngine())
        let container = try await makeContainer(engine)
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let result = try await SQLBlockExecutor(context: context).execute(SQLBlock(steps: [
            .transaction(.begin), .statement(insert("discarded")), .statement(select("discarded")),
            .transaction(.rollback)
        ]), options: options())
        #expect(result.results.count == 4)
        guard case .rows(let rows) = result.results[2] else { Issue.record("Expected prior observations"); return }
        #expect(rows.rows.first?.values == [.primitive(.string("value"))])
        #expect(try await container.testBaseContext().model(for: "discarded", as: EntityMutationFixture.self) == nil)
        #expect(engine.control.openedRangeCursorCount == engine.control.finishedRangeCursorCount)
    }

    @Test("Explicit commit persists one complete request")
    func explicitCommit() async throws {
        let container = try await makeContainer(InMemoryEngine())
        defer { await container.shutdown() }
        let result = try await SQLBlockExecutor(context: container.testBaseContext()).execute(SQLBlock(steps: [
            .transaction(.begin), .statement(insert("committed")), .transaction(.commit)
        ]), options: options())
        #expect(result.results.count == 3)
        #expect(try await container.testBaseContext().model(for: "committed", as: EntityMutationFixture.self)?.title == "value")
    }

    @Test("Settings restore at rollback-to and release preserves current settings")
    func checkpointSettings() async throws {
        let container = try await makeContainer(InMemoryEngine())
        defer { await container.shutdown() }
        let result = try await SQLBlockExecutor(context: container.testBaseContext()).execute(SQLBlock(steps: [
            .setting(.set(name: .statementTimeout, value: .string("1000ms"), scope: .session)),
            .savepoint("policy"),
            .setting(.set(name: .statementTimeout, value: .string("2s"), scope: .local)),
            .setting(.show(.statementTimeout)), .rollbackTo("policy"),
            .setting(.show(.statementTimeout)),
            .setting(.set(name: .statementTimeout, value: .string("3s"), scope: .local)),
            .release("policy"), .setting(.show(.statementTimeout))
        ]), options: options())
        #expect(try scalar(result, 3) == "2000")
        #expect(try scalar(result, 5) == "1000")
        #expect(try scalar(result, 8) == "3000")
    }

    @Test("Session and local spellings both end at request close")
    func settingsDoNotEscapeRequest() async throws {
        let container = try await makeContainer(InMemoryEngine())
        defer { await container.shutdown() }
        let executor = SQLBlockExecutor(context: container.testBaseContext())
        _ = try await executor.execute(SQLBlock(steps: [
            .setting(.set(name: .statementTimeout, value: .string("20s"), scope: .session)),
            .setting(.set(name: .timeZone, value: .string("+02:00"), scope: .local))
        ]), options: options())
        let result = try await executor.execute(SQLBlock(steps: [
            .setting(.show(.statementTimeout)), .setting(.show(.timeZone))
        ]), options: options())
        #expect(try scalar(result, 0) == "0")
        #expect(try scalar(result, 1) == "UTC")
    }

    @Test("Invalid setting aborts earlier writes and never reaches later rollback-to")
    func invalidSettingAbortsWholeRequest() async throws {
        let container = try await makeContainer(InMemoryEngine())
        defer { await container.shutdown() }
        await #expect(throws: SQLBlockExecutionError.invalidSettingValue(.statementTimeout)) {
            try await SQLBlockExecutor(context: container.testBaseContext()).execute(SQLBlock(steps: [
                .statement(insert("failed")), .savepoint("later"),
                .setting(.set(name: .statementTimeout, value: .string("-1ms"), scope: .local)),
                .rollbackTo("later"), .statement(insert("never"))
            ]), options: options())
        }
        #expect(try await container.testBaseContext().model(for: "failed", as: EntityMutationFixture.self) == nil)
        #expect(try await container.testBaseContext().model(for: "never", as: EntityMutationFixture.self) == nil)
    }

    @Test("TimeZone setting changes the admitted temporal evaluator")
    func timeZoneAffectsActualExpression() async throws {
        let container = try await makeContainer(InMemoryEngine())
        defer { await container.shutdown() }
        let cast = Expression.cast(.cast(.string("2026-01-01 00:00:00+00"),
            targetType: .timestamp(withTimeZone: true)), targetType: .text)
        let result = try await SQLBlockExecutor(context: container.testBaseContext()).execute(SQLBlock(steps: [
            .setting(.set(name: .timeZone, value: .string("+02:00"), scope: .session)),
            .statement(.select(SelectQuery(projection: [.expression(cast)], source: .values([[]]))))
        ]), options: options())
        let value = try scalar(result, 1)
        #expect(value.contains("02:00:00"))
        #expect(value.contains("+02"))
    }

    @Test("Zero disables only the statement timeout overlay")
    func disabledStatementTimeout() async throws {
        let container = try await makeContainer(InMemoryEngine())
        defer { await container.shutdown() }
        let result = try await SQLBlockExecutor(context: container.testBaseContext()).execute(SQLBlock(steps: [
            .setting(.set(name: .statementTimeout, value: .string("0"), scope: .local)),
            .statement(insert("disabled"))
        ]), options: options())
        #expect(result.results.count == 2)
        #expect(try await container.testBaseContext().model(for: "disabled", as: EntityMutationFixture.self) != nil)
    }

    @Test("SHOW preserves the preconfigured named zone identifier")
    func preconfiguredNamedTimeZone() async throws {
        let provider = try SQLTimeZoneDatabase(definitions: [
            .init(name: "Supplied/UTC", tzif: SQLTimeZoneFixtures.utc())],
            provenance: ByteString(utf8: "UTC fixture"),
            limits: .init(maximumInputBytes: 1_000, maximumRetainedBytes: 4_000, maximumTransitions: 10))
        let meter = DatabaseWorkMeter(budget: ExecutionBudget(), monotonicClock: TestProcessMonotonicClock())
        let rules = try provider.rules(named: "Supplied/UTC", workMeter: meter)
        let temporal = try SQLTemporalConfiguration(timeZone: .named(name: "Supplied/UTC", rules: rules),
            rulesProvider: provider, limits: .init(maximumAbbreviations: 0, maximumRetainedBytes: 12))
        let container = try await makeContainer(InMemoryEngine(), temporal: temporal)
        defer { await container.shutdown() }
        let result = try await SQLBlockExecutor(context: container.testBaseContext()).execute(SQLBlock(steps: [
            .setting(.show(.timeZone)),
            .setting(.set(name: .timeZone, value: .string("+02:00"), scope: .local)),
            .setting(.reset(.timeZone, scope: .local)),
            .setting(.show(.timeZone))
        ]), options: options())
        #expect(try scalar(result, 0) == "Supplied/UTC")
        #expect(try scalar(result, 3) == "Supplied/UTC")
    }

    @Test("Statement timeout waits for mandatory cancellation and producing task close")
    func timeoutJoinsCancellationAndProducer() async throws {
        let close = StorageOperationBarrier()
        let producer = StorageOperationBarrier()
        let returned = Mutex(false)
        let task = Task {
            defer { returned.withLock { $0 = true } }
            return try await ActiveTransactionCancellation.$request.withValue({ _ in
                await close.enterAndWait()
                return nil
            }) {
                try await SQLBlockStatementDeadline.run(milliseconds: 1, clock: TestProcessMonotonicClock()) {
                    await producer.enterAndWait()
                    try Task.checkCancellation()
                    return 1
                }
            }
        }
        let closeMonitor = try await close.waitUntilEntered(beforeCompletionOf: task)
        #expect(returned.withLock { $0 } == false)
        close.release()
        #expect(returned.withLock { $0 } == false)
        producer.release()
        await #expect(throws: SQLBlockExecutionError.statementTimeout) { try await task.value }
        await closeMonitor.value
        #expect(returned.withLock { $0 })
    }

    private func makeContainer(_ engine: any StorageEngine, temporal: SQLTemporalConfiguration = .utc) async throws -> DBContainer {
        try await DBContainer.open(for: Schema(entities: [EntityMutationFixture.schemaEntity]),
            configuration: .testing(storageEngine: engine, sqlTemporal: temporal),
            runtimeConfiguration: DatabaseFrameworkRuntime.configuration(
                executionIdentity: .init(identifier: "s16-controls", revision: 1),
                entityRuntimes: [DatabaseFrameworkRuntime.entity(EntityMutationFixture.self)]), security: .testingDisabled)
    }

    private func options() throws -> SQLBlockExecutionOptions {
        SQLBlockExecutionOptions(checkpointLimits: try TransactionCheckpointLimits(maximumEntries: 10_000,
            maximumDepth: 32, maximumRetainedBytes: 4 * 1_024 * 1_024,
            maximumWorkUnits: 1_000_000, maximumReadPageRows: 128))
    }

    private func insert(_ id: String) -> QueryStatement {
        .insert(InsertQuery(target: TableRef(EntityMutationFixture.persistableType),
            columns: ["id", "title"], source: .values([[.string(id), .string("value")]])))
    }

    private func select(_ id: String) -> QueryStatement {
        .select(SelectQuery(projection: [.column("title")], source: .table(TableRef(EntityMutationFixture.persistableType)),
            filter: .equal(.col("id"), .string(id))))
    }

    private func scalar(_ result: SQLBlockResult, _ index: Int) throws -> String {
        let value = try #require(result.results.indices.contains(index) ? result.results[index] : nil)
        guard case .rows(let response) = value,
              case .primitive(.string(let text)) = response.rows.first?.values.first else {
            throw SQLBlockExecutionError.unsupportedStatement
        }
        return text
    }
}
