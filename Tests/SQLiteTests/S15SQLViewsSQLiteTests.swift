#if SQLITE
import Database
import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import TestHeartbeat
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("S15 file-backed SQLite views", .timeLimit(.minutes(1)), .heartbeat)
struct S15SQLViewsSQLiteTests {
    @Test("Ordinary stable bindings and duplicate materialized values survive file-backed reopen")
    func durableDefinitionsAndRefresh() async throws {
        let database = try SQLiteTestDatabase(prefix: "s15-views-reopen"); defer { database.remove() }
        let first = try await open(database); defer { await first.shutdown() }
        _ = try await run(first, [createTable(), insert([.int(2), .int(2)]),
            .ddl(.createView(SQLCreateView(name: TableRef("v"),
                query: SelectQuery(projection: [.wildcard()], source: .table(TableRef("items")))))),
            .ddl(.createMaterializedView(SQLCreateMaterializedView(name: TableRef("mv"),
                query: SelectQuery(projection: [.wildcard()], source: .table(TableRef("v"))))))])
        _ = try await run(first, [.ddl(.alterTable(SQLAlterTable(name: TableRef("items"), actions: [
            .renameColumn(name: "value", to: "renamed"), .renameTable(to: "renamed_items")])))])
        await first.shutdown()
        let restored = try await reopen(database); defer { await restored.shutdown() }
        #expect(try await select(restored, "v").columns.map(\.name) == ["value"])
        #expect(try await select(restored, "mv").rows.map(\.values) == [[.primitive(.int32(2))], [.primitive(.int32(2))]])
        _ = try await run(restored, [.insert(InsertQuery(target: TableRef("renamed_items"), columns: ["renamed"], source: .values([[.int(3)]]))),
            .ddl(.refreshMaterializedView(name: TableRef("mv"), withData: true))])
        let values = try await select(restored, "mv").rows.map(\.values)
        #expect(values.count == 3)
        #expect(values.filter { $0 == [.primitive(.int32(2))] }.count == 2)
        #expect(values.contains([.primitive(.int32(3))]))
    }

    @Test("Failed refresh and checkpoint rollback preserve the last complete materialization across reopen")
    func refreshFailureAndSavepoint() async throws {
        let database = try SQLiteTestDatabase(prefix: "s15-views-rollback"); defer { database.remove() }
        let first = try await open(database); defer { await first.shutdown() }
        _ = try await run(first, [createTable(), insert([.int(2)]),
            .ddl(.createMaterializedView(SQLCreateMaterializedView(name: TableRef("mv"),
                query: SelectQuery(projection: [.expression(.divide(.int(10), .col("value")), alias: "quotient")],
                    source: .table(TableRef("items"))))))])
        let before = try await select(first, "mv").rows.map(\.values)
        _ = try await run(first, [insert([.int(0)])])
        await #expect(throws: Error.self) {
            _ = try await run(first, [.ddl(.refreshMaterializedView(name: TableRef("mv"), withData: true))])
        }
        #expect(try await select(first, "mv").rows.map(\.values) == before)
        let checkpoint = try SQLBlock(steps: [.savepoint("before"),
            .statement(.ddl(.refreshMaterializedView(name: TableRef("mv"), withData: false))),
            .rollbackTo("before"), .statement(.select(SelectQuery(projection: [.wildcard()], source: .table(TableRef("mv")))))])
        let result = try await SQLBlockExecutor(context: first.testBaseContext()).execute(checkpoint, options: options())
        guard case .rows(let rows) = result.results[3] else { throw SQLBlockExecutionError.unsupportedStatement }
        #expect(rows.rows.map(\.values) == before)
        await first.shutdown()
        let restored = try await reopen(database); defer { await restored.shutdown() }
        #expect(try await select(restored, "mv").rows.map(\.values) == before)
    }

    private func open(_ database: SQLiteTestDatabase) async throws -> DBContainer {
        let schema = try Schema(entities: [])
        return try await DBContainer.open(testing: schema,
            configuration: .testing(storageEngine: SQLiteStorageEngine(configuration: .file(database.path))),
            runtimeConfiguration: DatabaseFrameworkRuntime.configuration(
                executionIdentity: .init(identifier: "s15-sqlite-views", revision: 1), schema: schema), security: .testingDisabled)
    }
    private func reopen(_ database: SQLiteTestDatabase) async throws -> DBContainer {
        try await DBContainer.openRestoringSchema(configuration: .testing(storageEngine:
            SQLiteStorageEngine(configuration: .file(database.path))), security: .testingDisabled) { schema in
            try DatabaseFrameworkRuntime.configuration(executionIdentity: .init(identifier: "s15-sqlite-views", revision: 1), schema: schema)
        }
    }
    private func createTable() -> QueryStatement {
        .ddl(.createTable(SQLCreateTable(name: TableRef("items"),
            source: .columns([SQLColumnDefinition(name: "value", type: .integer)], constraints: []))))
    }
    private func insert(_ values: [Expression]) -> QueryStatement {
        .insert(InsertQuery(target: TableRef("items"), columns: ["value"], source: .values(values.map { [$0] })))
    }
    private func run(_ container: DBContainer, _ statements: [QueryStatement]) async throws -> SQLBlockResult {
        try await SQLBlockExecutor(context: container.testBaseContext()).execute(
            SQLBlock(steps: statements.map { .statement($0) }), options: options())
    }
    private func select(_ container: DBContainer, _ table: String) async throws -> QueryResponse {
        let result = try await run(container, [.select(SelectQuery(projection: [.wildcard()], source: .table(TableRef(table))))])
        guard case .rows(let rows) = result.results[0] else { throw SQLBlockExecutionError.unsupportedStatement }; return rows
    }
    private func options() throws -> SQLBlockExecutionOptions {
        SQLBlockExecutionOptions(checkpointLimits: try TransactionCheckpointLimits(maximumEntries: 10_000,
            maximumDepth: 16, maximumRetainedBytes: 16_777_216, maximumWorkUnits: 1_000_000, maximumReadPageRows: 128))
    }
}
#endif
