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

@Suite("S16 file-backed metadata views", .timeLimit(.minutes(1)), .heartbeat)
struct S16SQLMetadataViewSQLiteTests {
    @Test("Captured virtual sources survive shadowing, own-DDL rollback and authoritative file reopen")
    func virtualBindingsRollbackAndReopen() async throws {
        let database = try SQLiteTestDatabase(prefix: "s16-metadata-view"); defer { database.remove() }
        let first = try await open(database); defer { await first.shutdown() }
        _ = try await run(first, [.statement(table("items", columns: ["a", "b"]))])
        let query = SelectQuery(projection: [.column("column_name")], source: .table(TableRef("columns")),
            filter: .equal(.col("table_name"), .string("items")))
        _ = try await run(first, [.statement(.ddl(.createView(SQLCreateView(
            name: TableRef(schema: "public", table: "metadata_columns"), query: query))))],
            searchPath: SQLSearchPath(entries: [.schema("information_schema")]))
        _ = try await run(first, [.statement(table("columns", columns: ["shadow"]))])
        let read = QueryStatement.select(SelectQuery(projection: [.wildcard()], source: .table(TableRef("metadata_columns"))))
        let result = try await run(first, [.savepoint("before"),
            .statement(.ddl(.alterTable(SQLAlterTable(name: TableRef("items"), actions: [
                .addColumn(SQLColumnDefinition(name: "temporary", type: .integer), ifNotExists: false)])))),
            .statement(read), .rollbackTo("before"), .statement(read)])
        #expect(Set(try rows(result, 2).rows.map(\.values)) == Set(["a", "b", "temporary"].map { [QueryValue.primitive(.string($0))] }))
        #expect(Set(try rows(result, 4).rows.map(\.values)) == Set(["a", "b"].map { [QueryValue.primitive(.string($0))] }))
        let published = first.sqlCatalog
        let captured = try #require(published.views.first?.definition.bindings.first)
        #expect(captured.kind == .informationSchema)
        #expect(captured.relationID == "columns")
        await first.shutdown()
        let reopened = try await reopen(database); defer { await reopened.shutdown() }
        #expect(reopened.sqlCatalog == published)
        let restored = try rows(await run(reopened, [.statement(read)]), 0)
        #expect(restored.columns.map(\.name) == ["column_name"])
        #expect(Set(restored.rows.map(\.values)) == Set(["a", "b"].map { [QueryValue.primitive(.string($0))] }))
    }

    private func open(_ database: SQLiteTestDatabase) async throws -> DBContainer {
        let schema = try Schema(entities: [])
        return try await DBContainer.open(testing: schema,
            configuration: .testing(storageEngine: SQLiteStorageEngine(configuration: .file(database.path))),
            runtimeConfiguration: DatabaseFrameworkRuntime.configuration(
                executionIdentity: .init(identifier: "s16-metadata-views-sqlite", revision: 1), schema: schema), security: .testingDisabled)
    }
    private func reopen(_ database: SQLiteTestDatabase) async throws -> DBContainer {
        try await DBContainer.openRestoringSchema(configuration: .testing(storageEngine:
            SQLiteStorageEngine(configuration: .file(database.path))), security: .testingDisabled) { schema in
            try DatabaseFrameworkRuntime.configuration(executionIdentity: .init(identifier: "s16-metadata-views-sqlite", revision: 1), schema: schema)
        }
    }
    private func table(_ name: String, columns: [String]) -> QueryStatement {
        .ddl(.createTable(SQLCreateTable(name: TableRef(name), source: .columns(
            columns.map { SQLColumnDefinition(name: $0, type: .integer) }, constraints: []))))
    }
    private func run(_ container: DBContainer, _ steps: [SQLBlockStep], searchPath: SQLSearchPath = .default) async throws -> SQLBlockResult {
        try await SQLBlockExecutor(context: container.testBaseContext()).execute(SQLBlock(steps: steps),
            options: SQLBlockExecutionOptions(checkpointLimits: TransactionCheckpointLimits(maximumEntries: 10_000,
                maximumDepth: 16, maximumRetainedBytes: 16_777_216, maximumWorkUnits: 1_000_000, maximumReadPageRows: 128), searchPath: searchPath))
    }
    private func rows(_ result: SQLBlockResult, _ index: Int) throws -> QueryResponse {
        guard case .rows(let rows) = result.results[index] else { throw SQLBlockExecutionError.unsupportedStatement }
        return rows
    }
}
#endif
