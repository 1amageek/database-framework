import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import QueryAST
import StorageKit
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import Database
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("Ordinary SQL EXPLAIN execution", .timeLimit(.minutes(1)))
struct S16SQLExplainExecutionTests {
    @Persistable
    struct ExplainItem {
        #Directory<ExplainItem>("sql-explain-items")
        var id: String = ""
        var title: String = ""
    }

    @Test("Non-ANALYZE validates DML and volatile SELECT without executing them")
    func planningHasNoEffects() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let inserted = try SQLParser().parse("INSERT INTO \"ExplainItem\" (id, title) VALUES ('one', 'planned')")
        let result = try await context.executeSQLBlock(SQLBlock(steps: [.explain(.init(statement: inserted))]), options: options())
        #expect(try text(result.results[0]).contains("Insert"))
        #expect(try await context.model(for: "one", as: ExplainItem.self) == nil)
        let division = try SQLParser().parse("SELECT 1 / 0")
        _ = try await context.executeSQLBlock(SQLBlock(steps: [.explain(.init(statement: division))]), options: options())
        await #expect(throws: (any Error).self) {
            try await context.executeSQLBlock(SQLBlock(steps: [.statement(division)]), options: options())
        }
    }

    @Test("ANALYZE executes DML once and reports measured affected rows")
    func analyzeCommitsOnce() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let insert = try SQLParser().parse("INSERT INTO \"ExplainItem\" (id, title) VALUES ('once', 'committed')")
        let result = try await context.executeSQLBlock(SQLBlock(steps: [
            .explain(.init(statement: insert, options: .init(analyze: true)))]), options: options())
        let plan = try text(result.results[0])
        #expect(plan.contains("Actual Rows: 1"))
        #expect(plan.contains("Work Units:"))
        #expect(plan.contains("Execution Time (ms):"))
        #expect(try await context.model(for: "once", as: ExplainItem.self)?.title == "committed")
        let query = try await context.executeSQL("SELECT id FROM \"ExplainItem\"")
        #expect(query.rows.count == 1)
    }

    @Test("ANALYZE participates in checkpoint rollback")
    func analyzeRollback() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let insert = try SQLParser().parse("INSERT INTO \"ExplainItem\" (id, title) VALUES ('temporary', 'rolled back')")
        let result = try await context.executeSQLBlock(SQLBlock(steps: [.savepoint("before"),
            .explain(.init(statement: insert, options: .init(analyze: true))), .rollbackTo("before")]), options: options())
        #expect(try text(result.results[1]).contains("Actual Rows: 1"))
        #expect(try await context.model(for: "temporary", as: ExplainItem.self) == nil)
    }

    @Test("Non-ANALYZE rejects invalid bound fields and parameter types")
    func invalidBindingsFail() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let missing = try SQLParser().parse("SELECT missing FROM \"ExplainItem\"")
        await #expect(throws: (any Error).self) {
            try await context.executeSQLBlock(SQLBlock(steps: [.explain(.init(statement: missing))]), options: options())
        }
        let invalid = try SQLParser().parse("INSERT INTO \"ExplainItem\" (missing) VALUES (1)")
        await #expect(throws: (any Error).self) {
            try await context.executeSQLBlock(SQLBlock(steps: [.explain(.init(statement: invalid))]), options: options())
        }
    }

    @Test("Aggregate and join plans expose the selected relational operators")
    func selectedRelationalBranches() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let aggregate = try SQLParser().parse("SELECT count(*) FROM \"ExplainItem\"")
        let join = try SQLParser().parse("SELECT a.id FROM \"ExplainItem\" AS a JOIN \"ExplainItem\" AS b ON a.id = b.id")
        let result = try await context.executeSQLBlock(SQLBlock(steps: [
            .explain(.init(statement: aggregate)), .explain(.init(statement: join))]), options: options())
        #expect(try text(result.results[0]).contains("Aggregate"))
        #expect(try text(result.results[1]).contains("Join"))
    }

    @Test("Catalog index selection matches the ordinary reader before and after CREATE INDEX")
    func actualIndexChoice() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let query = try SQLParser().parse("SELECT title FROM \"ExplainItem\"")
        let before = try await context.executeSQLBlock(SQLBlock(steps: [
            .explain(.init(statement: query))]), options: options())
        #expect(try text(before.results[0]).contains("Model Read"))
        let create = try SQLParser().parse("CREATE INDEX explain_by_title ON \"ExplainItem\" (title)")
        _ = try await context.executeStatement(create, options: options())
        let after = try await context.executeSQLBlock(SQLBlock(steps: [
            .explain(.init(statement: query, options: .init(analyze: true)))]), options: options())
        let plan = try text(after.results[0])
        #expect(plan.contains("Index Scan"))
        #expect(plan.contains("Index: explain_by_title"))
        #expect(plan.contains("Actual Rows: 0"))
    }

    @Test("EXPLAIN preserves a view's captured relation after base rename with and without a catalog index")
    func viewCapturedResolutionAfterRename() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        _ = try await context.executeSQLBlock(SQLParser().parseBlock("""
            CREATE TABLE items (id integer);
            INSERT INTO items VALUES (7);
            CREATE VIEW v AS SELECT id FROM items;
            ALTER TABLE items RENAME TO renamed;
            """), options: options())
        let query = try SQLParser().parse("SELECT id FROM v")
        let ordinary = try await context.executeSQL("SELECT id FROM v")
        #expect(ordinary.rows.map(\.values) == [[.primitive(.int32(7))]])
        let beforeIndex = try await context.executeSQLBlock(SQLBlock(steps: [
            .explain(.init(statement: query))]), options: options())
        let modelPlan = try text(beforeIndex.results[0])
        #expect(modelPlan.contains("View"))
        #expect(modelPlan.contains("Model Read"))
        #expect(!modelPlan.contains("Index Scan"))

        _ = try await context.executeStatement(SQLParser().parse("CREATE INDEX renamed_by_id ON renamed (id)"), options: options())
        let indexedOrdinary = try await context.executeSQL("SELECT id FROM v")
        #expect(indexedOrdinary.rows == ordinary.rows)
        let afterIndex = try await context.executeSQLBlock(SQLBlock(steps: [
            .explain(.init(statement: query, options: .init(analyze: true)))]), options: options())
        let indexPlan = try text(afterIndex.results[0])
        #expect(indexPlan.contains("View"))
        #expect(indexPlan.contains("Index Scan"))
        #expect(indexPlan.contains("Index: renamed_by_id"))
        #expect(indexPlan.contains("Actual Rows: 1"))
    }

    @Test("Non-ANALYZE does not consume selected sequence functions")
    func sequencePlanningHasNoEffects() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        _ = try await context.executeSQLBlock(SQLParser().parseBlock("CREATE SEQUENCE explain_counter"), options: options())
        let sequence = try SQLParser().parse("SELECT nextval('explain_counter')")
        _ = try await context.executeSQLBlock(SQLBlock(steps: [
            .explain(.init(statement: sequence))]), options: options())
        let actual = try await context.executeSQL("SELECT nextval('explain_counter')")
        #expect(actual.rows.first?.values == [.primitive(.int64(1))])
    }

    private func makeContainer() async throws -> DBContainer {
        let schema = try Schema(entities: [ExplainItem.schemaEntity])
        #if SQLITE
        let engine: any StorageEngine = try SQLiteStorageEngine(configuration: .inMemory)
        #else
        let engine: any StorageEngine = InMemoryEngine()
        #endif
        return try await DBContainer.open(for: schema, configuration: .testing(storageEngine: engine),
            runtimeConfiguration: DatabaseFrameworkRuntime.configuration(
                executionIdentity: .init(identifier: "sql-explain-tests", revision: 1),
                entityRuntimes: [DatabaseFrameworkRuntime.entity(ExplainItem.self)]), security: .testingDisabled)
    }

    private func options() throws -> SQLBlockExecutionOptions {
        SQLBlockExecutionOptions(checkpointLimits: try TransactionCheckpointLimits(maximumEntries: 10_000,
            maximumDepth: 32, maximumRetainedBytes: 4 * 1_024 * 1_024,
            maximumWorkUnits: 1_000_000, maximumReadPageRows: 128))
    }

    private func text(_ outcome: SQLStatementResult) throws -> String {
        guard case .rows(let rows) = outcome else { throw ExplainExecutionTestFailure.expectedPlan }
        var text = ""
        for row in rows.rows {
            guard case .primitive(.string(let value))? = row.values.first else {
                throw ExplainExecutionTestFailure.expectedPlan
            }
            if !text.isEmpty { text.append("\n") }
            text.append(value)
        }
        return text
    }
}

private enum ExplainExecutionTestFailure: Error { case expectedPlan }
