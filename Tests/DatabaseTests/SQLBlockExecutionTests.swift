import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import QueryAST
import StorageKit
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import Database
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("Public semantic SQL blocks", .timeLimit(.minutes(1)))
struct SQLBlockExecutionTests {
    @Persistable
    struct BlockItem {
        #Directory<BlockItem>("sql-block-items")
        var id: String = ""
        var title: String = ""
    }

    @Test("One block commits writes and preserves ordered typed and empty rows")
    func readOwnWritesAndOrderedResults() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let result = try await context.executeSQLBlock(SQLBlock(steps: [
            .statement(insert("one", "before")),
            .statement(parse("SELECT title AS value, title AS value FROM \"BlockItem\"")),
            .statement(parse("SELECT title AS value FROM \"BlockItem\" WHERE FALSE")),
            .statement(update("after")),
            .statement(parse("SELECT title FROM \"BlockItem\""))
        ]), options: options())
        #expect(result.results.count == 5)
        expectCommand(result.results[0], .insert, 1)
        let duplicate = try rows(result.results[1])
        #expect(duplicate.columns.map(\.name) == ["value", "value"])
        #expect(duplicate.rows.first?.values == [.primitive(.string("before")), .primitive(.string("before"))])
        let empty = try rows(result.results[2])
        #expect(empty.rows.isEmpty)
        #expect(empty.columns.count == 1)
        #expect(empty.columns[0].type == .primitive(.string, isArray: false))
        expectCommand(result.results[3], .update, 1)
        #expect(try rows(result.results[4]).rows.first?.values == [.primitive(.string("after"))])
        #expect(try await context.model(for: "one", as: BlockItem.self)?.title == "after")
    }

    @Test("Repeated names restore their nearest checkpoint while earlier results survive")
    func nestedNamesAndRetainedResults() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let read = try parse("SELECT title FROM \"BlockItem\"")
        let result = try await context.executeSQLBlock(SQLBlock(steps: [
            .statement(insert("one", "first")), .savepoint("same"),
            .statement(update("second")), .savepoint("same"),
            .statement(update("third")), .statement(read),
            .rollbackTo("same"), .statement(read), .release("same"),
            .rollbackTo("same"), .statement(read), .release("same")
        ]), options: options())
        #expect(result.results.count == 12)
        #expect(try rows(result.results[5]).rows.first?.values == [.primitive(.string("third"))])
        #expect(try rows(result.results[7]).rows.first?.values == [.primitive(.string("second"))])
        #expect(try rows(result.results[10]).rows.first?.values == [.primitive(.string("first"))])
        expectCommand(result.results[6], .rollbackTo, nil)
        #expect(try await context.model(for: "one", as: BlockItem.self)?.title == "first")
    }

    @Test("The first statement error aborts all writes and never executes later recovery")
    func firstStatementFailureStopsBlock() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        _ = try await context.executeStatement(insert("seed", "original"), options: options())
        await #expect {
            try await context.executeSQLBlock(SQLBlock(steps: [
                .statement(insert("before", "uncommitted")), .savepoint("point"),
                .statement(insert("seed", "duplicate")), .rollbackTo("point"),
                .statement(insert("after", "must-not-run"))
            ]), options: options())
        } throws: { error in
            guard case DatabaseContextError.preconditionFailed(let identity, .notExists, _) = error else {
                return false
            }
            return identity.entity == BlockItem.persistableType
        }
        #expect(try await context.model(for: "before", as: BlockItem.self) == nil)
        #expect(try await context.model(for: "after", as: BlockItem.self) == nil)
        #expect(try await context.model(for: "seed", as: BlockItem.self)?.title == "original")
    }

    @Test("Released and unknown checkpoint errors roll back the complete request")
    func invalidCheckpointRollsBack() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        for suffix in [[SQLBlockStep.rollbackTo("missing")], [.savepoint("missing"), .release("missing"), .rollbackTo("missing")]] {
            await #expect(throws: SQLBlockExecutionError.unknownCheckpoint("missing")) {
                try await context.executeSQLBlock(SQLBlock(steps: [.statement(insert("one", "temporary"))] + suffix), options: options())
            }
            #expect(try await context.model(for: "one", as: BlockItem.self) == nil)
        }
    }

    @Test("Logical catalog aliases preserve the physical write and missing names never use physical fallback")
    func catalogMappingAdmission() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let old = container.sqlCatalog
        let relation = try #require(old.relations.first)
        let renamed = try SQLRelation(id: relation.id, namespaceID: relation.namespaceID,
            name: "renamed", entityName: relation.entityName, columns: relation.columns,
            primaryKeyColumnIDs: relation.primaryKeyColumnIDs,
            primaryKeyConstraint: relation.primaryKeyConstraint, constraints: relation.constraints)
        _ = try await container.publishSQLCatalog(SQLCatalog(namespaces: old.namespaces, relations: [renamed]),
            expectedGeneration: container.schemaGeneration, idempotencyKey: "block-catalog",
            authorization: TestBaseEnvironment.authorization)
        let context = container.testBaseContext()
        let result = try await context.executeStatement(insert("one", "alias", table: "renamed"), options: options())
        expectCommand(result, .insert, 1)
        await #expect(throws: SQLCatalogBindingError.missingRelation(namespace: nil, name: "BlockItem")) {
            try await context.executeStatement(insert("one", "physical"), options: options())
        }
        #expect(try await context.model(for: "one", as: BlockItem.self)?.title == "alias")
    }

    @Test("Parameters and cumulative output budgets reject a block without partial effects")
    func cumulativeAdmissionAndBudgets() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let parameterized = try parse("SELECT $1")
        await #expect(throws: QueryParameterBindingError.self) {
            try await context.executeSQLBlock(SQLBlock(steps: [
                .statement(insert("missing", "not-saved")), .statement(parameterized)
            ]), options: options())
        }
        let parameters = [QueryParameter(position: 1, value: .primitive(.int32(7)), type: .sql(.integer))]
        let value = try await context.executeStatement(parameterized, parameters: parameters, options: options())
        #expect(try rows(value).rows.first?.values == [.primitive(.int32(7))])
        await #expect(throws: DatabaseWorkLimitError.self) {
            try await context.executeSQLBlock(SQLBlock(steps: [
                .statement(insert("limited", "not-saved")),
                .statement(parse("SELECT 1")), .statement(parse("SELECT 2"))
            ]), options: options(budget: ExecutionBudget(maximumRows: 2)))
        }
        #expect(try await context.model(for: "missing", as: BlockItem.self) == nil)
        #expect(try await context.model(for: "limited", as: BlockItem.self) == nil)
    }

    @Test("The singleton facade reports rows and mutation command counts")
    func singletonStatementFacade() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        expectCommand(try await context.executeStatement(insert("one", "single"), options: options()), .insert, 1)
        let row = try await context.executeStatement(parse("SELECT title FROM public.\"BlockItem\""), options: options())
        #expect(try rows(row).rows.first?.values == [.primitive(.string("single"))])
        let deleted = try rows(await context.executeStatement(
            parse("DELETE FROM \"BlockItem\" RETURNING id"), options: options()))
        #expect(deleted.affectedRows == 1)
        #expect(deleted.columns.map(\.name) == ["id"])
        #expect(deleted.rows.first?.values == [.primitive(.string("one"))])
        #expect(try await context.model(for: "one", as: BlockItem.self) == nil)
    }

    @Test("Zero output rows remain valid for reads and reject DML before effects")
    func zeroRowBudget() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let zero = try options(budget: ExecutionBudget(maximumRows: 0))
        let result = try await context.executeSQLBlock(SQLBlock(steps: [
            .savepoint("read"), .statement(parse("SELECT 1 AS value WHERE FALSE")),
            .rollbackTo("read"), .release("read")
        ]), options: zero)
        #expect(try rows(result.results[1]).rows.isEmpty)
        await #expect(throws: DatabaseWorkLimitError.maximumRows(
            stage: .mutationPlanning, consumed: 0, requested: 1, maximum: 0)) {
            try await context.executeStatement(insert("zero", "forbidden"), options: zero)
        }
        #expect(try await context.model(for: "zero", as: BlockItem.self) == nil)
    }

    @Test("Distinct statement inserts each report and consume exactly one affected row")
    func distinctStatementEffectsRespectExactBudget() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let result = try await context.executeSQLBlock(SQLBlock(steps: [
            .statement(insert("first", "one")), .statement(insert("second", "two"))
        ]), options: options(budget: ExecutionBudget(maximumRows: 2)))
        #expect(result.results.count == 2)
        expectCommand(result.results[0], .insert, 1)
        expectCommand(result.results[1], .insert, 1)
        #expect(try await context.model(for: "first", as: BlockItem.self)?.title == "one")
        #expect(try await context.model(for: "second", as: BlockItem.self)?.title == "two")
    }

    @Test("A delete reports its statement effect even when the transaction net effect vanishes")
    func statementDeleteSurvivesNetEffectCoalescing() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let result = try await context.executeSQLBlock(SQLBlock(steps: [
            .statement(insert("temporary", "value")),
            .statement(parse("DELETE FROM \"BlockItem\" WHERE id = 'temporary'"))
        ]), options: options(budget: ExecutionBudget(maximumRows: 2)))
        #expect(result.results.count == 2)
        expectCommand(result.results[0], .insert, 1)
        expectCommand(result.results[1], .delete, 1)
        #expect(try await context.model(for: "temporary", as: BlockItem.self) == nil)
    }

    @Test("One hundred VALUES rows retain ordered RETURNING and committed values")
    func bulkInsertRetainsOrderedResults() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let values = (0..<100).map { "('bulk-\($0)', 'value-\($0)')" }.joined(separator: ",")
        let result = try await context.executeSQLBlock(
            "INSERT INTO \"BlockItem\" (id, title) VALUES " + values + " RETURNING id, title",
            options: options())
        let returned = try rows(#require(result.results.first))
        #expect(returned.affectedRows == 100)
        #expect(returned.columns.map(\.name) == ["id", "title"])
        #expect(returned.rows.map(\.values) == (0..<100).map {
            [.primitive(.string("bulk-\($0)")), .primitive(.string("value-\($0)"))]
        })
        for index in [0, 50, 99] {
            #expect(try await context.model(for: "bulk-\(index)", as: BlockItem.self)?.title == "value-\(index)")
        }
        let count = try await context.executeSQL("SELECT COUNT(*) FROM \"BlockItem\"")
        #expect(count.rows.map(\.values) == [[.primitive(.int64(100))]])
    }

    @Test("Late bulk value failure and row budget rejection preserve whole-statement rollback")
    func bulkInsertFailureRollsBack() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let values = (0..<99).map { "('bulk-\($0)', 'value-\($0)')" }.joined(separator: ",")
        do {
            _ = try await context.executeSQLBlock(
                "INSERT INTO \"BlockItem\" (id, title) VALUES " + values
                    + ",('bulk-99', CAST(1 / 0 AS TEXT))", options: options())
            Issue.record("The last value must fail without publishing earlier rows")
        } catch let failure as SQLExecutionFailure {
            guard failure.diagnostic.sqlState == "22012" else { throw failure }
        }
        let complete = values + ",('bulk-99', 'value-99')"
        await #expect {
            try await context.executeSQLBlock(
                "INSERT INTO \"BlockItem\" (id, title) VALUES " + complete,
                options: options(budget: ExecutionBudget(maximumRows: 99)))
        } throws: { error in
            guard let failure = error as? SQLExecutionFailure,
                  let cause = failure.underlyingError as? DatabaseEntityMutationError,
                  case .changeLimitExceeded(let actual, let maximum) = cause else { return false }
            return actual == 100 && maximum == 99
        }
        let empty = try await context.executeSQL("SELECT COUNT(*) FROM \"BlockItem\"")
        #expect(empty.rows.map(\.values) == [[.primitive(.int64(0))]])
        let recovered = try await context.executeSQLBlock(
            "INSERT INTO \"BlockItem\" (id, title) VALUES " + complete,
            options: options(budget: ExecutionBudget(maximumRows: 100)))
        #expect(recovered.results.count == 1)
        expectCommand(recovered.results[0], .insert, 100)
    }

    @Test("Wide CTE metadata and nested expression preparation preserve atomic writes")
    func wideStatementPreparation() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let definitions = (0..<100).map { "c\($0) AS (SELECT \($0) AS n)" }.joined(separator: ",")
        let subqueries = (0..<100).map { "(SELECT \($0))" }.joined(separator: ",")
        let prefix = "WITH " + definitions + " INSERT INTO \"BlockItem\" (id, title) VALUES "
        let result = try await context.executeSQLBlock(prefix + "('wide', CAST(cardinality(ARRAY[" + subqueries + "]) AS TEXT))",
            options: options())
        expectCommand(try #require(result.results.first), .insert, 1)
        #expect(try await context.model(for: "wide", as: BlockItem.self)?.title == "100")
        do {
            _ = try await context.executeSQLBlock(prefix + "('failed', CAST(1 / 0 AS TEXT))", options: options())
            Issue.record("A prepared CTE list must not turn a late value failure into success")
        } catch let failure as SQLExecutionFailure {
            guard failure.diagnostic.sqlState == "22012" else { throw failure }
        }
        #expect(try await context.model(for: "failed", as: BlockItem.self) == nil)
        #expect(try await context.model(for: "wide", as: BlockItem.self)?.title == "100")
    }

    @Test("A later CTE name does not hide an earlier physical snapshot")
    func preparationRetainsLexicalDeclarations() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        _ = try await context.executeStatement(insert("seed", "original"), options: options())
        let result = try await context.executeSQLBlock("""
            WITH source AS (SELECT title FROM "BlockItem" WHERE id = 'seed'),
                 "BlockItem" AS (SELECT 'shadowed' AS title)
            INSERT INTO "BlockItem" (id, title) SELECT 'copy', title FROM source
            """, options: options())
        expectCommand(try #require(result.results.first), .insert, 1)
        #expect(try await context.model(for: "copy", as: BlockItem.self)?.title == "original")
        let eager = try await context.executeSQLBlock("""
            WITH copied AS (INSERT INTO "BlockItem" (id, title)
                    VALUES ('eager', (SELECT title FROM "BlockItem" WHERE id = 'seed')) RETURNING title),
                 "BlockItem" AS (SELECT 'shadowed' AS title)
            INSERT INTO "BlockItem" (id, title) SELECT 'eager-copy', title FROM copied
            """, options: options())
        expectCommand(try #require(eager.results.first), .insert, 1)
        #expect(try await context.model(for: "eager", as: BlockItem.self)?.title == "original")
        #expect(try await context.model(for: "eager-copy", as: BlockItem.self)?.title == "original")
    }

    private func makeContainer() async throws -> DBContainer {
        let schema = try Schema(entities: [BlockItem.schemaEntity])
        #if SQLITE
        let engine: any StorageEngine = try SQLiteStorageEngine(configuration: .inMemory)
        #else
        let engine: any StorageEngine = InMemoryEngine()
        #endif
        return try await DBContainer.open(for: schema, configuration: .testing(storageEngine: engine),
            runtimeConfiguration: DatabaseFrameworkRuntime.configuration(
                executionIdentity: .init(identifier: "sql-block-tests", revision: 1),
                entityRuntimes: [DatabaseFrameworkRuntime.entity(BlockItem.self)]), security: .testingDisabled)
    }

    private func options(budget: ExecutionBudget = ExecutionBudget()) throws -> SQLBlockExecutionOptions {
        SQLBlockExecutionOptions(checkpointLimits: try TransactionCheckpointLimits(maximumEntries: 10_000,
            maximumDepth: 32, maximumRetainedBytes: 4 * 1_024 * 1_024,
            maximumWorkUnits: 1_000_000, maximumReadPageRows: 128), budget: budget)
    }

    private func parse(_ sql: String) throws -> QueryStatement { try SQLParser().parse(sql) }
    private func insert(_ id: String, _ title: String, table: String = "BlockItem") -> QueryStatement {
        .insert(InsertQuery(target: TableRef(table), columns: ["id", "title"], source: .values([[.string(id), .string(title)]])))
    }
    private func update(_ title: String) -> QueryStatement {
        .update(UpdateQuery(target: TableRef("BlockItem"), assignments: [Assignment(column: "title", value: .string(title))]))
    }
    private func rows(_ result: SQLStatementResult) throws -> QueryResponse {
        guard case .rows(let rows) = result else { throw SQLBlockTestFailure.expectedRows }
        return rows
    }
    private func expectCommand(_ result: SQLStatementResult, _ expected: SQLCommandKind, _ count: UInt64?) {
        guard case .command(let kind, let actual) = result else { Issue.record("Expected a command outcome"); return }
        #expect(kind == expected)
        #expect(actual == count)
    }
}

private enum SQLBlockTestFailure: Error { case expectedRows }
