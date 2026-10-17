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
        await #expect(throws: DatabaseEntityMutationError.self) {
            try await context.executeSQLBlock(SQLBlock(steps: [
                .statement(insert("before", "uncommitted")), .savepoint("point"),
                .statement(insert("seed", "duplicate")), .rollbackTo("point"),
                .statement(insert("after", "must-not-run"))
            ]), options: options())
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
        // FIXME(INCOMPLETE_IMPLEMENTATION): S14 must implement RETURNING before this public semantic statement can succeed.
        await #expect(throws: DatabaseEntityStatementMutationError.unsupportedStatement(
            "DELETE RETURNING is not representable by mutation effects")) {
            try await context.executeStatement(parse("DELETE FROM \"BlockItem\" RETURNING id"), options: options())
        }
        #expect(try await context.model(for: "one", as: BlockItem.self)?.title == "single")
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
