import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("S16 prepared statement execution", .timeLimit(.minutes(1)))
struct S16PreparationTests {
    @Test("PREPARE validates an unused definition without running its default")
    func preparationHasNoEffects() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        _ = try await run([.prepare(SQLPrepare(name: "pending", statement: .insert(
            InsertQuery(target: TableRef("items"), columns: ["id"], source: .values([[.string("never")]])))))], container)
        let result = try await run([.statement(.select(SelectQuery(projection: [.wildcard()], source: .table(TableRef("items")))))], container)
        guard case .rows(let rows) = result.results[0] else { Issue.record("Expected rows"); return }
        #expect(rows.rows.isEmpty)
    }

    @Test("PREPARE does not consume a selected NEXTVAL")
    func preparationDoesNotAllocateSequence() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        _ = try await run([.sequence(.create(SQLSequence(id: "counter", namespaceID: "public", name: "counter",
            start: 1, increment: 1, minimum: 1, maximum: 100)))], container)
        let query = SelectQuery(projection: [.expression(.function(FunctionCall(name: "nextval",
            arguments: [.string("counter")])))], source: .values([[]]))
        _ = try await run([.prepare(SQLPrepare(name: "next", statement: .select(query)))], container)
        let result = try await run([.statement(.select(query))], container)
        guard case .rows(let rows) = result.results[0] else { Issue.record("Expected rows"); return }
        #expect(rows.rows.first?.values == [.primitive(.int64(1))])
    }

    @Test("PREPARE rejects invalid target names even without EXECUTE")
    func invalidUnusedDefinition() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        await #expect(throws: (any Error).self) {
            try await run([.prepare(SQLPrepare(name: "invalid", statement: .update(UpdateQuery(
                target: TableRef("items"), assignments: [Assignment(column: "missing", value: .int(1))]))))], container)
        }
    }

    @Test("Context infers an undeclared INSERT parameter independently of request values")
    func inferredIndependentParameters() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let definition = SQLPrepare(name: "insert", statement: .insert(InsertQuery(target: TableRef("items"),
            source: .values([[.string("one"), .parameter(.position(1))]]), returning: [.column("value")])))
        let result = try await SQLBlockExecutor(context: container.testBaseContext()).execute(
            SQLBlock(steps: [.prepare(definition), .execute(SQLExecute(name: "insert", arguments: [.int(7)]))]),
            parameters: [QueryParameter(position: 1, value: .primitive(.string("outer")))], options: options())
        guard case .rows(let rows) = result.results[1] else { Issue.record("Expected returning rows"); return }
        #expect(rows.rows.first?.values == [.primitive(.int64(7))])
    }

    @Test("A prepared definition survives rollback to its earlier savepoint")
    func survivesSavepointRollback() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let result = try await run([.savepoint("before"), .prepare(constantDefinition()),
            .rollbackTo("before"), .execute(SQLExecute(name: "constant", arguments: [.int(8)]))], container)
        guard case .rows(let rows) = result.results[3] else { Issue.record("Expected rows"); return }
        #expect(rows.rows.first?.values == [.primitive(.int64(8))])
    }

    @Test("Duplicate, missing and deallocated names fail explicitly")
    func nameFailures() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        await #expect(throws: SQLPreparedExecutionError.duplicateName("constant")) {
            try await run([.prepare(constantDefinition()), .prepare(constantDefinition())], container)
        }
        await #expect(throws: SQLPreparedExecutionError.unknownName("constant")) {
            try await run([.prepare(constantDefinition()), .deallocate("constant"),
                .execute(SQLExecute(name: "constant", arguments: [.int(1)]))], container)
        }
        await #expect(throws: SQLPreparedExecutionError.unknownName("constant")) {
            try await run([.execute(SQLExecute(name: "constant"))], container)
        }
    }

    @Test("Prepared state ends when a request completes")
    func requestLifetime() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        _ = try await run([.prepare(constantDefinition())], container)
        await #expect(throws: SQLPreparedExecutionError.unknownName("constant")) {
            try await run([.execute(SQLExecute(name: "constant", arguments: [.int(1)]))], container)
        }
    }

    @Test("EXECUTE checks exact argument count and canonical types")
    func argumentFailures() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        await #expect(throws: SQLPreparedExecutionError.arity(name: "constant", expected: 1, actual: 0)) {
            try await run([.prepare(constantDefinition()), .execute(SQLExecute(name: "constant"))], container)
        }
        await #expect(throws: SQLPreparedExecutionError.argumentType(position: 1)) {
            try await run([.prepare(constantDefinition()), .execute(SQLExecute(name: "constant", arguments: [.bool(true)]))], container)
        }
        let narrow = SQLPrepare(name: "narrow", parameterTypes: [.smallint], statement: .select(
            SelectQuery(projection: [.expression(.parameter(.position(1)))], source: .values([[]]))))
        await #expect {
            try await run([.prepare(narrow), .execute(SQLExecute(name: "narrow", arguments: [.int(32768)]))], container)
        } throws: { error in
            guard let readError = error as? CanonicalReadError,
                  case .expressionEvaluation(.numericOverflow, occurrence: nil) = readError else { return false }
            return true
        }
    }

    @Test("Changing search path invalidates a previously prepared binding")
    func searchPathStaleness() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        await #expect(throws: SQLPreparedExecutionError.staleBinding("constant")) {
            try await run([.prepare(constantDefinition()),
                .setting(.set(name: .searchPath, value: .string("other, public"), scope: .local)),
                .execute(SQLExecute(name: "constant", arguments: [.int(1)]))], container)
        }
    }

    @Test("EXECUTE applies the selected canonical array assignment conversion")
    func arrayArgumentConversion() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let definition = SQLPrepare(name: "array", parameterTypes: [.array(.bigint)], statement: .select(
            SelectQuery(projection: [.expression(.parameter(.position(1)))], source: .values([[]]))))
        let result = try await run([.prepare(definition), .execute(SQLExecute(name: "array", arguments: [
            .array([.cast(.int(7), targetType: .smallint)])]))], container)
        guard case .rows(let rows) = result.results[1], let first = rows.rows.first,
              case .array(let array) = first.values[0] else { Issue.record("Expected converted array"); return }
        #expect(array.elementType == .bigint)
        #expect(Array(array.elements) == [.primitive(.int64(7))])
    }

    @Test("A schema change invalidates an earlier prepared definition")
    func schemaStaleness() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        await #expect(throws: SQLPreparedExecutionError.staleBinding("constant")) {
            try await run([.prepare(constantDefinition()), .statement(.ddl(.createTable(
                SQLCreateTable(name: TableRef("other"), source: .columns([
                    SQLColumnDefinition(name: "value", type: .integer)], constraints: []))))),
                .execute(SQLExecute(name: "constant", arguments: [.int(1)]))], container)
        }
    }

    @Test("An automatic retry creates a fresh prepared registry")
    func automaticRetry() async throws {
        let engine = ControlledStorageEngine(base: InMemoryEngine())
        let container = try await makeContainer(engine: engine)
        defer { await container.shutdown() }
        engine.control.failNextMutatingCommit(with: .transactionConflict)
        let result = try await SQLBlockExecutor(context: container.testBaseContext()).execute(SQLBlock(steps: [
            .prepare(constantDefinition()), .statement(.insert(InsertQuery(target: TableRef("items"),
                source: .values([[.string("retry"), .int(1)]])))),
            .execute(SQLExecute(name: "constant", arguments: [.int(9)]))]),
            options: SQLBlockExecutionOptions(checkpointLimits: try options().checkpointLimits,
                configuration: TransactionConfiguration(maximumAttempts: 2, maxRetryDelay: 0, initialRetryDelay: 0)))
        guard case .rows(let rows) = result.results[2] else { Issue.record("Expected rows"); return }
        #expect(rows.rows.first?.values == [.primitive(.int64(9))])
    }

    private func constantDefinition() -> SQLPrepare {
        SQLPrepare(name: "constant", parameterTypes: [.bigint], statement: .select(
            SelectQuery(projection: [.expression(.parameter(.position(1)))], source: .values([[]]))))
    }
    private func run(_ steps: [SQLBlockStep], _ container: DBContainer) async throws -> SQLBlockResult {
        try await SQLBlockExecutor(context: container.testBaseContext()).execute(SQLBlock(steps: steps), options: options())
    }
    private func options() throws -> SQLBlockExecutionOptions {
        SQLBlockExecutionOptions(checkpointLimits: try TransactionCheckpointLimits(maximumEntries: 10_000,
            maximumDepth: 32, maximumRetainedBytes: 4 * 1_024 * 1_024,
            maximumWorkUnits: 1_000_000, maximumReadPageRows: 128))
    }
    private func makeContainer(engine: any StorageEngine = InMemoryEngine()) async throws -> DBContainer {
        let fields = [FieldSchema(name: "id", fieldNumber: 1, type: .string),
            FieldSchema(name: "value", fieldNumber: 2, type: .int64, isOptional: true, queryType: .sql(.bigint))]
        let schema = try Schema(entities: [Schema.Entity(name: "PreparedItem", identifierType: .string,
            fields: fields, directoryComponents: [.staticPath("s16-prepared-items")])], version: .init(1, 0, 0))
        let columns = try [SQLColumn(id: "id", name: "id", fieldNumber: 1),
            SQLColumn(id: "value", name: "value", backing: .field(number: 2),
                policy: .input(defaultExpression: .divide(.int(1), .int(0))))]
        let relation = try SQLRelation(id: "items", namespaceID: "public", name: "items",
            entityName: "PreparedItem", columns: columns)
        let container = try await DBContainer.open(testing: schema,
            configuration: .testing(storageEngine: engine),
            runtimeConfiguration: DatabaseFrameworkRuntime.configuration(
                executionIdentity: .init(identifier: "s16-preparation", revision: 1), schema: schema), security: .testingDisabled)
        _ = try await container.publishSQLCatalog(SQLCatalog(namespaces: [SQLNamespace(id: "public", name: "public")],
            relations: [relation]), expectedGeneration: container.schemaGeneration,
            idempotencyKey: "s16-preparation-initial", authorization: TestBaseEnvironment.authorization)
        return container
    }
}
