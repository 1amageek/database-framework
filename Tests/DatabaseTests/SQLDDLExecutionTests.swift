import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import QueryAST
import StorageKit
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import Database
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("Transactional SQL DDL", .timeLimit(.minutes(1)))
struct SQLDDLExecutionTests {
    @Test("Logical renames and checkpoint rollback preserve physical IDs and data")
    func logicalNamesAndCheckpoint() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        _ = try await run([.ddl(.createTable(table("items", columns: [column("value", .integer)]))),
            parse("INSERT INTO items VALUES (7)")], context)
        let original = try #require(container.sqlCatalog.relations.first)
        let result = try await context.executeSQLBlock(SQLBlock(steps: [
            .savepoint("before"), .statement(.ddl(.alterTable(SQLAlterTable(name: TableRef("items"), actions: [
                .renameTable(to: "renamed"), .renameColumn(name: "value", to: "renamed_value")])))),
            .statement(parse("SELECT renamed_value FROM renamed")), .rollbackTo("before"),
            .statement(parse("SELECT value FROM items"))]), options: options())
        #expect(try values(result.results[2]) == [[.primitive(.int32(7))]])
        #expect(try values(result.results[4]) == [[.primitive(.int32(7))]])
        #expect(container.sqlCatalog.relations.first?.id == original.id)
        #expect(container.sqlCatalog.relations.first?.entityName == original.entityName)
        #expect(container.sqlCatalog.relations.first?.columns.first?.fieldNumber == original.columns.first?.fieldNumber)
    }

    @Test("IF NOT EXISTS skips a mismatched table and a failing CTAS query")
    func ifNotExistsSkipsDefinition() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        _ = try await run([.ddl(.createTable(table("items", columns: [column("value", .integer)])))], context)
        let old = container.sqlCatalog
        let duplicate = try SQLCreateTable(name: TableRef("items"), source: .query(
            select("SELECT 1 / 0 AS impossible"), columns: nil, withData: true), ifNotExists: true)
        _ = try await run([.ddl(.createTable(duplicate))], context)
        #expect(container.sqlCatalog == old)
    }

    @Test("ADD defaults, TYPE USING and late conversion failure preserve atomic data")
    func backfillConversionAndRollback() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        _ = try await run([.ddl(.createTable(table("items", columns: [column("value", .integer)]))),
            parse("INSERT INTO items VALUES (4), (8)"),
            .ddl(.alterTable(SQLAlterTable(name: TableRef("items"), actions: [
                .addColumn(column("added", .integer, defaultExpression: .int(3)), ifNotExists: false)]))),
            .ddl(.alterTable(SQLAlterTable(name: TableRef("items"), actions: [
                .alterColumnType(name: "value", type: .bigint, using: .add(.col("value"), .col("added")))])))], context)
        #expect(try await context.executeSQL("SELECT value, added FROM items ORDER BY value").rows.map(\.values)
            == [[.primitive(.int64(7)), .primitive(.int32(3))], [.primitive(.int64(11)), .primitive(.int32(3))]])
        let before = container.sqlCatalog
        await #expect(throws: (any Error).self) {
            try await run([.ddl(.alterTable(SQLAlterTable(name: TableRef("items"), actions: [
                .addColumn(column("uncommitted", .text), ifNotExists: false),
                .alterColumnType(name: "value", type: .integer, using: .divide(.int(1), .int(0)))])))], context)
        }
        #expect(container.sqlCatalog == before)
        #expect(try await context.executeSQL("SELECT value FROM items ORDER BY value").rows.map(\.values)
            == [[.primitive(.int64(7))], [.primitive(.int64(11))]])
        _ = try await run([.ddl(.createTable(table("pair", columns: [column("a", .integer), column("b", .integer)]))),
            parse("INSERT INTO pair VALUES (2, 5)"),
            .ddl(.alterTable(SQLAlterTable(name: TableRef("pair"), actions: [
                .alterColumnType(name: "a", type: .bigint, using: .col("b")),
                .alterColumnType(name: "b", type: .bigint, using: .col("a"))])))], context)
        #expect(try await context.executeSQL("SELECT a, b FROM pair").rows.map(\.values)
            == [[.primitive(.int64(5)), .primitive(.int64(2))]])
        _ = try await run([.ddl(.createSequence(SQLCreateSequence(name: TableRef("backfill_counter")))),
            .ddl(.alterTable(SQLAlterTable(name: TableRef("items"), actions: [
                .addColumn(column("allocated", .bigint, defaultExpression: .function(FunctionCall(name: "nextval", arguments: [.literal(.string("backfill_counter"))]))), ifNotExists: false)])))], context)
        #expect(try await context.executeSQL("SELECT allocated FROM items ORDER BY allocated").rows.map(\.values)
            == [[.primitive(.int64(1))], [.primitive(.int64(2))]])
        for (name, policy) in [("serial_backfill", SQLColumnDefinitionPolicy.serial),
                               ("identity_backfill", .identity(generation: .byDefault, options: SQLSequenceOptions()))] {
            _ = try await run([.ddl(.createTable(table(name, columns: [column("value", .integer)]))),
                parse("INSERT INTO \(name) VALUES (7), (8)")], context)
            let beforeAddition = container.sqlCatalog
            let addition = QueryStatement.ddl(.alterTable(SQLAlterTable(name: TableRef(name), actions: [
                .addColumn(SQLColumnDefinition(name: "n", type: .integer, policy: policy), ifNotExists: false)])))
            let rolledBack = try await context.executeSQLBlock(SQLBlock(steps: [
                .savepoint("before-sequence-column"), .statement(addition),
                .statement(parse("SELECT n FROM \(name) ORDER BY n")), .rollbackTo("before-sequence-column")]), options: options())
            #expect(try values(rolledBack.results[2]) == [[.primitive(.int32(1))], [.primitive(.int32(2))]])
            #expect(container.sqlCatalog == beforeAddition)
            #expect(try await context.executeSQL("SELECT value FROM \(name) ORDER BY value").rows.map(\.values)
                == [[.primitive(.int32(7))], [.primitive(.int32(8))]])
            _ = try await run([addition, parse("INSERT INTO \(name)(value) VALUES (9)")], context)
            #expect(try await context.executeSQL("SELECT n FROM \(name) ORDER BY n").rows.map(\.values)
                == [[.primitive(.int32(3))], [.primitive(.int32(4))], [.primitive(.int32(5))]])
        }
    }

    @Test("TYPE USING cannot turn an incompatible original DEFAULT into a valid default")
    func usingDoesNotTransformDefault() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        _ = try await run([.ddl(.createTable(table("items", columns: [column("value", .text, defaultExpression: .literal(.string("invalid")))]))),
            parse("INSERT INTO items VALUES ('5')")], context)
        await #expect(throws: (any Error).self) {
            try await run([.ddl(.alterTable(SQLAlterTable(name: TableRef("items"), actions: [
                .alterColumnType(name: "value", type: .integer, using: .cast(.col("value"), targetType: .integer))])))], context)
        }
        #expect(try await context.executeSQL("SELECT value FROM items").rows.map(\.values) == [[.primitive(.string("5"))]])
    }

    @Test("A dropped maximum field number is never reused by another column")
    func fieldNumberHighWaterMark() async throws {
        #expect(try SQLDDLExecutor.nextFieldNumber(after: 1) == 2)
        #expect(throws: SQLDDLExecutionError.invalidDefinition("field number exhausted")) {
            try SQLDDLExecutor.nextFieldNumber(after: Int.max)
        }
        if let maximumFieldNumber = Int(exactly: UInt32.max) {
            #expect(throws: SQLDDLExecutionError.invalidDefinition("field number exhausted")) {
                try SQLDDLExecutor.nextFieldNumber(after: maximumFieldNumber)
            }
        }
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        _ = try await run([.ddl(.createTable(table("items", columns: [column("a", .integer), column("b", .integer)])))], context)
        let oldNumber = try #require(container.sqlCatalog.relations.first?.columns.last?.fieldNumber)
        _ = try await run([.ddl(.alterTable(SQLAlterTable(name: TableRef("items"), actions: [
            .dropColumn(name: "b", ifExists: false, behavior: .restrict),
            .addColumn(column("c", .integer), ifNotExists: false)])))], context)
        #expect(try #require(container.sqlCatalog.relations.first?.columns.last?.fieldNumber) > oldNumber)
    }

    @Test("CASCADE removes a foreign key while keeping its referring table and rows")
    func dropCascadePreservesChildTable() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let parent = SQLCreateTable(name: TableRef("parent"), source: .columns([column("key", .integer)], constraints: [
            SQLTableConstraint(kind: .primaryKey(columns: ["key"]))]))
        let child = SQLCreateTable(name: TableRef("child"), source: .columns([column("parent_key", .integer)], constraints: [
            SQLTableConstraint(kind: .foreignKey(SQLForeignKeyDefinition(columns: ["parent_key"], referencedTable: TableRef("parent"))))]))
        _ = try await run([.ddl(.createTable(parent)), .ddl(.createTable(child)),
            parse("INSERT INTO parent VALUES (1)"), parse("INSERT INTO child VALUES (1)")], context)
        await #expect(throws: SQLDDLExecutionError.self) {
            try await run([.ddl(.dropTable(names: [TableRef("parent")], ifExists: false, behavior: .restrict))], context)
        }
        _ = try await run([.ddl(.dropTable(names: [TableRef("parent")], ifExists: false, behavior: .cascade))], context)
        #expect(container.sqlCatalog.relations.map(\.name) == ["child"])
        #expect(container.sqlCatalog.relations[0].constraints.isEmpty)
        #expect(try await context.executeSQL("SELECT parent_key FROM child").rows.map(\.values) == [[.primitive(.int32(1))]])
        _ = try await run([.ddl(.createTable(SQLCreateTable(name: TableRef("sequence_owner"), source: .columns([
            SQLColumnDefinition(name: "id", type: .integer, policy: .serial), column("keep", .integer)], constraints: [])))),
            .ddl(.createTable(table("sequence_consumer", columns: [column("x", .integer,
                defaultExpression: .function(FunctionCall(name: "nextval", arguments: [.literal(.string("sequence_owner_id_seq"))])))]))),
            parse("INSERT INTO sequence_owner(keep) VALUES (7)"), parse("INSERT INTO sequence_consumer DEFAULT VALUES")], context)
        let beforeDrop = container.sqlCatalog
        let owned = try #require(beforeDrop.sequences.first { $0.name == "sequence_owner_id_seq" })
        await #expect(throws: SQLDDLExecutionError.self) {
            try await run([.ddl(.alterTable(SQLAlterTable(name: TableRef("sequence_owner"), actions: [
                .dropColumn(name: "id", ifExists: false, behavior: .restrict)])))], context)
        }
        #expect(container.sqlCatalog == beforeDrop)
        let cascade = QueryStatement.ddl(.alterTable(SQLAlterTable(name: TableRef("sequence_owner"), actions: [
            .dropColumn(name: "id", ifExists: false, behavior: .cascade)])))
        _ = try await context.executeSQLBlock(SQLBlock(steps: [.savepoint("before-owned-sequence"),
            .statement(cascade), .rollbackTo("before-owned-sequence")]), options: options())
        #expect(container.sqlCatalog == beforeDrop)
        _ = try await run([cascade, parse("INSERT INTO sequence_consumer DEFAULT VALUES")], context)
        #expect(!container.sqlCatalog.sequences.contains { $0.id == owned.id })
        let consumer = try #require(container.sqlCatalog.relations.first { $0.name == "sequence_consumer" })
        #expect(consumer.columns[0].policy == .input(defaultExpression: nil))
        #expect(consumer.columns[0].sequenceBindings.isEmpty)
        #expect(try await context.executeSQL("SELECT keep FROM sequence_owner").rows.map(\.values) == [[.primitive(.int32(7))]])
        #expect(try await context.executeSQL("SELECT x FROM sequence_consumer ORDER BY x NULLS LAST").rows.map(\.values)
            == [[.primitive(.int32(2))], [.primitive(.null)]])
    }

    @Test("Temporary tables shadow unqualified names and disappear after successful block close")
    func temporaryLifetime() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        _ = try await run([.ddl(.createTable(table("items", columns: [column("value", .integer)]))), parse("INSERT INTO items VALUES (1)")], context)
        let temporary = SQLCreateTable(name: TableRef("items"), source: .columns([column("value", .integer)], constraints: []), temporary: true)
        let result = try await run([.ddl(.createTable(temporary)), parse("INSERT INTO items VALUES (2)"),
            parse("SELECT value FROM items"), parse("SELECT value FROM public.items")], context)
        #expect(try values(result.results[2]) == [[.primitive(.int32(2))]])
        #expect(try values(result.results[3]) == [[.primitive(.int32(1))]])
        #expect(try await context.executeSQL("SELECT value FROM items").rows.map(\.values) == [[.primitive(.int32(1))]])
        #expect(!container.sqlCatalog.namespaces.contains { $0.name.hasPrefix("sql_temp_") })
        let beforeFailure = container.sqlCatalog
        await #expect(throws: (any Error).self) {
            try await run([.ddl(.createTable(temporary)), parse("SELECT 1 / 0")], context)
        }
        #expect(container.sqlCatalog == beforeFailure)
        #expect(try await context.executeSQL("SELECT value FROM items").rows.map(\.values) == [[.primitive(.int32(1))]])
    }

    @Test("Zero-row and WITH NO DATA CTAS derive true types without evaluating values")
    func ctasMetadata() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        _ = try await run([.ddl(.createTable(SQLCreateTable(name: TableRef("empty"), source: .query(
            select("SELECT 5::bigint AS value WHERE FALSE"), columns: nil, withData: true)))),
            .ddl(.createTable(SQLCreateTable(name: TableRef("unfilled"), source: .query(
                select("SELECT (1 / 0)::integer AS value"), columns: nil, withData: false)))),
            .ddl(.createTable(SQLCreateTable(name: TableRef("inferred"), source: .query(
                select("SELECT NULL AS nullable_value, 'text' AS text_value"), columns: nil, withData: true))))], context)
        let empty = try await context.executeSQL("SELECT value FROM empty")
        #expect(empty.rows.isEmpty)
        #expect(empty.columns[0].type == .sql(.bigint))
        #expect(try await context.executeSQL("SELECT value FROM unfilled").rows.isEmpty)
        let inferred = try await context.executeSQL("SELECT * FROM inferred")
        #expect(inferred.columns.map(\.type) == [.sql(.text), .sql(.text)])
        #expect(inferred.rows.map(\.values) == [[.primitive(.null), .primitive(.string("text"))]])
    }

    @Test("Literal defaults preserve sequence identity through sequence and schema rename")
    func sequenceRenameBindings() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        _ = try await run([.ddl(.createSchema(name: "owned", ifNotExists: false)),
            .ddl(.createSequence(SQLCreateSequence(name: TableRef(schema: "owned", table: "counter")))),
            .ddl(.createTable(SQLCreateTable(name: TableRef(schema: "owned", table: "items"), source: .columns([
                column("value", .bigint, defaultExpression: .function(FunctionCall(name: "nextval", arguments: [.literal(.string("counter"))])))], constraints: [])))),
            .ddl(.alterSequence(SQLAlterSequence(name: TableRef(schema: "owned", table: "counter"), renameTo: "renamed"))),
            .ddl(.renameSchema(name: "owned", to: "moved")), parse("INSERT INTO moved.items DEFAULT VALUES")], context)
        #expect(try await context.executeSQL("SELECT value FROM moved.items").rows.map(\.values) == [[.primitive(.int64(1))]])
        let binding = try #require(container.sqlCatalog.relations.first?.columns.first?.sequenceBindings.first)
        #expect(binding.originalName == "counter")
        #expect(binding.sequenceID == container.sqlCatalog.sequences.first?.id)
        _ = try await run([.ddl(.createSequence(SQLCreateSequence(name: TableRef("small_counter"), options: SQLSequenceOptions(type: .smallint)))),
            .ddl(.alterSequence(SQLAlterSequence(name: TableRef("small_counter"), options: SQLSequenceOptions(maximum: .some(.none))))),
            parse("SELECT setval('small_counter', 32767)")], context)
        let small = try #require(container.sqlCatalog.sequences.first { $0.name == "small_counter" })
        #expect(small.declaredType == .smallint)
        #expect(small.maximum == Int64(Int16.max))
        await #expect(throws: (any Error).self) { try await context.executeSQL("SELECT nextval('small_counter')") }
    }

    private func column(_ name: String, _ type: DataType, defaultExpression: Expression? = nil) -> SQLColumnDefinition {
        SQLColumnDefinition(name: name, type: type, policy: .input(defaultExpression: defaultExpression))
    }
    private func table(_ name: String, columns: [SQLColumnDefinition]) -> SQLCreateTable {
        SQLCreateTable(name: TableRef(name), source: .columns(columns, constraints: []))
    }
    private func parse(_ sql: String) throws -> QueryStatement { try SQLParser().parse(sql) }
    private func select(_ sql: String) throws -> SelectQuery {
        guard case .select(let query) = try parse(sql) else { throw SQLDDLExecutionError.invalidDefinition("expected SELECT fixture") }
        return query
    }
    private func run(_ statements: [QueryStatement], _ context: DatabaseContext) async throws -> SQLBlockResult {
        try await context.executeSQLBlock(SQLBlock(steps: statements.map(SQLBlockStep.statement)), options: options())
    }
    private func options() throws -> SQLBlockExecutionOptions {
        SQLBlockExecutionOptions(checkpointLimits: try TransactionCheckpointLimits(maximumEntries: 10_000, maximumDepth: 32,
            maximumRetainedBytes: 16 * 1024 * 1024, maximumWorkUnits: 1_000_000, maximumReadPageRows: 128))
    }
    private func values(_ result: SQLStatementResult) throws -> [[QueryValue]] {
        guard case .rows(let rows) = result else { throw SQLDDLExecutionError.invalidDefinition("expected rows fixture") }
        return rows.rows.map(\.values)
    }
    private func makeContainer() async throws -> DBContainer {
        let schema = try Schema(entities: [])
        return try await DBContainer.open(for: schema, configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: DatabaseFrameworkRuntime.configuration(
                executionIdentity: .init(identifier: "sql-ddl-tests", revision: 1), schema: schema), security: .testingDisabled)
    }
}
