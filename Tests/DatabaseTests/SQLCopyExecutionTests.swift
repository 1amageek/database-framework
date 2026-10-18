import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import QueryAST
import StorageKit
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import Database
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Persistable
private struct SecuredCopyItem: SecurityPolicy {
    #Directory<SecuredCopyItem>("secured-copy-items")
    var id: String = ""
    @Restricted(read: .roles(["copy-secret-reader"]), write: .roles(["copy-secret-writer"]))
    var secret: String = ""
    static func permitsRead(of resource: borrowing Self, in context: borrowing AuthorizationContext) -> Bool {
        context.principal?.roles.contains("copy-reader") == true
    }
    static func permitsQuery(_ query: borrowing SecurityQuery, in context: borrowing AuthorizationContext) -> Bool {
        context.principal?.roles.contains("copy-reader") == true
    }
    static func permitsCreate(_ resource: borrowing Self, in context: borrowing AuthorizationContext) -> Bool {
        context.principal?.roles.contains("copy-writer") == true
    }
    static func permitsUpdate(from resource: borrowing Self, to updated: borrowing Self, in context: borrowing AuthorizationContext) -> Bool { false }
    static func permitsDelete(_ resource: borrowing Self, in context: borrowing AuthorizationContext) -> Bool { false }
}

@Suite("Transactional request-contained COPY", .timeLimit(.minutes(1)))
struct SQLCopyExecutionTests {
    @Test("CSV import uses own DDL, ordered columns, contextual casts and defaults")
    func csvRoundTrip() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let csv = try SQLCopyOptions(format: .csv, header: true)
        let result = try await run([
            .statement(parse("CREATE TABLE items (id integer, content text, extra integer DEFAULT 17)")),
            .copy(SQLCopyCommand(target: .table(TableRef("items"), columns: ["content", "id"]), direction: .from,
                options: csv, data: ByteString(utf8: "content,id\r\n\"line\nnext\",1\r\n\"\",2\r\n,3\r\n"))),
            .copy(SQLCopyCommand(target: .query(select("SELECT id, content, extra FROM items ORDER BY id")),
                direction: .to, options: csv))], context)
        expectCommand(result.results[1], count: 3)
        guard case .copy(let copied) = result.results[2] else { Issue.record("Expected typed COPY bytes"); return }
        #expect(copied.affectedRows == 3)
        #expect(copied.bytes == ByteString(utf8: "id,content,extra\n1,\"line\nnext\",17\n2,\"\",17\n3,,17\n"))
        let readback = try await context.executeSQL("SELECT id, content, extra FROM items ORDER BY id")
        #expect(readback.rows.map(\.values) == [
            [.primitive(.int32(1)), .primitive(.string("line\nnext")), .primitive(.int32(17))],
            [.primitive(.int32(2)), .primitive(.string("")), .primitive(.int32(17))],
            [.primitive(.int32(3)), .primitive(.null), .primitive(.int32(17))]])
    }

    @Test("TEXT import and export distinguish NULL, empty and escaped controls")
    func textRoundTrip() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let input = "1\t\\N\n2\t\n3\tline\\nnext\n4\t\\\\N\n"
        let result = try await run([
            .statement(parse("CREATE TABLE items (id integer, content text)")),
            .copy(SQLCopyCommand(target: .table(TableRef("items"), columns: []), direction: .from,
                data: ByteString(utf8: input))),
            .copy(SQLCopyCommand(target: .query(select("SELECT id, content FROM items ORDER BY id")), direction: .to))], context)
        expectCommand(result.results[1], count: 4)
        guard case .copy(let copied) = result.results[2] else { Issue.record("Expected typed COPY output"); return }
        #expect(copied.affectedRows == 4)
        #expect(copied.bytes == ByteString(utf8: input))
    }

    @Test("A late malformed COPY row rolls back its prior rows and prior block statements")
    func lateMalformedRollsBack() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        _ = try await run([.statement(parse("CREATE TABLE items (id integer, content text)")),
            .statement(parse("INSERT INTO items VALUES (9, 'retained')"))], context)
        await #expect(throws: SQLCopyExecutionError.self) {
            try await run([.statement(parse("INSERT INTO items VALUES (8, 'uncommitted')")),
                .copy(SQLCopyCommand(target: .table(TableRef("items"), columns: []), direction: .from,
                    options: SQLCopyOptions(format: .csv), data: ByteString(utf8: "1,good\n2,\"unfinished")))], context)
        }
        let rows = try await context.executeSQL("SELECT id, content FROM items ORDER BY id")
        #expect(rows.rows.map(\.values) == [[.primitive(.int32(9)), .primitive(.string("retained"))]])
    }

    @Test("A late contextual conversion or constraint failure rolls back earlier COPY rows")
    func lateSemanticFailureRollsBack() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        _ = try await run([.statement(parse("CREATE TABLE items (id integer NOT NULL, content text)"))], context)
        for data in ["1\tgood\ninvalid\tbad\n", "1\tgood\n\\N\tbad\n"] {
            await #expect(throws: (any Error).self) {
                try await run([.copy(SQLCopyCommand(target: .table(TableRef("items"), columns: []), direction: .from,
                    data: ByteString(utf8: data)))], context)
            }
            #expect(try await context.executeSQL("SELECT * FROM items").rows.isEmpty)
        }
    }

    @Test("COPY supplies explicit identity values through the ordinary identity mutation policy")
    func identityInput() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let result = try await run([
            .statement(parse("CREATE TABLE items (id integer GENERATED ALWAYS AS IDENTITY, content text)")),
            .copy(SQLCopyCommand(target: .table(TableRef("items"), columns: []), direction: .from,
                data: ByteString(utf8: "42\texplicit\n"))),
            .statement(parse("SELECT id FROM items"))], context)
        expectCommand(result.results[1], count: 1)
        guard case .rows(let rows) = result.results[2] else { Issue.record("Expected selected identity"); return }
        #expect(rows.rows.map(\.values) == [[.primitive(.int32(42))]])
    }

    @Test("Exact row budget succeeds and one-past rejects COPY atomically")
    func rowBudget() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        _ = try await run([.statement(parse("CREATE TABLE items (id integer)"))], context)
        let command = SQLCopyCommand(target: .table(TableRef("items"), columns: []), direction: .from,
            data: ByteString(utf8: "1\n2\n"))
        let result = try await run([.copy(command)], context, budget: ExecutionBudget(maximumRows: 2))
        expectCommand(result.results[0], count: 2)
        await #expect(throws: DatabaseWorkLimitError.self) {
            try await run([.copy(SQLCopyCommand(target: .table(TableRef("items"), columns: []), direction: .from,
                data: ByteString(utf8: "3\n4\n")))], context, budget: ExecutionBudget(maximumRows: 1))
        }
        #expect(try await context.executeSQL("SELECT id FROM items ORDER BY id").rows.map(\.values)
            == [[.primitive(.int32(1))], [.primitive(.int32(2))]])
    }

    @Test("COPY preserves entity and restricted-field read/write policies")
    func securityPolicies() async throws {
        let schema = try Schema(entities: [SecuredCopyItem.schemaEntity])
        let configuration = try DatabaseFrameworkRuntime.configuration(
            executionIdentity: .init(identifier: "sql-copy-security", revision: 1),
            entityRuntimes: [DatabaseFrameworkRuntime.entity(SecuredCopyItem.self)],
            authorizationPolicies: [AuthorizationPolicyHandler(SecuredCopyItem.self)])
        let container = try await DBContainer.open(for: schema,
            configuration: .testing(storageEngine: InMemoryEngine()), runtimeConfiguration: configuration, security: .enabled())
        defer { await container.shutdown() }
        func authorization(_ roles: Set<String>) -> AuthorizationContext {
            .authenticated(Principal(identifier: "test-runner", roles: roles.union(["admin", "test-runner"])))
        }
        let allowed = container.testBaseContext(authorization: authorization([
            "copy-reader", "copy-writer", "copy-secret-reader", "copy-secret-writer"]))
        try allowed.insert(SecuredCopyItem(id: "one", secret: "private")); try await allowed.save()
        let target = SQLCopyTarget.table(TableRef(SecuredCopyItem.persistableType), columns: ["id", "secret"])
        for roles in [Set(["copy-secret-reader"]), Set(["copy-reader"])] {
            let denied = container.testBaseContext(authorization: authorization(roles))
            await #expect(throws: (any Error).self) {
                try await run([.copy(SQLCopyCommand(target: target, direction: .to))], denied)
            }
        }
        for roles in [Set(["copy-secret-writer"]), Set(["copy-writer"])] {
            let denied = container.testBaseContext(authorization: authorization(roles))
            await #expect(throws: (any Error).self) {
                try await run([.copy(SQLCopyCommand(target: target, direction: .from,
                    data: ByteString(utf8: "two\tdenied\n")))], denied)
            }
        }
        let result = try await run([.copy(SQLCopyCommand(target: target, direction: .to))], allowed)
        guard case .copy(let copied) = result.results[0] else { Issue.record("Expected authorized COPY bytes"); return }
        #expect(copied.bytes == ByteString(utf8: "one\tprivate\n"))
        #expect(copied.affectedRows == 1)
    }

    private func makeContainer() async throws -> DBContainer {
        let schema = try Schema(entities: [])
        #if SQLITE
        let engine: any StorageEngine = try SQLiteStorageEngine(configuration: .inMemory)
        #else
        let engine: any StorageEngine = InMemoryEngine()
        #endif
        return try await DBContainer.open(for: schema, configuration: .testing(storageEngine: engine),
            runtimeConfiguration: DatabaseFrameworkRuntime.configuration(
                executionIdentity: .init(identifier: "sql-copy-tests", revision: 1), schema: schema), security: .testingDisabled)
    }
    private func run(_ steps: [SQLBlockStep], _ context: DatabaseContext,
        budget: ExecutionBudget = ExecutionBudget()) async throws -> SQLBlockResult {
        try await context.executeSQLBlock(SQLBlock(steps: steps), options: SQLBlockExecutionOptions(
            checkpointLimits: TransactionCheckpointLimits(maximumEntries: 10_000, maximumDepth: 32,
                maximumRetainedBytes: 16 * 1024 * 1024, maximumWorkUnits: 1_000_000, maximumReadPageRows: 128), budget: budget))
    }
    private func parse(_ sql: String) throws -> QueryStatement { try SQLParser().parse(sql) }
    private func select(_ sql: String) throws -> SelectQuery {
        guard case .select(let query) = try parse(sql) else { throw SQLCopyExecutionError.invalidTarget }
        return query
    }
    private func expectCommand(_ result: SQLStatementResult, count: UInt64) {
        guard case .command(let kind, let affected) = result else { Issue.record("Expected COPY command count"); return }
        #expect(kind == .copy); #expect(affected == count)
    }
}
