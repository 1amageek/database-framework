#if SQLITE
import Database
import DatabaseEmbedded
@_spi(Testing) import DatabaseEngine
import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import QueryAST
import TestSupport
import Testing

@Suite("Named SQLite application access", .timeLimit(.minutes(1)))
struct NamedDatabaseAccessTests {
    private var identity: DatabaseExecutionRuntimeIdentity { .init(identifier: "named-sqlite", revision: 1) }

    @Test("Typed SQL and bound IR remain isolated across databases and generations")
    func databaseIsolation() async throws {
        let primaryFile = try SQLiteTestDatabase(prefix: "named-primary"), analyticsFile = try SQLiteTestDatabase(prefix: "named-analytics")
        defer { primaryFile.remove(); analyticsFile.remove() }
        let primary = try await open(primaryFile), analytics = try await open(analyticsFile)
        defer { await primary.shutdown(); await analytics.shutdown() }
        let directory = DatabaseDirectory()
        try await directory.configure(databases: [
            "primary": .borrowingEmbedded(context: primary.testBaseContext(), maximumConcurrentExecutions: 2),
            "analytics": .borrowingEmbedded(context: analytics.testBaseContext(), maximumConcurrentExecutions: 2)])
        defer { await directory.shutdown() }
        let first = try directory.database("primary"), second = try directory.database("analytics")
        for database in [first, second] {
            _ = try await database.execute("CREATE TABLE \"FluentExecutionPerson\"(id text, name text, age integer, \"isActive\" boolean, nickname text)")
        }
        let tuples = (1...100).map { "('\($0)', 'value\($0)', \($0), true, NULL)" }.joined(separator: ",")
        _ = try await first.execute(SQL(text: "INSERT INTO \"FluentExecutionPerson\" VALUES " + tuples))
        _ = try await second.execute("INSERT INTO \"FluentExecutionPerson\" VALUES ('a', 'analytics', 20, true, NULL)")
        let pure = try FluentExecutionPerson.select(#field(\FluentExecutionPerson.name), #field(\FluentExecutionPerson.age))
            .where(#field(\FluentExecutionPerson.age) > Int32(0)).orderBy(#field(\FluentExecutionPerson.age)).limit(3)
        let bound = try first.entity(FluentExecutionPerson.self).select(#field(\FluentExecutionPerson.name), #field(\FluentExecutionPerson.age))
            .where(#field(\FluentExecutionPerson.age) > Int32(0)).orderBy(#field(\FluentExecutionPerson.age)).limit(3)
        let sql = try await first.execute("SELECT name, age FROM \"FluentExecutionPerson\" WHERE age > 0 ORDER BY age LIMIT 3")
        #expect(bound.definition == pure.definition)
        #expect(try await values(bound.execute().rows) == values(first.execute(pure).rows))
        #expect(try await values(sql.rows) == values(first.execute(pure).rows))
        #expect(try await values(second.execute(pure).rows) == [[.primitive(.string("analytics")), .primitive(.int32(20))]])
        await directory.shutdown()
        #expect(try await values(sql.rows).count == 3)
        try await directory.configure(databases: ["primary": .borrowingEmbedded(context: analytics.testBaseContext(), maximumConcurrentExecutions: 2)])
        await #expect(throws: AccessError.self) { try await bound.execute() }
        #expect(try await values(directory.database("primary").execute(pure).rows) == [[.primitive(.string("analytics")), .primitive(.int32(20))]])
    }

    @Test("Server scopes sharing one container retain independent original authority")
    func requestAuthority() async throws {
        let file = try SQLiteTestDatabase(prefix: "named-authority")
        defer { file.remove() }
        let schema = try Schema(entities: [])
        let runtime = try DatabaseFrameworkRuntime.configuration(executionIdentity: identity, schema: schema,
            schemaDrivenAuthorizationPolicyFactory: { entity in AuthorizationPolicyHandler(schemaDriven: entity) { _, _ in true } },
            sqlAuthorizationPolicyFactory: { entity, namespace, name in
                guard namespace.name == "public", name == "items" else { return nil }
                return AuthorizationPolicyHandler(schemaDriven: entity) { _, context in context.principal?.roles.contains("reader") == true }
            })
        let container = try await DBContainer.open(for: schema, configuration: .file(file.path), runtimeConfiguration: runtime, security: .enabled())
        defer { await container.shutdown() }
        let allowed = DatabaseDirectory(), denied = DatabaseDirectory()
        defer { await allowed.shutdown(); await denied.shutdown() }
        let authorized = AuthorizationContext.authenticated(Principal(identifier: "authorized", roles: ["admin", "reader"]))
        let unauthorized = AuthorizationContext.authenticated(Principal(identifier: "unauthorized", roles: ["admin"]))
        #if MultiBase
        let base = try TestBaseEnvironment.id()
        for identifier in ["authorized", "unauthorized"] {
            try await container.grantDatabaseAccessForTesting(.init(subject: .principal(identifier), resource: .database, access: .all),
                authorization: TestBaseEnvironment.authorization)
            try await container.grantBaseAccessForTesting(.init(subject: .principal(identifier), resource: .base(base), access: .all),
                authorization: TestBaseEnvironment.authorization)
        }
        #endif
        try await allowed.configure(databases: ["primary": .borrowingEmbedded(context: container.testBaseContext(authorization: authorized), maximumConcurrentExecutions: 2)])
        try await denied.configure(databases: ["primary": .borrowingEmbedded(context: container.testBaseContext(authorization: unauthorized), maximumConcurrentExecutions: 2)])
        let writer = try allowed.database("primary"), reader = try denied.database("primary")
        _ = try await writer.execute("CREATE TABLE items(id integer); INSERT INTO items VALUES (1)")
        for input in [QueryInput.sql("SELECT id FROM items"), .statement(try SQLParser().parse("SELECT id FROM items"))] {
            await #expect(throws: (any Error).self) { try await reader.execute(input) }
        }
        await #expect(throws: (any Error).self) { try await reader.entity("items").select("id").execute() }
        #expect(try await values(writer.entity("items").select("id").execute().rows) == [[.primitive(.int32(1))]])
        await denied.shutdown()
        #expect(try await values(writer.execute("SELECT id FROM items").rows).count == 1)
    }

    @Test("Named execution preserves per-call budgets, first-error rollback and owned shutdown")
    func budgetsAndOwnedClosure() async throws {
        let file = try SQLiteTestDatabase(prefix: "named-budget")
        defer { file.remove() }
        let container = try await open(file)
        defer { await container.shutdown() }
        let context = container.testBaseContext(), directory = DatabaseDirectory()
        try await directory.configure(databases: ["primary": .owningEmbedded(maximumConcurrentExecutions: 2,
            makeContainer: { container }, context: { _ in context })])
        defer { await directory.shutdown() }
        let database = try directory.database("primary")
        _ = try await database.execute("CREATE TABLE items(id integer); INSERT INTO items VALUES(1)")
        await #expect(throws: SQLExecutionFailure.self) {
            try await database.execute("INSERT INTO items VALUES(2)", options: .init(budget: .init(maximumWorkUnits: 0)))
        }
        await #expect(throws: SQLExecutionFailure.self) {
            try await database.execute("INSERT INTO items VALUES(3); SELECT missing; INSERT INTO items VALUES(4)")
        }
        #expect(try await values(database.execute("SELECT id FROM items").rows) == [[.primitive(.int32(1))]])
        await directory.shutdown()
        let closed = try await context.openConnection()
        defer { await closed.close() }
        await #expect(throws: (any Error).self) { try await closed.execute("SELECT 1") }
    }

    private func open(_ file: SQLiteTestDatabase) async throws -> DBContainer {
        try await DBContainer.open(for: Schema(entities: []), configuration: .file(file.path),
            runtimeConfiguration: DatabaseFrameworkRuntime.configuration(executionIdentity: identity, entityRuntimes: []), security: .testingDisabled)
    }
    private func values<Rows: QueryRows>(_ rows: Rows) async throws -> [[QueryValue]] {
        var output: [[QueryValue]] = []
        for try await row in rows { output.append(row.values) }
        return output
    }
}
#endif
