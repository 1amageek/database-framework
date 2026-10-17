import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import Database
@_spi(Testing) @_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("S13 admitted SQL session values", .timeLimit(.minutes(1)))
struct S13SQLSessionTests {
    @Test("Namespace functions use existing search-path entries in order and include implicit schemas only when requested")
    func admittedSearchPath() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        try await addNamespaces(["test-runner", "reports"], to: container)
        let path = try SQLSearchPath(entries: [.schema("missing"), .currentUser, .schema("reports"),
            .schema("public"), .currentUser, .schema("reports")])
        let response = try await container.testBaseContext().executeSQL(
            "SELECT CURRENT_SCHEMA, current_schemas(FALSE), current_schemas(TRUE)", searchPath: path)
        let row = try #require(response.rows.first)
        #expect(row.values[0] == .primitive(.string("test-runner")))
        try expectSchemas(row.values[1], ["test-runner", "reports", "public"])
        try expectSchemas(row.values[2], ["pg_catalog", "test-runner", "reports", "public"])
    }

    @Test("An empty effective path produces NULL current schema and a typed empty explicit schema array")
    func emptySearchPath() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let path = try SQLSearchPath(entries: [.schema("missing")])
        let response = try await container.testBaseContext().executeSQL(
            "SELECT current_schema(), current_schemas(FALSE), current_schemas(TRUE)", searchPath: path)
        let row = try #require(response.rows.first)
        #expect(row.values[0].isNull)
        #expect(response.columns[0].isNullable)
        #expect(response.columns[1].type == .sql(.array(.text)))
        try expectSchemas(row.values[1], [])
        try expectSchemas(row.values[2], ["pg_catalog"])
    }

    @Test("Schema names preserve byte identity when deduplicating canonically equivalent Unicode")
    func exactNamespaceIdentity() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let composed = "\u{00E9}"
        let decomposed = "e\u{0301}"
        try await addNamespaces([composed, decomposed], to: container)
        let path = try SQLSearchPath(entries: [.schema(composed), .schema(decomposed), .schema(composed)])
        let response = try await container.testBaseContext().executeSQL(
            "SELECT current_schemas(FALSE)", searchPath: path)
        try expectSchemas(try #require(response.rows.first).values[0], [composed, decomposed])
    }

    @Test("Current and session users identify the authenticated context rather than a configured or shared default")
    func authenticatedIdentity() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        #if MultiBase
        try await container.grantTestDatabaseAccess(to: .principal("alice"), access: .all)
        try await container.grantTestBaseAccess(to: .principal("alice"), access: .all)
        #endif
        let alice = container.testBaseContext(authorization: .authenticated(
            Principal(identifier: "alice", roles: ["test-runner", "admin"])))
        let aliceResponse = try await alice.executeSQL("SELECT CURRENT_USER, SESSION_USER, CURRENT_ROLE")
        #expect(aliceResponse.rows.map(\.values) == [[.primitive(.string("alice")),
            .primitive(.string("alice")), .primitive(.string("alice"))]])
        let original = try await container.testBaseContext().executeSQL("SELECT CURRENT_USER, SESSION_USER")
        #expect(original.rows.map(\.values) == [[.primitive(.string("test-runner")), .primitive(.string("test-runner"))]])
    }

    @Test("Current database uses explicit SQL identity and rejects a debugging-name substitute")
    func explicitDatabaseIdentity() async throws {
        let named = try await makeContainer(databaseName: "application-db", debugName: "unrelated-debug-name")
        defer { await named.shutdown() }
        let result = try await named.testBaseContext().executeSQL("SELECT current_database(), CURRENT_CATALOG")
        #expect(result.rows.map(\.values) == [[.primitive(.string("application-db")), .primitive(.string("application-db"))]])
        let unnamed = try await makeContainer(debugName: "must-not-become-sql-identity")
        defer { await unnamed.shutdown() }
        await #expect(throws: SQLSessionEvaluationError.missingDatabaseIdentity) {
            try await unnamed.testBaseContext().executeSQL("SELECT current_database()")
        }
    }

    #if !MultiBase
    @Test("Anonymous sessions report a typed identity failure instead of inventing a user")
    func anonymousIdentityFailure() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext(authorization: .anonymous)
        for sql in ["SELECT CURRENT_USER", "SELECT SESSION_USER"] {
            await #expect(throws: SQLSessionEvaluationError.unauthenticatedPrincipal) {
                try await context.executeSQL(sql)
            }
        }
    }
    #endif

    private func makeContainer(databaseName: String? = nil, debugName: String? = nil) async throws -> DBContainer {
        let entity = try Schema.Entity(name: "SessionItem", identifierType: .string,
            fields: [FieldSchema(name: "id", fieldNumber: 1, type: .string)],
            directoryComponents: [.staticPath("s13-session-items")])
        let schema = try Schema(entities: [entity])
        let configuration: DBConfiguration
        #if MultiBase
        configuration = DBConfiguration(testingName: debugName,
            storageTopology: try .testing(storageEngine: InMemoryEngine()),
            monotonicClock: TestProcessMonotonicClock(), wallClock: FixedTestWallClock(),
            sqlDatabaseName: databaseName, testingBaseID: try TestBaseEnvironment.id(),
            testingPrincipal: Principal(identifier: "test-runner", roles: ["test-runner", "admin"]))
        #else
        configuration = try DBConfiguration(name: debugName, storageEngine: InMemoryEngine(),
            databaseRootPath: testingDatabaseRootPath(), monotonicClock: TestProcessMonotonicClock(),
            wallClock: FixedTestWallClock(), sqlDatabaseName: databaseName)
        #endif
        return try await DBContainer.open(for: schema, configuration: configuration,
            runtimeConfiguration: DatabaseFrameworkRuntime.configuration(
                executionIdentity: .init(identifier: "s13-sql-session", revision: 1), schema: schema),
            security: .testingDisabled)
    }

    private func addNamespaces(_ names: [String], to container: DBContainer) async throws {
        let current = container.sqlCatalog
        let namespaces = try current.namespaces + names.enumerated().map { index, name in
            try SQLNamespace(id: "session-namespace-\(index)", name: name)
        }
        let catalog = try SQLCatalog(namespaces: namespaces, relations: current.relations,
            sequences: current.sequences)
        _ = try await container.publishSQLCatalog(catalog, expectedGeneration: container.schemaGeneration,
            idempotencyKey: "session-namespaces", authorization: TestBaseEnvironment.authorization)
    }

    private func expectSchemas(_ value: QueryValue, _ expected: [String]) throws {
        guard case .array(let array) = value else {
            Issue.record("Expected a SQL text array")
            return
        }
        #expect(array.elementType == .text)
        #expect(array.elements.count == expected.count)
        for (value, name) in zip(array.elements, expected) {
            guard case .primitive(.string(let actual)) = value else {
                Issue.record("Expected a schema name")
                continue
            }
            #expect(actual.utf8.elementsEqual(name.utf8))
        }
    }
}
