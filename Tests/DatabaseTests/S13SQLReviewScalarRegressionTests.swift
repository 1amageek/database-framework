import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import Database
@_spi(Testing) @_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("S13 explicit catalog search-path review regressions", .timeLimit(.minutes(1)))
struct S13SQLReviewScalarSessionTests {
    @Test("Including implicit schemas preserves an explicit pg_catalog position")
    func explicitCatalogKeepsSearchPriority() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let path = try SQLSearchPath(entries: [.schema("missing"), .schema("public"),
            .schema("pg_catalog"), .schema("public"), .schema("pg_catalog")])
        let result = try await container.testBaseContext().executeSQL(
            "SELECT CURRENT_SCHEMA, current_schemas(FALSE), current_schemas(TRUE)", searchPath: path)
        let row = try #require(result.rows.first)
        #expect(row.values[0] == .primitive(.string("public")))
        try expectSchemas(row.values[1], ["public", "pg_catalog"])
        try expectSchemas(row.values[2], ["public", "pg_catalog"])
    }

    @Test("The current-user expansion preserves its explicit pg_catalog position")
    func currentUserCatalogKeepsSearchPriority() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        #if MultiBase
        try await container.grantTestDatabaseAccess(to: .principal("pg_catalog"), access: .all)
        try await container.grantTestBaseAccess(to: .principal("pg_catalog"), access: .all)
        #endif
        let context = container.testBaseContext(authorization: .authenticated(Principal(identifier: "pg_catalog")))
        let path = try SQLSearchPath(entries: [.schema("public"), .currentUser])
        let result = try await context.executeSQL(
            "SELECT CURRENT_SCHEMA, current_schemas(FALSE), current_schemas(TRUE)", searchPath: path)
        let row = try #require(result.rows.first)
        #expect(row.values[0] == .primitive(.string("public")))
        try expectSchemas(row.values[1], ["public", "pg_catalog"])
        try expectSchemas(row.values[2], ["public", "pg_catalog"])
    }

    private func makeContainer() async throws -> DBContainer {
        let schema = try Schema(entities: [Schema.Entity(name: "ReviewSessionItem", identifierType: .string,
            fields: [FieldSchema(name: "id", fieldNumber: 1, type: .string)],
            directoryComponents: [.staticPath("s13-review-session")])])
        return try await DBContainer.open(for: schema,
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: DatabaseFrameworkRuntime.configuration(
                executionIdentity: .init(identifier: "s13-review-session", revision: 1), schema: schema),
            security: .testingDisabled)
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
