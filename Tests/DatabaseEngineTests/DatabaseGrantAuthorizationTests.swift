import DatabaseKit
import DatabaseRuntime
import StorageKit
import TestSupport
import Testing
@testable import DatabaseEngine

@Suite("Persisted Grant authorization")
struct DatabaseGrantAuthorizationTests {
    @Persistable
    struct SecuredItem {
        var id: String
        var value: String
    }

    @Test("Principal and role Grants form an exact bit union")
    func principalAndRoleGrantsFormExactUnion() async throws {
        let container = try await makeContainer()
        try await container.grantTestBaseAccess(
            to: .principal("reader-writer"),
            access: .read
        )
        try await container.grantTestBaseAccess(
            to: .principalRole("writer"),
            access: .write
        )
        let context = context(
            container: container,
            principal: Principal(
                identifier: "reader-writer",
                roles: ["writer"]
            )
        )

        try await context.withTransaction(
            requiredAccess: .read.union(.write),
            configuration: .readOnly
        ) { _ in () }
        await #expect(throws: DatabaseGrantAuthorizationError.self) {
            try await context.withTransaction(
                requiredAccess: .administer,
                configuration: .readOnly
            ) { _ in () }
        }
    }

    @Test("Access bits remain independent")
    func accessBitsRemainIndependent() async throws {
        let container = try await makeContainer()
        let cases: [(String, Security.Access, Security.Access)] = [
            ("reader", .read, .write),
            ("writer", .write, .administer),
            ("administrator", .administer, .read),
        ]

        for (identifier, granted, denied) in cases {
            try await container.grantTestBaseAccess(
                to: .principal(identifier),
                access: granted
            )
            let context = context(
                container: container,
                principal: Principal(identifier: identifier)
            )
            try await context.withTransaction(
                requiredAccess: granted,
                configuration: .readOnly
            ) { _ in () }
            await #expect(throws: DatabaseGrantAuthorizationError.self) {
                try await context.withTransaction(
                    requiredAccess: denied,
                    configuration: .readOnly
                ) { _ in () }
            }
        }
    }

    @Test("Database Grants do not inherit into a Base")
    func databaseGrantDoesNotInheritIntoBase() async throws {
        let container = try await makeContainer()
        let identifier = "database-only"
        try await container.grantTestDatabaseAccess(
            to: .principal(identifier),
            access: .all
        )
        let context = context(
            container: container,
            principal: Principal(identifier: identifier)
        )

        do {
            try await context.withTransaction(
                requiredAccess: .read,
                configuration: .readOnly
            ) { _ in () }
            Issue.record("Expected the Base-local Grant check to fail")
        } catch let error as DatabaseGrantAuthorizationError {
            #expect(
                error == .denied(
                    resource: .base(try TestBaseEnvironment.id()),
                    required: .read
                )
            )
        }
    }

    private func makeContainer() async throws -> DBContainer {
        try await DBContainer.open(
            for: try Schema(
                entities: [try SecuredItem.schemaEntity],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(SecuredItem.self),
                ]
            ),
            security: .testingDisabled
        )
    }

    private func context(
        container: DBContainer,
        principal: Principal
    ) -> DatabaseContext {
        do {
            return container.session(authorization: .authenticated(principal))
                .base(try TestBaseEnvironment.id())
                .newContext()
        } catch {
            preconditionFailure("The fixed test Base identity must be valid")
        }
    }
}
