#if MultipleBases
import DatabaseKit
import DatabaseRuntime
import StorageKit
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

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

#if MultipleBases
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
#endif

    @Test("Revoking the final administering Grant is atomic and rejected")
    func finalAdministratorCannotBeRevoked() async throws {
        let engine = InMemoryEngine()
        let executor = StorageTransactionExecutor(engine: engine)
        let store = DatabaseGrantStore(resource: .database, root: Subspace())
        let administrator = Security.Grant(
            subject: .principal("administrator"),
            resource: .database,
            access: .all
        )
        try await executor.withTransaction(
            configuration: .batch,
            clock: TestProcessMonotonicClock()
        ) { transaction in
            try await store.installInitial(
                [administrator],
                transaction: transaction
            )
        }

        await #expect(
            throws: DatabaseGrantAuthorizationError.lastAdministrator
        ) {
            try await executor.withTransaction(
                configuration: .batch,
                clock: TestProcessMonotonicClock()
            ) { transaction in
                _ = try await store.revoke(
                    Security.Grant(
                        subject: administrator.subject,
                        resource: .database,
                        access: .administer
                    ),
                    expectedRevision: 1,
                    transaction: transaction
                )
            }
        }

        let persisted = try await executor.withTransaction(
            configuration: .readOnly,
            clock: TestProcessMonotonicClock()
        ) { transaction in
            try await store.direct(
                subject: administrator.subject,
                transaction: transaction
            )
        }
        #expect(persisted.revision == 1)
        #expect(persisted.grants == [administrator])
    }

    @Test("An administering Grant can be revoked when another remains")
    func administratorCanBeRevokedWhenAnotherRemains() async throws {
        let engine = InMemoryEngine()
        let executor = StorageTransactionExecutor(engine: engine)
        let store = DatabaseGrantStore(resource: .database, root: Subspace())
        let primary = Security.Grant(
            subject: .principal("primary"),
            resource: .database,
            access: .all
        )
        let backup = Security.Grant(
            subject: .principalRole("backup"),
            resource: .database,
            access: .administer
        )
        try await executor.withTransaction(
            configuration: .batch,
            clock: TestProcessMonotonicClock()
        ) { transaction in
            try await store.installInitial(
                [primary, backup],
                transaction: transaction
            )
        }

        let revision = try await executor.withTransaction(
            configuration: .batch,
            clock: TestProcessMonotonicClock()
        ) { transaction in
            try await store.revoke(
                Security.Grant(
                    subject: primary.subject,
                    resource: .database,
                    access: .administer
                ),
                expectedRevision: 1,
                transaction: transaction
            )
        }
        #expect(revision == 2)

        let grants = try await executor.withTransaction(
            configuration: .readOnly,
            clock: TestProcessMonotonicClock()
        ) { transaction in
            try await store.direct(transaction: transaction)
        }
        #expect(grants.revision == 2)
        #expect(grants.grants.contains(backup))
        #expect(
            grants.grants.contains(
                Security.Grant(
                    subject: primary.subject,
                    resource: .database,
                    access: [.read, .write]
                )
            )
        )
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
#if MultipleBases
        do {
            return container.session(authorization: .authenticated(principal))
                .base(try TestBaseEnvironment.id())
                .newContext()
        } catch {
            preconditionFailure("The fixed test Base identity must be valid")
        }
#else
        container.newContext(authorization: .authenticated(principal))
#endif
    }
}
#endif
