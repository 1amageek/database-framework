import DatabaseKit
import DatabaseRuntime
import StorageKit
import TestSupport
import Testing
@testable import DatabaseEngine

@Suite("Read authorization capability")
struct ReadAuthorizationCapabilityTests {
    @Persistable
    struct Anchor {
        var id: String = ""
    }

    @Test("Read transaction access rejects persistent mutations")
    func readTransactionRejectsMutations() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let key = ByteString(utf8: "read-authorization-capability")

        try await context.indexQueryContext.withTransaction { transaction in
            #expect(transaction.compaction == nil)
            let value = try await transaction.getValue(
                for: key,
                snapshot: true
            )
            #expect(value == nil)
            #expect(throws: DatabaseReadTransactionError.self) {
                try transaction.setValue(ByteString(utf8: "value"), for: key)
            }
            #expect(throws: DatabaseReadTransactionError.self) {
                try transaction.clear(key: key)
            }
            #expect(throws: DatabaseReadTransactionError.self) {
                try transaction.clearRange(
                    beginKey: key,
                    endKey: ByteString(
                        utf8: "read-authorization-capability~"
                    )
                )
            }
        }
    }

    @Test("Read transaction access rejects namespace mutations")
    func readTransactionRejectsNamespaceMutation() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()

        await #expect(throws: DatabaseReadTransactionError.self) {
            try await context.indexQueryContext.withTransaction { transaction in
                _ = try await container.engine.namespaceResolver.resolveOrCreate(
                    path: ["read-authorization-capability"],
                    transaction: transaction
                )
            }
        }
    }

    private func makeContainer() async throws -> DBContainer {
        try await DBContainer.open(
            testing: try Schema(
                entities: [try Anchor.schemaEntity],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(Anchor.self)
                ]
            ),
            security: .testingDisabled
        )
    }
}
