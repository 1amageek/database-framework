import DatabaseKit
import DatabaseTypes
import StorageKit
import TestSupport
import Testing
@testable import DatabaseEngine

@Persistable(type: "SchemaRegistryPersistenceRecord")
private struct SchemaRegistryPersistenceRecord {
    var id: String = ""
    var name: String
}

@Suite("Schema registry persistence")
struct SchemaRegistryPersistenceTests {
    @Test("Persisting an unchanged schema does not advance storage version")
    func unchangedSchemaDoesNotWrite() async throws {
        let engine = InMemoryEngine()
        let registry = SchemaRegistry(
            database: engine,
            root: Subspace(),
            clock: TestProcessMonotonicClock()
        )
        let schema = try Schema(
            entities: [try SchemaRegistryPersistenceRecord.schemaEntity]
        )

        try await registry.persist(schema)
        let versionAfterInitialPersistence = try await readVersion(of: engine)
        try await registry.persist(schema)
        let versionAfterRepeatedPersistence = try await readVersion(of: engine)

        #expect(versionAfterRepeatedPersistence == versionAfterInitialPersistence)
    }

    private func readVersion(of engine: InMemoryEngine) async throws -> Int64 {
        let transaction = try engine.createTransaction()
        do {
            let version = try await transaction.getReadVersion()
            try await transaction.cancel()
            return version
        } catch {
            try await transaction.cancel()
            throw error
        }
    }
}
