import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
@_spi(DatabaseExecution) import DatabaseWire
import StorageKit
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import Database
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("Remote publication with SQL-owned entities", .timeLimit(.minutes(1)))
struct SQLOwnedSchemaCompositionTests {
    @Persistable
    struct CompositionItem {
        #Directory<CompositionItem>("sql-owned-composition-items")
        var id: String = ""
        var title: String = ""
    }

    @Persistable
    struct CompositionNote {
        #Directory<CompositionNote>("sql-owned-composition-notes")
        var id: String = ""
        var body: String = ""
    }

    @Test("A declaration without SQL tables publishes them unchanged and rejects their redefinition")
    func composesSQLOwnedEntities() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        _ = try await context.executeSQLBlock(
            "CREATE TABLE sql_items(value INTEGER); INSERT INTO sql_items VALUES (7);", options: options())
        let lease = container.acquirePublishedSchemaLease()
        let sqlEntity = try #require(lease.schema.entities.first { $0.name != CompositionItem.persistableType })
        let declared = try Schema(entities: [CompositionItem.schemaEntity, CompositionNote.schemaEntity],
            version: Schema.Version(1, 1, 0))

        // Publishing the bare declaration would drop the SQL table, so it fails.
        await #expect(throws: SQLCatalogBindingError.self) {
            try await publish(declared, on: container, expected: lease.fingerprint, key: "bare")
        }

        let effective = try lease.applicationSchema(declaring: declared)
        #expect(effective.entities.map(\.name) == declared.entities.map(\.name) + [sqlEntity.name])
        #expect(effective.entity(named: sqlEntity.name) == sqlEntity)
        #expect(effective.version == declared.version)
        let redeclared = try Schema(entities: declared.entities + [sqlEntity], version: declared.version)
        #expect(try lease.applicationSchema(declaring: redeclared) == redeclared)
        let conflicting = try Schema.Entity(name: sqlEntity.name,
            identifierType: sqlEntity.identifierType == .bytes ? .uuid : .bytes, fields: sqlEntity.fields,
            directoryComponents: sqlEntity.directoryComponents, directoryLayer: sqlEntity.directoryLayer)
        #expect(throws: SQLCatalogBindingError.sqlOwnedEntityRedeclared(sqlEntity.name)) {
            try lease.applicationSchema(declaring: try Schema(entities: declared.entities + [conflicting],
                version: declared.version))
        }

        let result = try await publish(effective, on: container, expected: lease.fingerprint, key: "composed")
        #expect(container.schema == effective)
        #expect(container.schemaGeneration == result.generation)
        let rows = try await context.executeSQL("SELECT value FROM sql_items")
        #expect(rows.rows.map(\.values) == [[.primitive(.int32(7))]])
    }

    @discardableResult
    private func publish(_ schema: Schema, on container: DBContainer, expected: SchemaFingerprint,
        key: String) async throws -> DatabaseSchemaPublicationResult {
        try await container.publishSchema(schema, fingerprint: SchemaManifest(schema: schema).fingerprint(),
            expectedFingerprint: expected, idempotencyKey: key, authorization: TestBaseEnvironment.authorization,
            runtimeConfiguration: runtime(schema))
    }

    private func makeContainer() async throws -> DBContainer {
        let schema = try Schema(entities: [CompositionItem.schemaEntity], version: Schema.Version(1, 0, 0))
        return try await DBContainer.open(for: schema, configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: runtime(schema), security: .testingDisabled)
    }

    private func runtime(_ schema: Schema) throws -> DatabaseRuntimeConfiguration {
        try DatabaseFrameworkRuntime.configuration(
            executionIdentity: .init(identifier: "sql-owned-composition", revision: 1), schema: schema)
    }

    private func options() throws -> SQLBlockExecutionOptions {
        SQLBlockExecutionOptions(checkpointLimits: try TransactionCheckpointLimits(maximumEntries: 1_000,
            maximumDepth: 16, maximumRetainedBytes: 4 * 1_024 * 1_024,
            maximumWorkUnits: 1_000_000, maximumReadPageRows: 128))
    }
}
