import DatabaseKit
import DatabaseEngine
import DatabaseRuntime
import StorageKit
import Testing

@Suite("Database Schema Bootstrap Tests", .serialized)
struct DatabaseSchemaBootstrapTests {
    @Test("Initial schema version, catalog, and index state commit together")
    func commitsInitialSchema() async throws {
        let engine = InMemoryEngine()
        let container = try await makeVersionedContainer(engine: engine)

        try await container.migrateIfNeeded()

        #expect(try await container.getCurrentSchemaVersion() == Schema.Version(1, 0, 0))
        let entities = try await SchemaRegistry(database: engine).loadAll()
        #expect(entities.map(\.name).contains(BootstrapIndexedEntity.persistableType))
        let subspace = try await container.resolveDirectory(
            for: BootstrapIndexedEntity.self
        )
        let state = try await IndexLifecycleStore(
            container: container,
            subspace: subspace
        ).state(of: "bootstrap_value")
        #expect(state == .readable)
    }

    @Test("Unversioned rows cannot be silently adopted by initial schema")
    func rejectsUnversionedRows() async throws {
        let engine = InMemoryEngine()
        let unversioned = try await DBContainer.open(
            for: try Schema(
                entities: [
                    try BootstrapIndexedEntity.schemaEntity,
                ],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: .init(backend: .custom(engine)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                persistableTypes: [BootstrapIndexedEntity.self]
            ),
            security: .disabled
        )
        let context = unversioned.newContext()
        var entity = BootstrapIndexedEntity()
        entity.id = "orphan"
        entity.value = "value"
        try context.insert(entity)
        try await context.save()

        let versioned = try await makeVersionedContainer(engine: engine)
        await #expect(throws: MigrationPlanError.self) {
            try await versioned.migrateIfNeeded()
        }
        #expect(try await versioned.getCurrentSchemaVersion() == nil)
    }

    @Test("A reused version cannot conceal a different compiled schema")
    func rejectsDivergentSchemaAtCommittedVersion() async throws {
        let engine = InMemoryEngine()
        let initial = try await makeVersionedContainer(engine: engine)
        try await initial.migrateIfNeeded()

        let divergent = try await DBContainer.open(
            for: try Schema(
                entities: [
                    try DatabaseEndpointEntity.schemaEntity,
                ],
                version: Schema.Version(1, 0, 0)
            ),
            migrationPlan: BootstrapMigrationPlan.self,
            configuration: .init(backend: .custom(engine)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                persistableTypes: [
                    BootstrapIndexedEntity.self,
                    DatabaseEndpointEntity.self,
                ]
            ),
            security: .disabled
        )

        do {
            _ = try await divergent.migrationStatus()
            Issue.record("Expected the committed schema fingerprint to be rejected")
        } catch MigrationPlanError.schemaFingerprintMismatch(let version) {
            #expect(version == Schema.Version(1, 0, 0))
        }
    }

    private func makeVersionedContainer(
        engine: InMemoryEngine
    ) async throws -> DBContainer {
        try await DBContainer.open(
            for: BootstrapSchema.self,
            migrationPlan: BootstrapMigrationPlan.self,
            configuration: .init(backend: .custom(engine)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                persistableTypes: [BootstrapIndexedEntity.self]
            ),
            security: .disabled
        )
    }

    private enum BootstrapSchema: VersionedSchema {
        static let versionIdentifier = Schema.Version(1, 0, 0)
        static var entities: [Schema.Entity] {
            get throws(SchemaEntityError) {
                [try BootstrapIndexedEntity.schemaEntity]
            }
        }
    }

    private enum BootstrapMigrationPlan: SchemaMigrationPlan {
        static let schemas: [any VersionedSchema.Type] = [
            BootstrapSchema.self,
        ]
        static let stages: [MigrationStage] = []
    }
}
