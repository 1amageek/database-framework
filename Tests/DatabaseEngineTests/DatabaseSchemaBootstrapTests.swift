import DatabaseKit
import TestSupport
@_spi(DatabaseExecution) import DatabaseEngine
import DatabaseRuntime
import StorageKit
import Testing

@Suite("Database Schema Bootstrap Tests", .serialized)
struct DatabaseSchemaBootstrapTests {
    @Test("Initial schema version, catalog, and index state commit together")
    func commitsInitialSchema() async throws {
        let engine = InMemoryEngine()
        let container = try await makeVersionedContainer(engine: engine)

        try await container.testBaseAdmin().migrateIfNeeded()

        #expect(try await container.testBaseCurrentSchemaVersion() == Schema.Version(1, 0, 0))
        let entities = try await container.testPersistedControlSchemaEntities()
        #expect(
            entities.map { $0.name }
                .contains(BootstrapIndexedEntity.persistableType)
        )
        let state = try await container.testBaseContext().withDataOperation {
            let subspace = try await container.resolveDirectory(
                for: BootstrapIndexedEntity.self
            )
            return try await IndexLifecycleStore(
                container: container,
                subspace: subspace
            ).state(of: "bootstrap_value")
        }
        #expect(state == .readable)
    }

    @Test("Schema-driven rows retain their published version across a compiled reopen")
    func schemaDrivenRowsRetainPublishedVersion() async throws {
        let engine = InMemoryEngine()
        let unversioned = try await DBContainer.open(
            for: try Schema(
                entities: [
                    try BootstrapIndexedEntity.schemaEntity,
                ],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: .testing(storageEngine: engine),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
            entityRuntimes: [try DatabaseFrameworkRuntime.entity(BootstrapIndexedEntity.self)]
            ),
            security: .testingDisabled
        )
        let context = unversioned.testBaseContext()
        var entity = BootstrapIndexedEntity()
        entity.id = "orphan"
        entity.value = "value"
        try context.insert(entity)
        try await context.save()
        #expect(
            try await unversioned.testBaseCurrentSchemaVersion()
                == Schema.Version(1, 0, 0)
        )

        let versioned = try await makeVersionedContainer(engine: engine)
        try await versioned.testBaseAdmin().migrateIfNeeded()
        #expect(
            try await versioned.testBaseCurrentSchemaVersion()
                == Schema.Version(1, 0, 0)
        )
        let reopened = versioned.testBaseContext()
        #expect(
            try await reopened.model(
                for: "orphan",
                as: BootstrapIndexedEntity.self
            )?.value == "value"
        )
    }

    @Test("A reused version cannot conceal a different compiled schema")
    func rejectsDivergentSchemaAtCommittedVersion() async throws {
        let engine = InMemoryEngine()
        let initial = try await makeVersionedContainer(engine: engine)
        try await initial.testBaseAdmin().migrateIfNeeded()

        let divergent = try await DBContainer.open(
            for: try Schema(
                entities: [
                    try DivergentBootstrapEntity.schemaEntity,
                ],
                version: Schema.Version(1, 0, 0)
            ),
            migrationPlan: BootstrapMigrationPlan.self,
            configuration: .testing(storageEngine: engine),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
            entityRuntimes: [
                try DatabaseFrameworkRuntime.entity(BootstrapIndexedEntity.self),
                try DatabaseFrameworkRuntime.entity(DivergentBootstrapEntity.self),
            ]
            ),
            security: .testingDisabled
        )

        do {
            _ = try await divergent.testBaseAdmin().migrationStatus()
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
            configuration: .testing(storageEngine: engine),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
            entityRuntimes: [try DatabaseFrameworkRuntime.entity(BootstrapIndexedEntity.self)]
            ),
            security: .testingDisabled
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

@Persistable
private struct DivergentBootstrapEntity {
    #Directory<DivergentBootstrapEntity>("test", "schema-bootstrap-divergent")

    var id: String = ""
}
