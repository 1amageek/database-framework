#if SQLITE
import Testing
import Foundation
import Database
import DatabaseKitFoundation
import DatabaseTypes
import StorageKit
import TestHeartbeat
import DatabaseRuntime

@Polymorphable(identifier: "Entity")
@PolymorphicDirectory("sqlite_polymorphic_vector_migration", "entities")
protocol SQLitePolymorphicVectorEntityV1:
    Polymorphable<SQLitePolymorphicVectorEntityV1PolymorphicGroup>
{
    var id: String { get }
    var label: String { get }
    var entityType: String { get }
    var embedding: Vector { get }
}

@Polymorphable(identifier: "Entity")
@PolymorphicDirectory("sqlite_polymorphic_vector_migration", "entities")
@PolymorphicIndex(
    .vector(dimensions: 3, metric: .cosine),
    embedding: "embedding",
    name: "Entity_vector_embedding"
)
protocol SQLitePolymorphicVectorEntityV2:
    Polymorphable<SQLitePolymorphicVectorEntityV2PolymorphicGroup>
{
    var id: String { get }
    var label: String { get }
    var entityType: String { get }
    var embedding: Vector { get }
}

@Polymorphable(identifier: "Entity")
@PolymorphicDirectory("sqlite_polymorphic_vector_migration", "entities")
@PolymorphicIndex(
    .vector(dimensions: 3, metric: .cosine),
    embedding: "embedding",
    name: "Entity_vector_embedding"
)
protocol SQLitePolymorphicVectorEntityV3:
    Polymorphable<SQLitePolymorphicVectorEntityV3PolymorphicGroup>
{
    var id: String { get }
    var label: String { get }
    var entityType: String { get }
    var embedding: Vector { get }
}

@Persistable(type: "SQLitePolymorphicVectorPerson")
struct SQLitePolymorphicVectorPersonV1: SQLitePolymorphicVectorEntityV1 {
    #Directory<SQLitePolymorphicVectorPersonV1>("sqlite_polymorphic_vector_migration", "persons")

    var id: String = UUID().uuidString
    var name: String
    var embedding: Vector
    var created: Date = Date(timeIntervalSince1970: 0)
    var updated: Date = Date(timeIntervalSince1970: 0)
}

@Persistable(type: "SQLitePolymorphicVectorPerson")
struct SQLitePolymorphicVectorPersonV2: SQLitePolymorphicVectorEntityV2 {
    #Directory<SQLitePolymorphicVectorPersonV2>("sqlite_polymorphic_vector_migration", "persons")

    var id: String = UUID().uuidString
    var name: String
    var embedding: Vector
    var created: Date = Date(timeIntervalSince1970: 0)
    var updated: Date = Date(timeIntervalSince1970: 0)
}

@Persistable(type: "SQLitePolymorphicVectorPerson")
struct SQLitePolymorphicVectorPersonV3: SQLitePolymorphicVectorEntityV3 {
    #Directory<SQLitePolymorphicVectorPersonV3>("sqlite_polymorphic_vector_migration", "persons")

    var id: String = UUID().uuidString
    var name: String
    var embedding: Vector
    var created: Date = Date(timeIntervalSince1970: 0)
    var updated: Date = Date(timeIntervalSince1970: 0)
}

@Persistable(type: "SQLitePolymorphicVectorOrganization")
struct SQLitePolymorphicVectorOrganizationV1: SQLitePolymorphicVectorEntityV1 {
    #Directory<SQLitePolymorphicVectorOrganizationV1>("sqlite_polymorphic_vector_migration", "organizations")

    var id: String = UUID().uuidString
    var name: String
    var domain: String
    var embedding: Vector
    var created: Date = Date(timeIntervalSince1970: 0)
    var updated: Date = Date(timeIntervalSince1970: 0)
}

@Persistable(type: "SQLitePolymorphicVectorOrganization")
struct SQLitePolymorphicVectorOrganizationV2: SQLitePolymorphicVectorEntityV2 {
    #Directory<SQLitePolymorphicVectorOrganizationV2>("sqlite_polymorphic_vector_migration", "organizations")

    var id: String = UUID().uuidString
    var name: String
    var domain: String
    var embedding: Vector
    var created: Date = Date(timeIntervalSince1970: 0)
    var updated: Date = Date(timeIntervalSince1970: 0)
}

@Persistable(type: "SQLitePolymorphicVectorOrganization")
struct SQLitePolymorphicVectorOrganizationV3: SQLitePolymorphicVectorEntityV3 {
    #Directory<SQLitePolymorphicVectorOrganizationV3>("sqlite_polymorphic_vector_migration", "organizations")

    var id: String = UUID().uuidString
    var name: String
    var domain: String
    var embedding: Vector
    var created: Date = Date(timeIntervalSince1970: 0)
    var updated: Date = Date(timeIntervalSince1970: 0)
}

extension SQLitePolymorphicVectorPersonV1 {
    var label: String { name }
    var entityType: String { "persons" }
}

extension SQLitePolymorphicVectorPersonV2 {
    var label: String { name }
    var entityType: String { "persons" }
}

extension SQLitePolymorphicVectorPersonV3 {
    var label: String { name }
    var entityType: String { "persons" }
}

extension SQLitePolymorphicVectorOrganizationV1 {
    var label: String { name }
    var entityType: String { "organizations" }
}

extension SQLitePolymorphicVectorOrganizationV2 {
    var label: String { name }
    var entityType: String { "organizations" }
}

extension SQLitePolymorphicVectorOrganizationV3 {
    var label: String { name }
    var entityType: String { "organizations" }
}

enum SQLitePolymorphicVectorSchemaV1: VersionedSchema {
    static let versionIdentifier = Schema.Version(1, 0, 0)
    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) {
            [
                try SQLitePolymorphicVectorPersonV1.schemaEntity,
                try SQLitePolymorphicVectorOrganizationV1.schemaEntity,
            ]
        }
    }
}

enum SQLitePolymorphicVectorSchemaV2: VersionedSchema {
    static let versionIdentifier = Schema.Version(2, 0, 0)
    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) {
            [
                try SQLitePolymorphicVectorPersonV2.schemaEntity,
                try SQLitePolymorphicVectorOrganizationV2.schemaEntity,
            ]
        }
    }
}

enum SQLitePolymorphicVectorSchemaV3: VersionedSchema {
    static let versionIdentifier = Schema.Version(3, 0, 0)
    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) {
            [
                try SQLitePolymorphicVectorPersonV3.schemaEntity,
                try SQLitePolymorphicVectorOrganizationV3.schemaEntity,
            ]
        }
    }
}

enum SQLitePolymorphicVectorAddMigrationPlan: SchemaMigrationPlan {
    static var schemas: [any VersionedSchema.Type] {
        [SQLitePolymorphicVectorSchemaV1.self, SQLitePolymorphicVectorSchemaV2.self]
    }

    static var stages: [MigrationStage] {
        [
            .lightweight(
                fromVersion: SQLitePolymorphicVectorSchemaV1.self,
                toVersion: SQLitePolymorphicVectorSchemaV2.self
            )
        ]
    }
}

enum SQLitePolymorphicVectorRebuildMigrationPlan: SchemaMigrationPlan {
    static var schemas: [any VersionedSchema.Type] {
        [SQLitePolymorphicVectorSchemaV2.self, SQLitePolymorphicVectorSchemaV3.self]
    }

    static var stages: [MigrationStage] {
        [
            .custom(
                fromVersion: SQLitePolymorphicVectorSchemaV2.self,
                toVersion: SQLitePolymorphicVectorSchemaV3.self,
                willMigrate: rebuildEntityVectorIndex,
                didMigrate: nil
            )
        ]
    }

    static func rebuildEntityVectorIndex(context: MigrationContext) async throws {
        try await context.rebuildIndex(indexName: "Entity_vector_embedding", batchSize: 1)
    }
}

@Suite("Polymorphic Vector Migration SQLite Tests", .serialized, .heartbeat)
struct PolymorphicVectorMigrationSQLiteTests {
    @Test("SQLite polymorphic entity vector descriptors decode canonically for each member")
    func sqlitePolymorphicEntityVectorDescriptorsStayConcretePerMemberType() throws {
        let schema = try SQLitePolymorphicVectorSchemaV2.makeSchema()
        let personDescriptor = try #require(
            schema.polymorphicIndexDescriptors(
                identifier: SQLitePolymorphicVectorPersonV2.polymorphableType,
                memberType: SQLitePolymorphicVectorPersonV2.self
            ).first { $0.name == "Entity_vector_embedding" }
        )
        let organizationDescriptor = try #require(
            schema.polymorphicIndexDescriptors(
                identifier: SQLitePolymorphicVectorOrganizationV2.polymorphableType,
                memberType: SQLitePolymorphicVectorOrganizationV2.self
            ).first { $0.name == "Entity_vector_embedding" }
        )

        let personDefinition = try IndexDefinition(metadata: personDescriptor.kind)
        let organizationDefinition = try IndexDefinition(
            metadata: organizationDescriptor.kind
        )
        #expect(personDescriptor.fieldNames == ["embedding"])
        #expect(organizationDescriptor.fieldNames == ["embedding"])
        #expect(personDefinition == .vector(dimensions: 3, metric: .cosine))
        #expect(organizationDefinition == .vector(dimensions: 3, metric: .cosine))
    }

    @Test("SQLite migration backfills polymorphic entity vector index")
    func sqliteMigrationBackfillsPolymorphicEntityVectorIndex() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        let initialContainer = try await DBContainer.open(
            for: SQLitePolymorphicVectorSchemaV1.makeSchema(),
            configuration: .init(backend: .custom(engine)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(persistableTypes: [SQLitePolymorphicVectorPersonV1.self, SQLitePolymorphicVectorOrganizationV1.self]),
            security: .disabled
        )
        let initialContext = initialContainer.newContext()

        var anchor = SQLitePolymorphicVectorPersonV1(
            name: "Alice",
            embedding: try Vector(float32: [1, 0, 0])
        )
        anchor.id = "sqlite-memory-vector-person-anchor"
        try initialContext.insert(anchor)

        for offset in 0..<105 {
            var person = SQLitePolymorphicVectorPersonV1(
                name: "Other \(offset)",
                embedding: try Vector(float32: [0, 1, 0])
            )
            person.id = "sqlite-memory-vector-person-\(offset)"
            try initialContext.insert(person)
        }

        var organization = SQLitePolymorphicVectorOrganizationV1(
            name: "Creww",
            domain: "creww.example",
            embedding: try Vector(float32: [0.95, 0.05, 0])
        )
        organization.id = "sqlite-memory-vector-organization"
        try initialContext.insert(organization)

        try await initialContext.save()
        try await initialContainer.installSchemaSnapshot(for: Schema.Version(1, 0, 0))

        let migratedContainer = try await DBContainer.open(
            for: SQLitePolymorphicVectorSchemaV2.self,
            migrationPlan: SQLitePolymorphicVectorAddMigrationPlan.self,
            configuration: .init(backend: .custom(engine)),
            runtimeConfiguration: try Self.vectorRuntimeConfiguration(
                persistableTypes: [
                    SQLitePolymorphicVectorPersonV2.self,
                    SQLitePolymorphicVectorOrganizationV2.self,
                ]
            ),
            security: .disabled
        )
        try await migratedContainer.migrateIfNeeded()

        #expect(try await Self.countEntityVectorIndexEntries(container: migratedContainer) == 107)

        let page = try await migratedContainer.newContext()
            .findPolymorphic(SQLitePolymorphicVectorPersonV2.self)
            .vector(SQLitePolymorphicVectorPersonV2.fields.embedding, dimensions: 3)
            .query([1, 0, 0], k: 2)
            .metric(.cosine)
            .executePage()
        let ids = Set(page.results.compactMap(Self.resultIDV2))

        #expect(ids == Set([anchor.id, organization.id]))

        let organizationStartedPage = try await migratedContainer.newContext()
            .findPolymorphic(SQLitePolymorphicVectorOrganizationV2.self)
            .vector(
                SQLitePolymorphicVectorOrganizationV2.fields.embedding,
                dimensions: 3
            )
            .query([1, 0, 0], k: 2)
            .metric(.cosine)
            .executePage()
        let organizationStartedIDs = Set(organizationStartedPage.results.compactMap(Self.resultIDV2))

        #expect(organizationStartedIDs == Set([anchor.id, organization.id]))
    }

    @Test("SQLite custom migration rebuilds polymorphic entity vector index")
    func sqliteCustomMigrationRebuildsPolymorphicEntityVectorIndex() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        let initialContainer = try await DBContainer.open(
            for: SQLitePolymorphicVectorSchemaV2.makeSchema(),
            configuration: .init(backend: .custom(engine)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(persistableTypes: [SQLitePolymorphicVectorPersonV2.self, SQLitePolymorphicVectorOrganizationV2.self]),
            security: .disabled
        )
        let context = initialContainer.newContext()

        var person = SQLitePolymorphicVectorPersonV2(
            name: "Alice",
            embedding: try Vector(float32: [1, 0, 0])
        )
        person.id = "sqlite-memory-vector-rebuild-person"
        var organization = SQLitePolymorphicVectorOrganizationV2(
            name: "Creww",
            domain: "creww.example",
            embedding: try Vector(float32: [0.95, 0.05, 0])
        )
        organization.id = "sqlite-memory-vector-rebuild-organization"

        try context.upsert(person)
        try context.upsert(organization)
        try await context.save()
        try await initialContainer.installSchemaSnapshot(for: Schema.Version(2, 0, 0))
        try await Self.clearEntityVectorIndexEntries(container: initialContainer)
        #expect(try await Self.countEntityVectorIndexEntries(container: initialContainer) == 0)

        let migratedContainer = try await DBContainer.open(
            for: SQLitePolymorphicVectorSchemaV3.self,
            migrationPlan: SQLitePolymorphicVectorRebuildMigrationPlan.self,
            configuration: .init(backend: .custom(engine)),
            runtimeConfiguration: try Self.vectorRuntimeConfiguration(
                persistableTypes: [
                    SQLitePolymorphicVectorPersonV3.self,
                    SQLitePolymorphicVectorOrganizationV3.self,
                ]
            ),
            security: .disabled
        )
        try await migratedContainer.migrateIfNeeded()

        let page = try await migratedContainer.newContext()
            .findPolymorphic(SQLitePolymorphicVectorPersonV3.self)
            .vector(SQLitePolymorphicVectorPersonV3.fields.embedding, dimensions: 3)
            .query([1, 0, 0], k: 2)
            .metric(.cosine)
            .executePage()
        let ids = Set(page.results.compactMap(Self.resultIDV3))

        #expect(ids == Set([person.id, organization.id]))

        let organizationStartedPage = try await migratedContainer.newContext()
            .findPolymorphic(SQLitePolymorphicVectorOrganizationV3.self)
            .vector(
                SQLitePolymorphicVectorOrganizationV3.fields.embedding,
                dimensions: 3
            )
            .query([1, 0, 0], k: 2)
            .metric(.cosine)
            .executePage()
        let organizationStartedIDs = Set(organizationStartedPage.results.compactMap(Self.resultIDV3))

        #expect(organizationStartedIDs == Set([person.id, organization.id]))
        #expect(try await Self.countEntityVectorIndexEntries(container: migratedContainer) == 2)
        #expect(try await Self.entityVectorIndexState(container: migratedContainer) == .readable)
    }

    private static func vectorRuntimeConfiguration(
        persistableTypes: [any Persistable.Type]
    ) throws -> DatabaseRuntimeConfiguration {
        try DatabaseRuntimeConfiguration(
            indexMaintainerProviders: [
                VectorIndexMaintainerProvider()
            ],
            indexReadExecutors: [VectorReadExecutors.indexExecutor],
            polymorphicIndexReadExecutors: [VectorReadExecutors.polymorphicIndexExecutor],
            persistableTypes: persistableTypes
        )
    }

    private static func resultIDV2(_ result: PolymorphicQueryResult) -> String? {
        if let person = result.item(as: SQLitePolymorphicVectorPersonV2.self) {
            return person.id
        }
        if let organization = result.item(as: SQLitePolymorphicVectorOrganizationV2.self) {
            return organization.id
        }
        return nil
    }

    private static func resultIDV3(_ result: PolymorphicQueryResult) -> String? {
        if let person = result.item(as: SQLitePolymorphicVectorPersonV3.self) {
            return person.id
        }
        if let organization = result.item(as: SQLitePolymorphicVectorOrganizationV3.self) {
            return organization.id
        }
        return nil
    }

    private static func countEntityVectorIndexEntries(container: DBContainer) async throws -> Int {
        let indexSubspace = try await entityVectorIndexSubspace(container: container)

        return try await container.engine.withTransaction { transaction -> Int in
            let (begin, end) = indexSubspace.range()
            var count = 0
            for try await _ in transaction.getRange(begin: begin, end: end, snapshot: true) {
                count += 1
            }
            return count
        }
    }

    private static func clearEntityVectorIndexEntries(container: DBContainer) async throws {
        let indexSubspace = try await entityVectorIndexSubspace(container: container)
        let range = indexSubspace.range()

        try await container.engine.withTransaction { transaction in
            try transaction.clearRange(beginKey: range.begin, endKey: range.end)
        }
    }

    private static func entityVectorIndexState(container: DBContainer) async throws -> IndexState {
        let group = try container.polymorphicGroup(identifier: SQLitePolymorphicVectorPersonV2.polymorphableType)
        let groupSubspace = try await container.resolvePolymorphicDirectory(for: group.identifier)
        let lifecycleStore = IndexLifecycleStore(container: container, subspace: groupSubspace)
        return try await lifecycleStore.state(of: "Entity_vector_embedding")
    }

    private static func entityVectorIndexSubspace(container: DBContainer) async throws -> Subspace {
        let group = try container.polymorphicGroup(identifier: SQLitePolymorphicVectorPersonV2.polymorphableType)
        let groupSubspace = try await container.resolvePolymorphicDirectory(for: group.identifier)
        return groupSubspace
            .subspace(SubspaceKey.indexes)
            .subspace("Entity_vector_embedding")
    }
}
#endif
