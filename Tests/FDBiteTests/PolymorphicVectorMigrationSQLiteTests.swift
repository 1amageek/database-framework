#if SQLITE
import Testing
import Foundation
import Database
import StorageKit
import TestHeartbeat

protocol SQLiteMemoryVectorEntityV1: Polymorphable {
    var id: String { get }
    var label: String { get }
    var entityType: String { get }
    var embedding: [Float] { get }
}

protocol SQLiteMemoryVectorEntityV2: Polymorphable {
    var id: String { get }
    var label: String { get }
    var entityType: String { get }
    var embedding: [Float] { get }
}

protocol SQLiteMemoryVectorEntityV3: Polymorphable {
    var id: String { get }
    var label: String { get }
    var entityType: String { get }
    var embedding: [Float] { get }
}

extension SQLiteMemoryVectorEntityV1 {
    public static var polymorphableType: String { "Entity" }

    public static var polymorphicDirectoryPathComponents: [any DirectoryPathElement] {
        [Path("sqlite_memory_vector_migration"), Path("entities")]
    }
}

extension SQLiteMemoryVectorEntityV2 {
    public static var polymorphableType: String { "Entity" }

    public static var polymorphicDirectoryPathComponents: [any DirectoryPathElement] {
        [Path("sqlite_memory_vector_migration"), Path("entities")]
    }

    public static var polymorphicIndexDescriptors: [IndexDescriptor] {
        [
            IndexDescriptor(
                name: "Entity_vector_embedding",
                keyPaths: [\Self.embedding],
                kind: VectorIndexKind<Self>(
                    embedding: \Self.embedding,
                    dimensions: 3,
                    metric: .cosine
                )
            )
        ]
    }
}

extension SQLiteMemoryVectorEntityV3 {
    public static var polymorphableType: String { "Entity" }

    public static var polymorphicDirectoryPathComponents: [any DirectoryPathElement] {
        [Path("sqlite_memory_vector_migration"), Path("entities")]
    }

    public static var polymorphicIndexDescriptors: [IndexDescriptor] {
        [
            IndexDescriptor(
                name: "Entity_vector_embedding",
                keyPaths: [\Self.embedding],
                kind: VectorIndexKind<Self>(
                    embedding: \Self.embedding,
                    dimensions: 3,
                    metric: .cosine
                )
            )
        ]
    }
}

@Persistable(type: "SQLiteMemoryVectorPerson")
struct SQLiteMemoryVectorPersonV1: SQLiteMemoryVectorEntityV1 {
    #Directory<SQLiteMemoryVectorPersonV1>("sqlite_memory_vector_migration", "persons")

    var id: String = ULID().ulidString
    var name: String
    var embedding: [Float]
    var created: Date = Date(timeIntervalSince1970: 0)
    var updated: Date = Date(timeIntervalSince1970: 0)
}

@Persistable(type: "SQLiteMemoryVectorPerson")
struct SQLiteMemoryVectorPersonV2: SQLiteMemoryVectorEntityV2 {
    #Directory<SQLiteMemoryVectorPersonV2>("sqlite_memory_vector_migration", "persons")

    var id: String = ULID().ulidString
    var name: String
    var embedding: [Float]
    var created: Date = Date(timeIntervalSince1970: 0)
    var updated: Date = Date(timeIntervalSince1970: 0)
}

@Persistable(type: "SQLiteMemoryVectorPerson")
struct SQLiteMemoryVectorPersonV3: SQLiteMemoryVectorEntityV3 {
    #Directory<SQLiteMemoryVectorPersonV3>("sqlite_memory_vector_migration", "persons")

    var id: String = ULID().ulidString
    var name: String
    var embedding: [Float]
    var created: Date = Date(timeIntervalSince1970: 0)
    var updated: Date = Date(timeIntervalSince1970: 0)
}

@Persistable(type: "SQLiteMemoryVectorOrganization")
struct SQLiteMemoryVectorOrganizationV1: SQLiteMemoryVectorEntityV1 {
    #Directory<SQLiteMemoryVectorOrganizationV1>("sqlite_memory_vector_migration", "organizations")

    var id: String = ULID().ulidString
    var name: String
    var domain: String
    var embedding: [Float]
    var created: Date = Date(timeIntervalSince1970: 0)
    var updated: Date = Date(timeIntervalSince1970: 0)
}

@Persistable(type: "SQLiteMemoryVectorOrganization")
struct SQLiteMemoryVectorOrganizationV2: SQLiteMemoryVectorEntityV2 {
    #Directory<SQLiteMemoryVectorOrganizationV2>("sqlite_memory_vector_migration", "organizations")

    var id: String = ULID().ulidString
    var name: String
    var domain: String
    var embedding: [Float]
    var created: Date = Date(timeIntervalSince1970: 0)
    var updated: Date = Date(timeIntervalSince1970: 0)
}

@Persistable(type: "SQLiteMemoryVectorOrganization")
struct SQLiteMemoryVectorOrganizationV3: SQLiteMemoryVectorEntityV3 {
    #Directory<SQLiteMemoryVectorOrganizationV3>("sqlite_memory_vector_migration", "organizations")

    var id: String = ULID().ulidString
    var name: String
    var domain: String
    var embedding: [Float]
    var created: Date = Date(timeIntervalSince1970: 0)
    var updated: Date = Date(timeIntervalSince1970: 0)
}

extension SQLiteMemoryVectorPersonV1 {
    var label: String { name }
    var entityType: String { "persons" }
}

extension SQLiteMemoryVectorPersonV2 {
    var label: String { name }
    var entityType: String { "persons" }
}

extension SQLiteMemoryVectorPersonV3 {
    var label: String { name }
    var entityType: String { "persons" }
}

extension SQLiteMemoryVectorOrganizationV1 {
    var label: String { name }
    var entityType: String { "organizations" }
}

extension SQLiteMemoryVectorOrganizationV2 {
    var label: String { name }
    var entityType: String { "organizations" }
}

extension SQLiteMemoryVectorOrganizationV3 {
    var label: String { name }
    var entityType: String { "organizations" }
}

enum SQLiteMemoryVectorSchemaV1: VersionedSchema {
    static let versionIdentifier = Schema.Version(1, 0, 0)
    static let models: [any Persistable.Type] = [
        SQLiteMemoryVectorPersonV1.self,
        SQLiteMemoryVectorOrganizationV1.self,
    ]
}

enum SQLiteMemoryVectorSchemaV2: VersionedSchema {
    static let versionIdentifier = Schema.Version(2, 0, 0)
    static let models: [any Persistable.Type] = [
        SQLiteMemoryVectorPersonV2.self,
        SQLiteMemoryVectorOrganizationV2.self,
    ]
}

enum SQLiteMemoryVectorSchemaV3: VersionedSchema {
    static let versionIdentifier = Schema.Version(3, 0, 0)
    static let models: [any Persistable.Type] = [
        SQLiteMemoryVectorPersonV3.self,
        SQLiteMemoryVectorOrganizationV3.self,
    ]
}

enum SQLiteMemoryVectorAddMigrationPlan: SchemaMigrationPlan {
    static var schemas: [any VersionedSchema.Type] {
        [SQLiteMemoryVectorSchemaV1.self, SQLiteMemoryVectorSchemaV2.self]
    }

    static var stages: [MigrationStage] {
        [
            .lightweight(
                fromVersion: SQLiteMemoryVectorSchemaV1.self,
                toVersion: SQLiteMemoryVectorSchemaV2.self
            )
        ]
    }
}

enum SQLiteMemoryVectorRebuildMigrationPlan: SchemaMigrationPlan {
    static var schemas: [any VersionedSchema.Type] {
        [SQLiteMemoryVectorSchemaV2.self, SQLiteMemoryVectorSchemaV3.self]
    }

    static var stages: [MigrationStage] {
        [
            .custom(
                fromVersion: SQLiteMemoryVectorSchemaV2.self,
                toVersion: SQLiteMemoryVectorSchemaV3.self,
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
    @Test("SQLite Memory Entity vector descriptors stay concrete per member type")
    func sqliteMemoryEntityVectorDescriptorsStayConcretePerMemberType() throws {
        let schema = SQLiteMemoryVectorSchemaV2.makeSchema()
        let personDescriptor = try #require(
            schema.polymorphicIndexDescriptors(
                identifier: SQLiteMemoryVectorPersonV2.polymorphableType,
                memberType: SQLiteMemoryVectorPersonV2.self
            ).first { $0.name == "Entity_vector_embedding" }
        )
        let organizationDescriptor = try #require(
            schema.polymorphicIndexDescriptors(
                identifier: SQLiteMemoryVectorOrganizationV2.polymorphableType,
                memberType: SQLiteMemoryVectorOrganizationV2.self
            ).first { $0.name == "Entity_vector_embedding" }
        )

        #expect(personDescriptor.kind is VectorIndexKind<SQLiteMemoryVectorPersonV2>)
        #expect(organizationDescriptor.kind is VectorIndexKind<SQLiteMemoryVectorOrganizationV2>)
        #expect(personDescriptor.keyPaths.first is PartialKeyPath<SQLiteMemoryVectorPersonV2>)
        #expect(organizationDescriptor.keyPaths.first is PartialKeyPath<SQLiteMemoryVectorOrganizationV2>)
        #expect(personDescriptor.keyPaths.first is PartialKeyPath<SQLiteMemoryVectorOrganizationV2> == false)
        #expect(organizationDescriptor.keyPaths.first is PartialKeyPath<SQLiteMemoryVectorPersonV2> == false)
    }

    @Test("SQLite migration backfills swift-memory Entity vector index")
    func sqliteMigrationBackfillsSwiftMemoryEntityVectorIndex() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        let initialContainer = try await DBContainer(
            for: SQLiteMemoryVectorSchemaV1.makeSchema(),
            configuration: .init(backend: .custom(engine)),
            security: .disabled
        )
        let initialContext = initialContainer.newContext()

        var anchor = SQLiteMemoryVectorPersonV1(name: "Alice", embedding: [1, 0, 0])
        anchor.id = "sqlite-memory-vector-person-anchor"
        initialContext.insert(anchor)

        for offset in 0..<105 {
            var person = SQLiteMemoryVectorPersonV1(
                name: "Other \(offset)",
                embedding: [0, 1, 0]
            )
            person.id = "sqlite-memory-vector-person-\(offset)"
            initialContext.insert(person)
        }

        var organization = SQLiteMemoryVectorOrganizationV1(
            name: "Creww",
            domain: "creww.example",
            embedding: [0.95, 0.05, 0]
        )
        organization.id = "sqlite-memory-vector-organization"
        initialContext.insert(organization)

        try await initialContext.save()
        try await initialContainer.setCurrentSchemaVersion(Schema.Version(1, 0, 0))

        let migratedContainer = try await DBContainer(
            for: SQLiteMemoryVectorSchemaV2.self,
            migrationPlan: SQLiteMemoryVectorAddMigrationPlan.self,
            configuration: .init(backend: .custom(engine)),
            security: .disabled
        )
        try await migratedContainer.migrateIfNeeded()
        VectorReadBridge.registerReadExecutors()

        #expect(try await Self.countEntityVectorIndexEntries(container: migratedContainer) == 107)

        let page = try await migratedContainer.newContext()
            .findPolymorphic(SQLiteMemoryVectorPersonV2.self)
            .vector(\.embedding, dimensions: 3)
            .query([1, 0, 0], k: 2)
            .metric(.cosine)
            .executePage()
        let ids = Set(page.results.compactMap(Self.resultIDV2))

        #expect(ids == Set([anchor.id, organization.id]))

        let organizationStartedPage = try await migratedContainer.newContext()
            .findPolymorphic(SQLiteMemoryVectorOrganizationV2.self)
            .vector(\.embedding, dimensions: 3)
            .query([1, 0, 0], k: 2)
            .metric(.cosine)
            .executePage()
        let organizationStartedIDs = Set(organizationStartedPage.results.compactMap(Self.resultIDV2))

        #expect(organizationStartedIDs == Set([anchor.id, organization.id]))
    }

    @Test("SQLite custom migration rebuilds swift-memory Entity vector index")
    func sqliteCustomMigrationRebuildsSwiftMemoryEntityVectorIndex() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        let initialContainer = try await DBContainer(
            for: SQLiteMemoryVectorSchemaV2.makeSchema(),
            configuration: .init(backend: .custom(engine)),
            security: .disabled
        )
        let context = initialContainer.newContext()

        var person = SQLiteMemoryVectorPersonV2(name: "Alice", embedding: [1, 0, 0])
        person.id = "sqlite-memory-vector-rebuild-person"
        var organization = SQLiteMemoryVectorOrganizationV2(
            name: "Creww",
            domain: "creww.example",
            embedding: [0.95, 0.05, 0]
        )
        organization.id = "sqlite-memory-vector-rebuild-organization"

        try await context.savePolymorphic(person, as: SQLiteMemoryVectorPersonV2.self)
        try await context.savePolymorphic(organization, as: SQLiteMemoryVectorPersonV2.self)
        try await initialContainer.setCurrentSchemaVersion(Schema.Version(2, 0, 0))
        try await Self.clearEntityVectorIndexEntries(container: initialContainer)
        #expect(try await Self.countEntityVectorIndexEntries(container: initialContainer) == 0)

        let migratedContainer = try await DBContainer(
            for: SQLiteMemoryVectorSchemaV3.self,
            migrationPlan: SQLiteMemoryVectorRebuildMigrationPlan.self,
            configuration: .init(backend: .custom(engine)),
            security: .disabled
        )
        try await migratedContainer.migrateIfNeeded()
        VectorReadBridge.registerReadExecutors()

        let page = try await migratedContainer.newContext()
            .findPolymorphic(SQLiteMemoryVectorPersonV3.self)
            .vector(\.embedding, dimensions: 3)
            .query([1, 0, 0], k: 2)
            .metric(.cosine)
            .executePage()
        let ids = Set(page.results.compactMap(Self.resultIDV3))

        #expect(ids == Set([person.id, organization.id]))

        let organizationStartedPage = try await migratedContainer.newContext()
            .findPolymorphic(SQLiteMemoryVectorOrganizationV3.self)
            .vector(\.embedding, dimensions: 3)
            .query([1, 0, 0], k: 2)
            .metric(.cosine)
            .executePage()
        let organizationStartedIDs = Set(organizationStartedPage.results.compactMap(Self.resultIDV3))

        #expect(organizationStartedIDs == Set([person.id, organization.id]))
        #expect(try await Self.countEntityVectorIndexEntries(container: migratedContainer) == 2)
        #expect(try await Self.entityVectorIndexState(container: migratedContainer) == .readable)
    }

    private static func resultIDV2(_ result: PolymorphicQueryResult) -> String? {
        if let person = result.item(as: SQLiteMemoryVectorPersonV2.self) {
            return person.id
        }
        if let organization = result.item(as: SQLiteMemoryVectorOrganizationV2.self) {
            return organization.id
        }
        return nil
    }

    private static func resultIDV3(_ result: PolymorphicQueryResult) -> String? {
        if let person = result.item(as: SQLiteMemoryVectorPersonV3.self) {
            return person.id
        }
        if let organization = result.item(as: SQLiteMemoryVectorOrganizationV3.self) {
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
            transaction.clearRange(beginKey: range.begin, endKey: range.end)
        }
    }

    private static func entityVectorIndexState(container: DBContainer) async throws -> IndexState {
        let group = try container.polymorphicGroup(identifier: SQLiteMemoryVectorPersonV2.polymorphableType)
        let groupSubspace = try await container.resolvePolymorphicDirectory(for: group.identifier)
        let stateManager = IndexStateManager(container: container, subspace: groupSubspace)
        return try await stateManager.state(of: "Entity_vector_embedding")
    }

    private static func entityVectorIndexSubspace(container: DBContainer) async throws -> Subspace {
        let group = try container.polymorphicGroup(identifier: SQLiteMemoryVectorPersonV2.polymorphableType)
        let groupSubspace = try await container.resolvePolymorphicDirectory(for: group.identifier)
        return groupSubspace
            .subspace(SubspaceKey.indexes)
            .subspace("Entity_vector_embedding")
    }
}
#endif
