#if FOUNDATION_DB
import Testing
import Foundation
import StorageKit
import FDBStorage
import Core
import Vector
import TestHeartbeat
import TestSupport
@testable import DatabaseEngine
@testable import VectorIndex

protocol FDBMemoryVectorEntityV1: Polymorphable {
    var id: String { get }
    var label: String { get }
    var entityType: String { get }
    var embedding: [Float] { get }
}

protocol FDBMemoryVectorEntityV2: Polymorphable {
    var id: String { get }
    var label: String { get }
    var entityType: String { get }
    var embedding: [Float] { get }
}

protocol FDBMemoryVectorEntityV3: Polymorphable {
    var id: String { get }
    var label: String { get }
    var entityType: String { get }
    var embedding: [Float] { get }
}

extension FDBMemoryVectorEntityV1 {
    public static var polymorphableType: String { "Entity" }

    public static var polymorphicDirectoryPathComponents: [any DirectoryPathElement] {
        [Path("fdb_memory_vector_migration"), Path("entities")]
    }
}

extension FDBMemoryVectorEntityV2 {
    public static var polymorphableType: String { "Entity" }

    public static var polymorphicDirectoryPathComponents: [any DirectoryPathElement] {
        [Path("fdb_memory_vector_migration"), Path("entities")]
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

extension FDBMemoryVectorEntityV3 {
    public static var polymorphableType: String { "Entity" }

    public static var polymorphicDirectoryPathComponents: [any DirectoryPathElement] {
        [Path("fdb_memory_vector_migration"), Path("entities")]
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

@Persistable(type: "FDBMemoryVectorPerson")
struct FDBMemoryVectorPersonV1: FDBMemoryVectorEntityV1 {
    #Directory<FDBMemoryVectorPersonV1>("fdb_memory_vector_migration", "persons")

    var id: String = ULID().ulidString
    var name: String
    var embedding: [Float]
    var created: Date = Date(timeIntervalSince1970: 0)
    var updated: Date = Date(timeIntervalSince1970: 0)
}

@Persistable(type: "FDBMemoryVectorPerson")
struct FDBMemoryVectorPersonV2: FDBMemoryVectorEntityV2 {
    #Directory<FDBMemoryVectorPersonV2>("fdb_memory_vector_migration", "persons")

    var id: String = ULID().ulidString
    var name: String
    var embedding: [Float]
    var created: Date = Date(timeIntervalSince1970: 0)
    var updated: Date = Date(timeIntervalSince1970: 0)
}

@Persistable(type: "FDBMemoryVectorPerson")
struct FDBMemoryVectorPersonV3: FDBMemoryVectorEntityV3 {
    #Directory<FDBMemoryVectorPersonV3>("fdb_memory_vector_migration", "persons")

    var id: String = ULID().ulidString
    var name: String
    var embedding: [Float]
    var created: Date = Date(timeIntervalSince1970: 0)
    var updated: Date = Date(timeIntervalSince1970: 0)
}

@Persistable(type: "FDBMemoryVectorOrganization")
struct FDBMemoryVectorOrganizationV1: FDBMemoryVectorEntityV1 {
    #Directory<FDBMemoryVectorOrganizationV1>("fdb_memory_vector_migration", "organizations")

    var id: String = ULID().ulidString
    var name: String
    var domain: String
    var embedding: [Float]
    var created: Date = Date(timeIntervalSince1970: 0)
    var updated: Date = Date(timeIntervalSince1970: 0)
}

@Persistable(type: "FDBMemoryVectorOrganization")
struct FDBMemoryVectorOrganizationV2: FDBMemoryVectorEntityV2 {
    #Directory<FDBMemoryVectorOrganizationV2>("fdb_memory_vector_migration", "organizations")

    var id: String = ULID().ulidString
    var name: String
    var domain: String
    var embedding: [Float]
    var created: Date = Date(timeIntervalSince1970: 0)
    var updated: Date = Date(timeIntervalSince1970: 0)
}

@Persistable(type: "FDBMemoryVectorOrganization")
struct FDBMemoryVectorOrganizationV3: FDBMemoryVectorEntityV3 {
    #Directory<FDBMemoryVectorOrganizationV3>("fdb_memory_vector_migration", "organizations")

    var id: String = ULID().ulidString
    var name: String
    var domain: String
    var embedding: [Float]
    var created: Date = Date(timeIntervalSince1970: 0)
    var updated: Date = Date(timeIntervalSince1970: 0)
}

extension FDBMemoryVectorPersonV1 {
    var label: String { name }
    var entityType: String { "persons" }
}

extension FDBMemoryVectorPersonV2 {
    var label: String { name }
    var entityType: String { "persons" }
}

extension FDBMemoryVectorPersonV3 {
    var label: String { name }
    var entityType: String { "persons" }
}

extension FDBMemoryVectorOrganizationV1 {
    var label: String { name }
    var entityType: String { "organizations" }
}

extension FDBMemoryVectorOrganizationV2 {
    var label: String { name }
    var entityType: String { "organizations" }
}

extension FDBMemoryVectorOrganizationV3 {
    var label: String { name }
    var entityType: String { "organizations" }
}

enum FDBMemoryVectorSchemaV1: VersionedSchema {
    static let versionIdentifier = Schema.Version(1, 0, 0)
    static let models: [any Persistable.Type] = [
        FDBMemoryVectorPersonV1.self,
        FDBMemoryVectorOrganizationV1.self,
    ]
}

enum FDBMemoryVectorSchemaV2: VersionedSchema {
    static let versionIdentifier = Schema.Version(2, 0, 0)
    static let models: [any Persistable.Type] = [
        FDBMemoryVectorPersonV2.self,
        FDBMemoryVectorOrganizationV2.self,
    ]
}

enum FDBMemoryVectorSchemaV3: VersionedSchema {
    static let versionIdentifier = Schema.Version(3, 0, 0)
    static let models: [any Persistable.Type] = [
        FDBMemoryVectorPersonV3.self,
        FDBMemoryVectorOrganizationV3.self,
    ]
}

enum FDBMemoryVectorAddMigrationPlan: SchemaMigrationPlan {
    static var schemas: [any VersionedSchema.Type] {
        [FDBMemoryVectorSchemaV1.self, FDBMemoryVectorSchemaV2.self]
    }

    static var stages: [MigrationStage] {
        [
            .lightweight(
                fromVersion: FDBMemoryVectorSchemaV1.self,
                toVersion: FDBMemoryVectorSchemaV2.self
            )
        ]
    }
}

enum FDBMemoryVectorRebuildMigrationPlan: SchemaMigrationPlan {
    static var schemas: [any VersionedSchema.Type] {
        [FDBMemoryVectorSchemaV2.self, FDBMemoryVectorSchemaV3.self]
    }

    static var stages: [MigrationStage] {
        [
            .custom(
                fromVersion: FDBMemoryVectorSchemaV2.self,
                toVersion: FDBMemoryVectorSchemaV3.self,
                willMigrate: rebuildEntityVectorIndex,
                didMigrate: nil
            )
        ]
    }

    static func rebuildEntityVectorIndex(context: MigrationContext) async throws {
        try await context.rebuildIndex(indexName: "Entity_vector_embedding", batchSize: 1)
    }
}

@Suite("Polymorphic Vector Migration FDB Tests", .serialized, .heartbeat)
struct PolymorphicVectorMigrationFDBTests {
    @Test("FDB Memory Entity vector descriptors stay concrete per member type")
    func fdbMemoryEntityVectorDescriptorsStayConcretePerMemberType() throws {
        let schema = FDBMemoryVectorSchemaV2.makeSchema()
        let personDescriptor = try #require(
            schema.polymorphicIndexDescriptors(
                identifier: FDBMemoryVectorPersonV2.polymorphableType,
                memberType: FDBMemoryVectorPersonV2.self
            ).first { $0.name == "Entity_vector_embedding" }
        )
        let organizationDescriptor = try #require(
            schema.polymorphicIndexDescriptors(
                identifier: FDBMemoryVectorOrganizationV2.polymorphableType,
                memberType: FDBMemoryVectorOrganizationV2.self
            ).first { $0.name == "Entity_vector_embedding" }
        )

        #expect(personDescriptor.kind is VectorIndexKind<FDBMemoryVectorPersonV2>)
        #expect(organizationDescriptor.kind is VectorIndexKind<FDBMemoryVectorOrganizationV2>)
        #expect(personDescriptor.keyPaths.first is PartialKeyPath<FDBMemoryVectorPersonV2>)
        #expect(organizationDescriptor.keyPaths.first is PartialKeyPath<FDBMemoryVectorOrganizationV2>)
        #expect(personDescriptor.keyPaths.first is PartialKeyPath<FDBMemoryVectorOrganizationV2> == false)
        #expect(organizationDescriptor.keyPaths.first is PartialKeyPath<FDBMemoryVectorPersonV2> == false)
    }

    @Test("FDB migration backfills swift-memory Entity vector index across batch boundaries")
    func fdbMigrationBackfillsSwiftMemoryEntityVectorIndexAcrossBatchBoundaries() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let engine = try await Self.makeSystemPriorityEngine()
            try await Self.clearState(in: engine)

            let initialContainer = try await DBContainer(
                for: FDBMemoryVectorSchemaV1.makeSchema(),
                configuration: .init(backend: .custom(engine)),
                security: .disabled
            )
            let initialContext = initialContainer.newContext()

            var anchor = FDBMemoryVectorPersonV1(name: "Alice", embedding: [1, 0, 0])
            anchor.id = "fdb-memory-vector-person-anchor"
            initialContext.insert(anchor)

            for offset in 0..<105 {
                var person = FDBMemoryVectorPersonV1(
                    name: "Other \(offset)",
                    embedding: [0, 1, 0]
                )
                person.id = "fdb-memory-vector-person-\(offset)"
                initialContext.insert(person)
            }

            var organization = FDBMemoryVectorOrganizationV1(
                name: "Creww",
                domain: "creww.example",
                embedding: [0.95, 0.05, 0]
            )
            organization.id = "fdb-memory-vector-organization"
            initialContext.insert(organization)

            try await initialContext.save()
            try await initialContainer.setCurrentSchemaVersion(Schema.Version(1, 0, 0))

            let migratedContainer = try await DBContainer(
                for: FDBMemoryVectorSchemaV2.self,
                migrationPlan: FDBMemoryVectorAddMigrationPlan.self,
                configuration: .init(backend: .custom(engine)),
                security: .disabled
            )
            try await migratedContainer.migrateIfNeeded()
            VectorReadBridge.registerReadExecutors()

            #expect(try await Self.countEntityVectorIndexEntries(container: migratedContainer) == 107)

            let page = try await migratedContainer.newContext()
                .findPolymorphic(FDBMemoryVectorPersonV2.self)
                .vector(\.embedding, dimensions: 3)
                .query([1, 0, 0], k: 2)
                .metric(.cosine)
                .executePage()
            let ids = Set(page.results.compactMap(Self.resultIDV2))

            #expect(ids == Set([anchor.id, organization.id]))

            let organizationStartedPage = try await migratedContainer.newContext()
                .findPolymorphic(FDBMemoryVectorOrganizationV2.self)
                .vector(\.embedding, dimensions: 3)
                .query([1, 0, 0], k: 2)
                .metric(.cosine)
                .executePage()
            let organizationStartedIDs = Set(organizationStartedPage.results.compactMap(Self.resultIDV2))

            #expect(organizationStartedIDs == Set([anchor.id, organization.id]))
        }
    }

    @Test("FDB custom migration rebuilds swift-memory Entity vector index")
    func fdbCustomMigrationRebuildsSwiftMemoryEntityVectorIndex() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let engine = try await Self.makeSystemPriorityEngine()
            try await Self.clearState(in: engine)

            let initialContainer = try await DBContainer(
                for: FDBMemoryVectorSchemaV2.makeSchema(),
                configuration: .init(backend: .custom(engine)),
                security: .disabled
            )
            let context = initialContainer.newContext()

            var person = FDBMemoryVectorPersonV2(name: "Alice", embedding: [1, 0, 0])
            person.id = "fdb-memory-vector-rebuild-person"
            var organization = FDBMemoryVectorOrganizationV2(
                name: "Creww",
                domain: "creww.example",
                embedding: [0.95, 0.05, 0]
            )
            organization.id = "fdb-memory-vector-rebuild-organization"

            try await context.savePolymorphic(person, as: FDBMemoryVectorPersonV2.self)
            try await context.savePolymorphic(organization, as: FDBMemoryVectorPersonV2.self)
            try await initialContainer.setCurrentSchemaVersion(Schema.Version(2, 0, 0))
            try await Self.clearEntityVectorIndexEntries(container: initialContainer)
            #expect(try await Self.countEntityVectorIndexEntries(container: initialContainer) == 0)

            let migratedContainer = try await DBContainer(
                for: FDBMemoryVectorSchemaV3.self,
                migrationPlan: FDBMemoryVectorRebuildMigrationPlan.self,
                configuration: .init(backend: .custom(engine)),
                security: .disabled
            )
            try await migratedContainer.migrateIfNeeded()
            VectorReadBridge.registerReadExecutors()

            let page = try await migratedContainer.newContext()
                .findPolymorphic(FDBMemoryVectorPersonV3.self)
                .vector(\.embedding, dimensions: 3)
                .query([1, 0, 0], k: 2)
                .metric(.cosine)
                .executePage()
            let ids = Set(page.results.compactMap(Self.resultIDV3))

            #expect(ids == Set([person.id, organization.id]))

            let organizationStartedPage = try await migratedContainer.newContext()
                .findPolymorphic(FDBMemoryVectorOrganizationV3.self)
                .vector(\.embedding, dimensions: 3)
                .query([1, 0, 0], k: 2)
                .metric(.cosine)
                .executePage()
            let organizationStartedIDs = Set(organizationStartedPage.results.compactMap(Self.resultIDV3))

            #expect(organizationStartedIDs == Set([person.id, organization.id]))
            #expect(try await Self.countEntityVectorIndexEntries(container: migratedContainer) == 2)
            #expect(try await Self.entityVectorIndexState(container: migratedContainer) == .readable)
        }
    }

    private static func makeSystemPriorityEngine() async throws -> any StorageEngine {
        try await FDBTestSetup.shared.initialize()
        let engine = try await FDBTestSetup.shared.makeEngine()
        let database = FDBSystemPriorityDatabase(wrapping: engine.database)
        return try await FDBStorageEngine(configuration: .init(database: database))
    }

    private static func clearState(in database: any StorageEngine) async throws {
        for path in [
            ["fdb_memory_vector_migration"],
            ["_metadata"],
        ] {
            if try await database.directoryService.exists(path: path) {
                try await database.directoryService.remove(path: path)
            }
        }

        try await database.withTransaction { transaction in
            for typeName in [
                FDBMemoryVectorPersonV1.persistableType,
                FDBMemoryVectorOrganizationV1.persistableType,
            ] {
                transaction.clear(key: Tuple(["_schema", typeName]).pack())
            }
        }
    }

    private static func resultIDV2(_ result: PolymorphicQueryResult) -> String? {
        if let person = result.item(as: FDBMemoryVectorPersonV2.self) {
            return person.id
        }
        if let organization = result.item(as: FDBMemoryVectorOrganizationV2.self) {
            return organization.id
        }
        return nil
    }

    private static func resultIDV3(_ result: PolymorphicQueryResult) -> String? {
        if let person = result.item(as: FDBMemoryVectorPersonV3.self) {
            return person.id
        }
        if let organization = result.item(as: FDBMemoryVectorOrganizationV3.self) {
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
        let group = try container.polymorphicGroup(identifier: FDBMemoryVectorPersonV2.polymorphableType)
        let groupSubspace = try await container.resolvePolymorphicDirectory(for: group.identifier)
        let stateManager = IndexStateManager(container: container, subspace: groupSubspace)
        return try await stateManager.state(of: "Entity_vector_embedding")
    }

    private static func entityVectorIndexSubspace(container: DBContainer) async throws -> Subspace {
        let group = try container.polymorphicGroup(identifier: FDBMemoryVectorPersonV2.polymorphableType)
        let groupSubspace = try await container.resolvePolymorphicDirectory(for: group.identifier)
        return groupSubspace
            .subspace(SubspaceKey.indexes)
            .subspace("Entity_vector_embedding")
    }
}
#endif
