#if FOUNDATION_DB
import Testing
import Foundation
import StorageKit
import FDBStorage
import Core
import DatabaseValue
import Vector
import TestHeartbeat
import TestSupport
@testable import DatabaseEngine
@testable import VectorIndex
import DatabaseRuntime

protocol FDBPolymorphicVectorEntityV1: Polymorphable {
    var id: String { get }
    var label: String { get }
    var entityType: String { get }
    var embedding: [Float] { get }
}

protocol FDBPolymorphicVectorEntityV2: Polymorphable {
    var id: String { get }
    var label: String { get }
    var entityType: String { get }
    var embedding: [Float] { get }
}

protocol FDBPolymorphicVectorEntityV3: Polymorphable {
    var id: String { get }
    var label: String { get }
    var entityType: String { get }
    var embedding: [Float] { get }
}

extension FDBPolymorphicVectorEntityV1 {
    public static var polymorphableType: String { "Entity" }

    public static var polymorphicDirectoryPathComponents: [DirectoryPathComponent] {
        [.staticPath("fdb_polymorphic_vector_migration"), .staticPath("entities")]
    }
}

extension FDBPolymorphicVectorEntityV2 {
    public static var polymorphableType: String { "Entity" }

    public static var polymorphicDirectoryPathComponents: [DirectoryPathComponent] {
        [.staticPath("fdb_polymorphic_vector_migration"), .staticPath("entities")]
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

extension FDBPolymorphicVectorEntityV3 {
    public static var polymorphableType: String { "Entity" }

    public static var polymorphicDirectoryPathComponents: [DirectoryPathComponent] {
        [.staticPath("fdb_polymorphic_vector_migration"), .staticPath("entities")]
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

@Persistable(type: "FDBPolymorphicVectorPerson")
struct FDBPolymorphicVectorPersonV1: FDBPolymorphicVectorEntityV1 {
    #Directory<FDBPolymorphicVectorPersonV1>("fdb_polymorphic_vector_migration", "persons")

    var id: String = ULID().ulidString
    var name: String
    var embedding: [Float]
    var created: Date = Date(timeIntervalSince1970: 0)
    var updated: Date = Date(timeIntervalSince1970: 0)
}

@Persistable(type: "FDBPolymorphicVectorPerson")
struct FDBPolymorphicVectorPersonV2: FDBPolymorphicVectorEntityV2 {
    #Directory<FDBPolymorphicVectorPersonV2>("fdb_polymorphic_vector_migration", "persons")

    var id: String = ULID().ulidString
    var name: String
    var embedding: [Float]
    var created: Date = Date(timeIntervalSince1970: 0)
    var updated: Date = Date(timeIntervalSince1970: 0)
}

@Persistable(type: "FDBPolymorphicVectorPerson")
struct FDBPolymorphicVectorPersonV3: FDBPolymorphicVectorEntityV3 {
    #Directory<FDBPolymorphicVectorPersonV3>("fdb_polymorphic_vector_migration", "persons")

    var id: String = ULID().ulidString
    var name: String
    var embedding: [Float]
    var created: Date = Date(timeIntervalSince1970: 0)
    var updated: Date = Date(timeIntervalSince1970: 0)
}

@Persistable(type: "FDBPolymorphicVectorOrganization")
struct FDBPolymorphicVectorOrganizationV1: FDBPolymorphicVectorEntityV1 {
    #Directory<FDBPolymorphicVectorOrganizationV1>("fdb_polymorphic_vector_migration", "organizations")

    var id: String = ULID().ulidString
    var name: String
    var domain: String
    var embedding: [Float]
    var created: Date = Date(timeIntervalSince1970: 0)
    var updated: Date = Date(timeIntervalSince1970: 0)
}

@Persistable(type: "FDBPolymorphicVectorOrganization")
struct FDBPolymorphicVectorOrganizationV2: FDBPolymorphicVectorEntityV2 {
    #Directory<FDBPolymorphicVectorOrganizationV2>("fdb_polymorphic_vector_migration", "organizations")

    var id: String = ULID().ulidString
    var name: String
    var domain: String
    var embedding: [Float]
    var created: Date = Date(timeIntervalSince1970: 0)
    var updated: Date = Date(timeIntervalSince1970: 0)
}

@Persistable(type: "FDBPolymorphicVectorOrganization")
struct FDBPolymorphicVectorOrganizationV3: FDBPolymorphicVectorEntityV3 {
    #Directory<FDBPolymorphicVectorOrganizationV3>("fdb_polymorphic_vector_migration", "organizations")

    var id: String = ULID().ulidString
    var name: String
    var domain: String
    var embedding: [Float]
    var created: Date = Date(timeIntervalSince1970: 0)
    var updated: Date = Date(timeIntervalSince1970: 0)
}

extension FDBPolymorphicVectorPersonV1 {
    var label: String { name }
    var entityType: String { "persons" }
}

extension FDBPolymorphicVectorPersonV2 {
    var label: String { name }
    var entityType: String { "persons" }
}

extension FDBPolymorphicVectorPersonV3 {
    var label: String { name }
    var entityType: String { "persons" }
}

extension FDBPolymorphicVectorOrganizationV1 {
    var label: String { name }
    var entityType: String { "organizations" }
}

extension FDBPolymorphicVectorOrganizationV2 {
    var label: String { name }
    var entityType: String { "organizations" }
}

extension FDBPolymorphicVectorOrganizationV3 {
    var label: String { name }
    var entityType: String { "organizations" }
}

enum FDBPolymorphicVectorSchemaV1: VersionedSchema {
    static let versionIdentifier = Schema.Version(1, 0, 0)
    static let models: [any Persistable.Type] = [
        FDBPolymorphicVectorPersonV1.self,
        FDBPolymorphicVectorOrganizationV1.self,
    ]
}

enum FDBPolymorphicVectorSchemaV2: VersionedSchema {
    static let versionIdentifier = Schema.Version(2, 0, 0)
    static let models: [any Persistable.Type] = [
        FDBPolymorphicVectorPersonV2.self,
        FDBPolymorphicVectorOrganizationV2.self,
    ]
}

enum FDBPolymorphicVectorSchemaV3: VersionedSchema {
    static let versionIdentifier = Schema.Version(3, 0, 0)
    static let models: [any Persistable.Type] = [
        FDBPolymorphicVectorPersonV3.self,
        FDBPolymorphicVectorOrganizationV3.self,
    ]
}

enum FDBPolymorphicVectorAddMigrationPlan: SchemaMigrationPlan {
    static var schemas: [any VersionedSchema.Type] {
        [FDBPolymorphicVectorSchemaV1.self, FDBPolymorphicVectorSchemaV2.self]
    }

    static var stages: [MigrationStage] {
        [
            .lightweight(
                fromVersion: FDBPolymorphicVectorSchemaV1.self,
                toVersion: FDBPolymorphicVectorSchemaV2.self
            )
        ]
    }
}

enum FDBPolymorphicVectorRebuildMigrationPlan: SchemaMigrationPlan {
    static var schemas: [any VersionedSchema.Type] {
        [FDBPolymorphicVectorSchemaV2.self, FDBPolymorphicVectorSchemaV3.self]
    }

    static var stages: [MigrationStage] {
        [
            .custom(
                fromVersion: FDBPolymorphicVectorSchemaV2.self,
                toVersion: FDBPolymorphicVectorSchemaV3.self,
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
    @Test("FDB Memory Entity vector descriptors decode canonically for each member")
    func fdbMemoryEntityVectorDescriptorsStayConcretePerMemberType() throws {
        let schema = FDBPolymorphicVectorSchemaV2.makeSchema()
        let personDescriptor = try #require(
            schema.polymorphicIndexDescriptors(
                identifier: FDBPolymorphicVectorPersonV2.polymorphableType,
                memberType: FDBPolymorphicVectorPersonV2.self
            ).first { $0.name == "Entity_vector_embedding" }
        )
        let organizationDescriptor = try #require(
            schema.polymorphicIndexDescriptors(
                identifier: FDBPolymorphicVectorOrganizationV2.polymorphableType,
                memberType: FDBPolymorphicVectorOrganizationV2.self
            ).first { $0.name == "Entity_vector_embedding" }
        )

        let personKind = try VectorIndexKind<FDBPolymorphicVectorPersonV2>(
            canonical: personDescriptor.kind
        )
        let organizationKind = try VectorIndexKind<FDBPolymorphicVectorOrganizationV2>(
            canonical: organizationDescriptor.kind
        )
        #expect(personKind.fieldNames == ["embedding"])
        #expect(organizationKind.fieldNames == ["embedding"])
        #expect(personKind.dimensions == 3)
        #expect(organizationKind.dimensions == 3)
    }

    @Test("FDB migration backfills polymorphic entity vector index across batch boundaries")
    func fdbMigrationBackfillsPolymorphicEntityVectorIndexAcrossBatchBoundaries() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let engine = try await Self.makeSystemPriorityEngine()
            try await Self.clearState(in: engine)

            let initialContainer = try await DBContainer.open(
                for: FDBPolymorphicVectorSchemaV1.makeSchema(),
                configuration: .init(backend: .custom(engine)),
                runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(),
                security: .disabled
            )
            let initialContext = initialContainer.newContext()

            var anchor = FDBPolymorphicVectorPersonV1(name: "Alice", embedding: [1, 0, 0])
            anchor.id = "fdb-polymorphic-vector-person-anchor"
            try initialContext.insert(anchor)

            for offset in 0..<105 {
                var person = FDBPolymorphicVectorPersonV1(
                    name: "Other \(offset)",
                    embedding: [0, 1, 0]
                )
                person.id = "fdb-polymorphic-vector-person-\(offset)"
                try initialContext.insert(person)
            }

            var organization = FDBPolymorphicVectorOrganizationV1(
                name: "Creww",
                domain: "creww.example",
                embedding: [0.95, 0.05, 0]
            )
            organization.id = "fdb-polymorphic-vector-organization"
            try initialContext.insert(organization)

            try await initialContext.save()
            try await initialContainer.installSchemaSnapshot(for: Schema.Version(1, 0, 0))

            let migratedContainer = try await DBContainer.open(
                for: FDBPolymorphicVectorSchemaV2.self,
                migrationPlan: FDBPolymorphicVectorAddMigrationPlan.self,
                configuration: .init(backend: .custom(engine)),
                runtimeConfiguration: try Self.vectorRuntimeConfiguration(),
                security: .disabled
            )
            try await migratedContainer.migrateIfNeeded()

            #expect(try await Self.countEntityVectorIndexEntries(container: migratedContainer) == 107)

            let page = try await migratedContainer.newContext()
                .findPolymorphic(FDBPolymorphicVectorPersonV2.self)
                .vector(\.embedding, dimensions: 3)
                .query([1, 0, 0], k: 2)
                .metric(.cosine)
                .executePage()
            let ids = Set(page.results.compactMap(Self.resultIDV2))

            #expect(ids == Set([anchor.id, organization.id]))

            let organizationStartedPage = try await migratedContainer.newContext()
                .findPolymorphic(FDBPolymorphicVectorOrganizationV2.self)
                .vector(\.embedding, dimensions: 3)
                .query([1, 0, 0], k: 2)
                .metric(.cosine)
                .executePage()
            let organizationStartedIDs = Set(organizationStartedPage.results.compactMap(Self.resultIDV2))

            #expect(organizationStartedIDs == Set([anchor.id, organization.id]))
        }
    }

    @Test("FDB custom migration rebuilds polymorphic entity vector index")
    func fdbCustomMigrationRebuildsPolymorphicEntityVectorIndex() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let engine = try await Self.makeSystemPriorityEngine()
            try await Self.clearState(in: engine)

            let initialContainer = try await DBContainer.open(
                for: FDBPolymorphicVectorSchemaV2.makeSchema(),
                configuration: .init(backend: .custom(engine)),
                runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(),
                security: .disabled
            )
            let context = initialContainer.newContext()

            var person = FDBPolymorphicVectorPersonV2(name: "Alice", embedding: [1, 0, 0])
            person.id = "fdb-polymorphic-vector-rebuild-person"
            var organization = FDBPolymorphicVectorOrganizationV2(
                name: "Creww",
                domain: "creww.example",
                embedding: [0.95, 0.05, 0]
            )
            organization.id = "fdb-polymorphic-vector-rebuild-organization"

            try context.upsert(person)
            try context.upsert(organization)
            try await context.save()
            try await initialContainer.installSchemaSnapshot(for: Schema.Version(2, 0, 0))
            try await Self.clearEntityVectorIndexEntries(container: initialContainer)
            #expect(try await Self.countEntityVectorIndexEntries(container: initialContainer) == 0)

            let migratedContainer = try await DBContainer.open(
                for: FDBPolymorphicVectorSchemaV3.self,
                migrationPlan: FDBPolymorphicVectorRebuildMigrationPlan.self,
                configuration: .init(backend: .custom(engine)),
                runtimeConfiguration: try Self.vectorRuntimeConfiguration(),
                security: .disabled
            )
            try await migratedContainer.migrateIfNeeded()

            let page = try await migratedContainer.newContext()
                .findPolymorphic(FDBPolymorphicVectorPersonV3.self)
                .vector(\.embedding, dimensions: 3)
                .query([1, 0, 0], k: 2)
                .metric(.cosine)
                .executePage()
            let ids = Set(page.results.compactMap(Self.resultIDV3))

            #expect(ids == Set([person.id, organization.id]))

            let organizationStartedPage = try await migratedContainer.newContext()
                .findPolymorphic(FDBPolymorphicVectorOrganizationV3.self)
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

    private static func vectorRuntimeConfiguration() throws -> DatabaseRuntimeConfiguration {
        try DatabaseRuntimeConfiguration(
            indexMaintainerProviders: [
                VectorIndexMaintainerProvider()
            ],
            indexReadExecutors: [VectorReadExecutors.indexExecutor],
            polymorphicIndexReadExecutors: [VectorReadExecutors.polymorphicIndexExecutor]
        )
    }

    private static func makeSystemPriorityEngine() async throws -> any StorageEngine {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let engine = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let database = FDBSystemPriorityDatabase(wrapping: engine.database)
        return try await FDBStorageEngine(configuration: .init(database: database))
    }

    private static func clearState(in database: any StorageEngine) async throws {
        for path in [
            ["fdb_polymorphic_vector_migration"],
            ["_metadata"],
        ] {
            if try await database.directoryExists(path: path) {
                try await database.removeDirectory(path: path)
            }
        }

        try await database.withTransaction { transaction in
            for typeName in [
                FDBPolymorphicVectorPersonV1.persistableType,
                FDBPolymorphicVectorOrganizationV1.persistableType,
            ] {
                try transaction.clear(key: Tuple(["_schema", typeName]).pack())
            }
        }
    }

    private static func resultIDV2(_ result: PolymorphicQueryResult) -> String? {
        if let person = result.item(as: FDBPolymorphicVectorPersonV2.self) {
            return person.id
        }
        if let organization = result.item(as: FDBPolymorphicVectorOrganizationV2.self) {
            return organization.id
        }
        return nil
    }

    private static func resultIDV3(_ result: PolymorphicQueryResult) -> String? {
        if let person = result.item(as: FDBPolymorphicVectorPersonV3.self) {
            return person.id
        }
        if let organization = result.item(as: FDBPolymorphicVectorOrganizationV3.self) {
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
        let group = try container.polymorphicGroup(identifier: FDBPolymorphicVectorPersonV2.polymorphableType)
        let groupSubspace = try await container.resolvePolymorphicDirectory(for: group.identifier)
        let lifecycleStore = IndexLifecycleStore(container: container, subspace: groupSubspace)
        return try await lifecycleStore.state(of: "Entity_vector_embedding")
    }

    private static func entityVectorIndexSubspace(container: DBContainer) async throws -> Subspace {
        let group = try container.polymorphicGroup(identifier: FDBPolymorphicVectorPersonV2.polymorphableType)
        let groupSubspace = try await container.resolvePolymorphicDirectory(for: group.identifier)
        return groupSubspace
            .subspace(SubspaceKey.indexes)
            .subspace("Entity_vector_embedding")
    }
}
#endif
