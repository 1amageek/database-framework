#if FOUNDATION_DB
import Testing
import Foundation
import StorageKit
import FDBStorage
import FullText
import TestHeartbeat
import TestSupport
@testable import Core
@testable import DatabaseEngine

protocol FDBPolymorphicMigrationDocumentV1: Polymorphable {
    var id: String { get }
    var title: String { get }
}

extension FDBPolymorphicMigrationDocumentV1 {
    public static var polymorphableType: String { "FDBPolymorphicMigrationDocument" }

    public static var polymorphicDirectoryPathComponents: [any DirectoryPathElement] {
        [Path("polymorphic_migration_fdb_shared")]
    }
}

protocol FDBPolymorphicMigrationDocumentV2: Polymorphable {
    var id: String { get }
    var title: String { get }
}

protocol FDBPolymorphicMigrationDocumentV3: Polymorphable {
    var id: String { get }
    var title: String { get }
}

protocol FDBPolymorphicMigrationDocumentV4: Polymorphable {
    var id: String { get }
    var title: String { get }
}

extension FDBPolymorphicMigrationDocumentV2 {
    public static var polymorphableType: String { "FDBPolymorphicMigrationDocument" }

    public static var polymorphicDirectoryPathComponents: [any DirectoryPathElement] {
        [Path("polymorphic_migration_fdb_shared")]
    }

    public static var polymorphicIndexDescriptors: [IndexDescriptor] {
        [
            IndexDescriptor(
                name: "FDBPolymorphicMigrationDocument_title",
                keyPaths: [\Self.title],
                kind: ScalarIndexKind<Self>(fields: [\Self.title])
            ),
            IndexDescriptor(
                name: "FDBPolymorphicMigrationDocument_title_fulltext",
                keyPaths: [\Self.title],
                kind: FullTextIndexKind<Self>(fields: [\Self.title], tokenizer: .simple)
            ),
        ]
    }
}

extension FDBPolymorphicMigrationDocumentV3 {
    public static var polymorphableType: String { "FDBPolymorphicMigrationDocument" }

    public static var polymorphicDirectoryPathComponents: [any DirectoryPathElement] {
        [Path("polymorphic_migration_fdb_shared")]
    }
}

extension FDBPolymorphicMigrationDocumentV4 {
    public static var polymorphableType: String { "FDBPolymorphicMigrationDocument" }

    public static var polymorphicDirectoryPathComponents: [any DirectoryPathElement] {
        [Path("polymorphic_migration_fdb_shared")]
    }

    public static var polymorphicIndexDescriptors: [IndexDescriptor] {
        [
            IndexDescriptor(
                name: "FDBPolymorphicMigrationDocument_title",
                keyPaths: [\Self.title],
                kind: ScalarIndexKind<Self>(fields: [\Self.title])
            ),
            IndexDescriptor(
                name: "FDBPolymorphicMigrationDocument_title_fulltext",
                keyPaths: [\Self.title],
                kind: FullTextIndexKind<Self>(fields: [\Self.title], tokenizer: .simple)
            ),
        ]
    }
}

@Persistable(type: "FDBPolymorphicMigrationArticle")
struct FDBPolymorphicMigrationArticleV1: FDBPolymorphicMigrationDocumentV1 {
    #Directory<FDBPolymorphicMigrationArticleV1>("polymorphic_migration_fdb_articles")

    var id: String = ULID().ulidString
    var title: String
    var body: String
}

@Persistable(type: "FDBPolymorphicMigrationArticle")
struct FDBPolymorphicMigrationArticleV2: FDBPolymorphicMigrationDocumentV2 {
    #Directory<FDBPolymorphicMigrationArticleV2>("polymorphic_migration_fdb_articles")

    var id: String = ULID().ulidString
    var title: String
    var body: String
}

@Persistable(type: "FDBPolymorphicMigrationArticle")
struct FDBPolymorphicMigrationArticleV3: FDBPolymorphicMigrationDocumentV3 {
    #Directory<FDBPolymorphicMigrationArticleV3>("polymorphic_migration_fdb_articles")

    var id: String = ULID().ulidString
    var title: String
    var body: String
}

@Persistable(type: "FDBPolymorphicMigrationArticle")
struct FDBPolymorphicMigrationArticleV4: FDBPolymorphicMigrationDocumentV4 {
    #Directory<FDBPolymorphicMigrationArticleV4>("polymorphic_migration_fdb_articles")

    var id: String = ULID().ulidString
    var title: String
    var body: String
}

@Persistable(type: "FDBPolymorphicMigrationReport")
struct FDBPolymorphicMigrationReportV1: FDBPolymorphicMigrationDocumentV1 {
    #Directory<FDBPolymorphicMigrationReportV1>("polymorphic_migration_fdb_reports")

    var id: String = ULID().ulidString
    var title: String
    var pageCount: Int
}

@Persistable(type: "FDBPolymorphicMigrationReport")
struct FDBPolymorphicMigrationReportV2: FDBPolymorphicMigrationDocumentV2 {
    #Directory<FDBPolymorphicMigrationReportV2>("polymorphic_migration_fdb_reports")

    var id: String = ULID().ulidString
    var title: String
    var pageCount: Int
}

@Persistable(type: "FDBPolymorphicMigrationReport")
struct FDBPolymorphicMigrationReportV3: FDBPolymorphicMigrationDocumentV3 {
    #Directory<FDBPolymorphicMigrationReportV3>("polymorphic_migration_fdb_reports")

    var id: String = ULID().ulidString
    var title: String
    var pageCount: Int
}

@Persistable(type: "FDBPolymorphicMigrationReport")
struct FDBPolymorphicMigrationReportV4: FDBPolymorphicMigrationDocumentV4 {
    #Directory<FDBPolymorphicMigrationReportV4>("polymorphic_migration_fdb_reports")

    var id: String = ULID().ulidString
    var title: String
    var pageCount: Int
}

enum FDBPolymorphicMigrationSchemaV1: VersionedSchema {
    static let versionIdentifier = Schema.Version(1, 0, 0)

    static let models: [any Persistable.Type] = [
        FDBPolymorphicMigrationArticleV1.self,
        FDBPolymorphicMigrationReportV1.self,
    ]
}

enum FDBPolymorphicMigrationSchemaV2: VersionedSchema {
    static let versionIdentifier = Schema.Version(2, 0, 0)

    static let models: [any Persistable.Type] = [
        FDBPolymorphicMigrationArticleV2.self,
        FDBPolymorphicMigrationReportV2.self,
    ]
}

enum FDBPolymorphicMigrationSchemaV3: VersionedSchema {
    static let versionIdentifier = Schema.Version(3, 0, 0)

    static let models: [any Persistable.Type] = [
        FDBPolymorphicMigrationArticleV3.self,
        FDBPolymorphicMigrationReportV3.self,
    ]
}

enum FDBPolymorphicMigrationSchemaV4: VersionedSchema {
    static let versionIdentifier = Schema.Version(4, 0, 0)

    static let models: [any Persistable.Type] = [
        FDBPolymorphicMigrationArticleV4.self,
        FDBPolymorphicMigrationReportV4.self,
    ]
}

enum FDBPolymorphicMigrationPlan: SchemaMigrationPlan {
    static var schemas: [any VersionedSchema.Type] {
        [
            FDBPolymorphicMigrationSchemaV1.self,
            FDBPolymorphicMigrationSchemaV2.self,
        ]
    }

    static var stages: [MigrationStage] {
        [
            .lightweight(
                fromVersion: FDBPolymorphicMigrationSchemaV1.self,
                toVersion: FDBPolymorphicMigrationSchemaV2.self
            )
        ]
    }
}

enum FDBPolymorphicRemovalMigrationPlan: SchemaMigrationPlan {
    static var schemas: [any VersionedSchema.Type] {
        [
            FDBPolymorphicMigrationSchemaV2.self,
            FDBPolymorphicMigrationSchemaV3.self,
        ]
    }

    static var stages: [MigrationStage] {
        [
            .lightweight(
                fromVersion: FDBPolymorphicMigrationSchemaV2.self,
                toVersion: FDBPolymorphicMigrationSchemaV3.self
            )
        ]
    }
}

enum FDBPolymorphicRebuildMigrationPlan: SchemaMigrationPlan {
    static var schemas: [any VersionedSchema.Type] {
        [
            FDBPolymorphicMigrationSchemaV2.self,
            FDBPolymorphicMigrationSchemaV4.self,
        ]
    }

    static var stages: [MigrationStage] {
        [
            .custom(
                fromVersion: FDBPolymorphicMigrationSchemaV2.self,
                toVersion: FDBPolymorphicMigrationSchemaV4.self,
                willMigrate: rebuildPolymorphicIndexes,
                didMigrate: nil
            )
        ]
    }

    static func rebuildPolymorphicIndexes(context: MigrationContext) async throws {
        try await context.rebuildIndex(
            indexName: "FDBPolymorphicMigrationDocument_title",
            batchSize: 1
        )
        try await context.rebuildIndex(
            indexName: "FDBPolymorphicMigrationDocument_title_fulltext",
            batchSize: 1
        )
    }
}

@Suite("Polymorphic Migration FDB Tests", .serialized, .heartbeat)
struct PolymorphicMigrationFDBTests {
    @Test("FDB migration backfills added polymorphic indexes and keeps them maintained")
    func fdbMigrationBackfillsAddedPolymorphicIndexesAndKeepsThemMaintained() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let engine = try await Self.makeSystemPriorityEngine()
            try await Self.clearState(in: engine)

            let initialContainer = try await DBContainer(
                for: FDBPolymorphicMigrationSchemaV1.makeSchema(),
                configuration: .init(backend: .custom(engine)),
                security: .disabled
            )
            let initialContext = initialContainer.newContext()

            var article = FDBPolymorphicMigrationArticleV1(title: "Legacy Needle Article", body: "Body")
            article.id = "fdb-polymorphic-migration-article"
            var report = FDBPolymorphicMigrationReportV1(title: "Legacy Needle Report", pageCount: 8)
            report.id = "fdb-polymorphic-migration-report"

            initialContext.insert(article)
            initialContext.insert(report)
            try await initialContext.save()
            try await initialContainer.setCurrentSchemaVersion(Schema.Version(1, 0, 0))

            let migratedContainer = try await DBContainer(
                for: FDBPolymorphicMigrationSchemaV2.self,
                migrationPlan: FDBPolymorphicMigrationPlan.self,
                configuration: .init(backend: .custom(engine)),
                security: .disabled
            )
            try await migratedContainer.migrateIfNeeded()

            let verificationContainer = try await DBContainer(
                for: FDBPolymorphicMigrationSchemaV2.makeSchema(),
                configuration: .init(backend: .custom(engine)),
                security: .disabled
            )
            let verificationContext = verificationContainer.newContext()
            let migratedResults = try await verificationContext
                .findPolymorphic(FDBPolymorphicMigrationArticleV2.self)
                .fullText(\.title)
                .term("needle")
                .execute()
            let migratedIDs = Set(migratedResults.compactMap(Self.resultID))

            #expect(migratedIDs == Set([article.id, report.id]))
            #expect(try await Self.countPolymorphicIndexEntries(
                container: verificationContainer,
                indexName: "FDBPolymorphicMigrationDocument_title"
            ) == 2)

            var updatedReport = FDBPolymorphicMigrationReportV2(
                title: "Migrated Beacon Report",
                pageCount: report.pageCount
            )
            updatedReport.id = report.id
            try await verificationContext.savePolymorphic(
                updatedReport,
                as: FDBPolymorphicMigrationArticleV2.self
            )

            let afterUpdateNeedle = try await verificationContext
                .findPolymorphic(FDBPolymorphicMigrationArticleV2.self)
                .fullText(\.title)
                .term("needle")
                .execute()
            let afterUpdateBeacon = try await verificationContext
                .findPolymorphic(FDBPolymorphicMigrationArticleV2.self)
                .fullText(\.title)
                .term("beacon")
                .execute()

            #expect(Set(afterUpdateNeedle.compactMap(Self.resultID)) == Set([article.id]))
            #expect(Set(afterUpdateBeacon.compactMap(Self.resultID)) == Set([report.id]))
            #expect(try await Self.countPolymorphicIndexEntries(
                container: verificationContainer,
                indexName: "FDBPolymorphicMigrationDocument_title"
            ) == 2)
        }
    }

    @Test("FDB migration removes polymorphic index data and disables index state")
    func fdbMigrationRemovesPolymorphicIndexDataAndDisablesIndexState() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let engine = try await Self.makeSystemPriorityEngine()
            try await Self.clearState(in: engine)

            let initialContainer = try await DBContainer(
                for: FDBPolymorphicMigrationSchemaV2.makeSchema(),
                configuration: .init(backend: .custom(engine)),
                security: .disabled
            )
            let initialContext = initialContainer.newContext()

            var article = FDBPolymorphicMigrationArticleV2(title: "Removal Needle Article", body: "Body")
            article.id = "fdb-polymorphic-removal-article"
            var report = FDBPolymorphicMigrationReportV2(title: "Removal Needle Report", pageCount: 8)
            report.id = "fdb-polymorphic-removal-report"

            try await initialContext.savePolymorphic(article, as: FDBPolymorphicMigrationArticleV2.self)
            try await initialContext.savePolymorphic(report, as: FDBPolymorphicMigrationArticleV2.self)
            try await initialContainer.setCurrentSchemaVersion(Schema.Version(2, 0, 0))

            #expect(try await Self.countPolymorphicIndexEntries(
                container: initialContainer,
                indexName: "FDBPolymorphicMigrationDocument_title"
            ) == 2)

            let migratedContainer = try await DBContainer(
                for: FDBPolymorphicMigrationSchemaV3.self,
                migrationPlan: FDBPolymorphicRemovalMigrationPlan.self,
                configuration: .init(backend: .custom(engine)),
                security: .disabled
            )
            try await migratedContainer.migrateIfNeeded()

            #expect(try await Self.countPolymorphicIndexEntries(
                container: migratedContainer,
                indexName: "FDBPolymorphicMigrationDocument_title"
            ) == 0)
            #expect(try await Self.polymorphicIndexState(
                container: migratedContainer,
                indexName: "FDBPolymorphicMigrationDocument_title"
            ) == .disabled)

            let postRemovalContext = migratedContainer.newContext()
            var postRemovalArticle = FDBPolymorphicMigrationArticleV3(
                title: "Removal Needle After",
                body: "Body"
            )
            postRemovalArticle.id = "fdb-polymorphic-removal-after"
            try await postRemovalContext.savePolymorphic(
                postRemovalArticle,
                as: FDBPolymorphicMigrationArticleV3.self
            )

            let fetched = try await postRemovalContext.fetchPolymorphic(
                FDBPolymorphicMigrationArticleV3.self
            )
            #expect(fetched.count == 3)
            #expect(try await Self.countPolymorphicIndexEntries(
                container: migratedContainer,
                indexName: "FDBPolymorphicMigrationDocument_title"
            ) == 0)
        }
    }

    @Test("FDB custom migration rebuilds corrupted polymorphic indexes")
    func fdbCustomMigrationRebuildsCorruptedPolymorphicIndexes() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let engine = try await Self.makeSystemPriorityEngine()
            try await Self.clearState(in: engine)

            let initialContainer = try await DBContainer(
                for: FDBPolymorphicMigrationSchemaV2.makeSchema(),
                configuration: .init(backend: .custom(engine)),
                security: .disabled
            )
            let initialContext = initialContainer.newContext()

            var article = FDBPolymorphicMigrationArticleV2(title: "Rebuild Needle Article", body: "Body")
            article.id = "fdb-polymorphic-rebuild-article"
            var report = FDBPolymorphicMigrationReportV2(title: "Rebuild Needle Report", pageCount: 5)
            report.id = "fdb-polymorphic-rebuild-report"

            try await initialContext.savePolymorphic(article, as: FDBPolymorphicMigrationArticleV2.self)
            try await initialContext.savePolymorphic(report, as: FDBPolymorphicMigrationArticleV2.self)
            try await initialContainer.setCurrentSchemaVersion(Schema.Version(2, 0, 0))

            try await Self.clearPolymorphicIndexEntries(
                container: initialContainer,
                indexName: "FDBPolymorphicMigrationDocument_title"
            )
            try await Self.clearPolymorphicIndexEntries(
                container: initialContainer,
                indexName: "FDBPolymorphicMigrationDocument_title_fulltext"
            )
            #expect(try await Self.countPolymorphicIndexEntries(
                container: initialContainer,
                indexName: "FDBPolymorphicMigrationDocument_title"
            ) == 0)

            let migratedContainer = try await DBContainer(
                for: FDBPolymorphicMigrationSchemaV4.self,
                migrationPlan: FDBPolymorphicRebuildMigrationPlan.self,
                configuration: .init(backend: .custom(engine)),
                security: .disabled
            )
            try await migratedContainer.migrateIfNeeded()

            let verificationContext = migratedContainer.newContext()
            let rebuiltResults = try await verificationContext
                .findPolymorphic(FDBPolymorphicMigrationArticleV4.self)
                .fullText(\.title)
                .term("needle")
                .execute()
            let rebuiltIDs = Set(rebuiltResults.compactMap(Self.resultIDV4))

            #expect(rebuiltIDs == Set([article.id, report.id]))
            #expect(try await Self.countPolymorphicIndexEntries(
                container: migratedContainer,
                indexName: "FDBPolymorphicMigrationDocument_title"
            ) == 2)
            #expect(try await Self.polymorphicIndexState(
                container: migratedContainer,
                indexName: "FDBPolymorphicMigrationDocument_title"
            ) == .readable)
        }
    }

    private static func makeSystemPriorityEngine() async throws -> any StorageEngine {
        try await FDBTestEnvironment.shared.ensureInitialized()
        let engine = try await FDBTestSetup.shared.makeEngine()
        let database = FDBSystemPriorityDatabase(wrapping: engine.database)
        return try await FDBStorageEngine(configuration: .init(database: database))
    }

    private static func clearState(in database: any StorageEngine) async throws {
        for path in [
            ["polymorphic_migration_fdb_articles"],
            ["polymorphic_migration_fdb_reports"],
            ["polymorphic_migration_fdb_shared"],
            ["_metadata"],
        ] {
            do {
                try await database.directoryService.remove(path: path)
            } catch {
            }
        }

        try await database.withTransaction { transaction in
            for typeName in [
                FDBPolymorphicMigrationArticleV1.persistableType,
                FDBPolymorphicMigrationReportV1.persistableType,
            ] {
                transaction.clear(key: Tuple(["_schema", typeName]).pack())
            }
        }
    }

    private static func resultID(_ result: PolymorphicQueryResult) -> String? {
        if let article = result.item(as: FDBPolymorphicMigrationArticleV2.self) {
            return article.id
        }
        if let report = result.item(as: FDBPolymorphicMigrationReportV2.self) {
            return report.id
        }
        return nil
    }

    private static func resultIDV4(_ result: PolymorphicQueryResult) -> String? {
        if let article = result.item(as: FDBPolymorphicMigrationArticleV4.self) {
            return article.id
        }
        if let report = result.item(as: FDBPolymorphicMigrationReportV4.self) {
            return report.id
        }
        return nil
    }

    private static func countPolymorphicIndexEntries(
        container: DBContainer,
        indexName: String
    ) async throws -> Int {
        let group = try container.polymorphicGroup(
            identifier: FDBPolymorphicMigrationArticleV2.polymorphableType
        )
        let groupSubspace = try await container.resolvePolymorphicDirectory(for: group.identifier)
        let indexSubspace = groupSubspace
            .subspace(SubspaceKey.indexes)
            .subspace(indexName)

        return try await container.engine.withTransaction { transaction -> Int in
            let (begin, end) = indexSubspace.range()
            var count = 0
            for try await _ in transaction.getRange(begin: begin, end: end, snapshot: true) {
                count += 1
            }
            return count
        }
    }

    private static func clearPolymorphicIndexEntries(
        container: DBContainer,
        indexName: String
    ) async throws {
        let group = try container.polymorphicGroup(
            identifier: FDBPolymorphicMigrationArticleV2.polymorphableType
        )
        let groupSubspace = try await container.resolvePolymorphicDirectory(for: group.identifier)
        let indexSubspace = groupSubspace
            .subspace(SubspaceKey.indexes)
            .subspace(indexName)
        let range = indexSubspace.range()

        try await container.engine.withTransaction { transaction in
            transaction.clearRange(beginKey: range.begin, endKey: range.end)
        }
    }

    private static func polymorphicIndexState(
        container: DBContainer,
        indexName: String
    ) async throws -> IndexState {
        let group = try container.polymorphicGroup(
            identifier: FDBPolymorphicMigrationArticleV2.polymorphableType
        )
        let groupSubspace = try await container.resolvePolymorphicDirectory(for: group.identifier)
        let stateManager = IndexStateManager(container: container, subspace: groupSubspace)
        return try await stateManager.state(of: indexName)
    }
}
#endif
