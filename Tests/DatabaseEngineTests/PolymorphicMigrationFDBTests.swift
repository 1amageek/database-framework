#if !os(WASI)
#if FOUNDATION_DB
import Testing
import Foundation
import StorageKit
import FDBStorage
import DatabaseKit
import TestHeartbeat
import TestSupport
@testable import DatabaseKit
@testable import DatabaseEngine
import DatabaseRuntime

@Polymorphable(identifier: "FDBPolymorphicMigrationDocument")
@PolymorphicDirectory("polymorphic_migration_fdb_shared")
protocol FDBPolymorphicMigrationDocumentV1:
    Polymorphable<FDBPolymorphicMigrationDocumentV1PolymorphicGroup>
{
    var id: String { get }
    var title: String { get }
}

@Polymorphable(identifier: "FDBPolymorphicMigrationDocument")
@PolymorphicDirectory("polymorphic_migration_fdb_shared")
@PolymorphicIndex(
    .scalar,
    fields: ["title"],
    name: "FDBPolymorphicMigrationDocument_title"
)
@PolymorphicIndex(
    .fullText(tokenizer: .simple),
    fields: ["title"],
    name: "FDBPolymorphicMigrationDocument_title_fulltext"
)
protocol FDBPolymorphicMigrationDocumentV2:
    Polymorphable<FDBPolymorphicMigrationDocumentV2PolymorphicGroup>
{
    var id: String { get }
    var title: String { get }
}

@Polymorphable(identifier: "FDBPolymorphicMigrationDocument")
@PolymorphicDirectory("polymorphic_migration_fdb_shared")
protocol FDBPolymorphicMigrationDocumentV3:
    Polymorphable<FDBPolymorphicMigrationDocumentV3PolymorphicGroup>
{
    var id: String { get }
    var title: String { get }
}

@Polymorphable(identifier: "FDBPolymorphicMigrationDocument")
@PolymorphicDirectory("polymorphic_migration_fdb_shared")
@PolymorphicIndex(
    .scalar,
    fields: ["title"],
    name: "FDBPolymorphicMigrationDocument_title"
)
@PolymorphicIndex(
    .fullText(tokenizer: .simple),
    fields: ["title"],
    name: "FDBPolymorphicMigrationDocument_title_fulltext"
)
protocol FDBPolymorphicMigrationDocumentV4:
    Polymorphable<FDBPolymorphicMigrationDocumentV4PolymorphicGroup>
{
    var id: String { get }
    var title: String { get }
}

@Persistable(type: "FDBPolymorphicMigrationArticle")
struct FDBPolymorphicMigrationArticleV1: FDBPolymorphicMigrationDocumentV1 {
    #Directory<FDBPolymorphicMigrationArticleV1>("polymorphic_migration_fdb_articles")

    var id: String = UUID().uuidString
    var title: String
    var body: String
}

@Persistable(type: "FDBPolymorphicMigrationArticle")
struct FDBPolymorphicMigrationArticleV2: FDBPolymorphicMigrationDocumentV2 {
    #Directory<FDBPolymorphicMigrationArticleV2>("polymorphic_migration_fdb_articles")

    var id: String = UUID().uuidString
    var title: String
    var body: String
}

@Persistable(type: "FDBPolymorphicMigrationArticle")
struct FDBPolymorphicMigrationArticleV3: FDBPolymorphicMigrationDocumentV3 {
    #Directory<FDBPolymorphicMigrationArticleV3>("polymorphic_migration_fdb_articles")

    var id: String = UUID().uuidString
    var title: String
    var body: String
}

@Persistable(type: "FDBPolymorphicMigrationArticle")
struct FDBPolymorphicMigrationArticleV4: FDBPolymorphicMigrationDocumentV4 {
    #Directory<FDBPolymorphicMigrationArticleV4>("polymorphic_migration_fdb_articles")

    var id: String = UUID().uuidString
    var title: String
    var body: String
}

@Persistable(type: "FDBPolymorphicMigrationReport")
struct FDBPolymorphicMigrationReportV1: FDBPolymorphicMigrationDocumentV1 {
    #Directory<FDBPolymorphicMigrationReportV1>("polymorphic_migration_fdb_reports")

    var id: String = UUID().uuidString
    var title: String
    var pageCount: Int64
}

@Persistable(type: "FDBPolymorphicMigrationReport")
struct FDBPolymorphicMigrationReportV2: FDBPolymorphicMigrationDocumentV2 {
    #Directory<FDBPolymorphicMigrationReportV2>("polymorphic_migration_fdb_reports")

    var id: String = UUID().uuidString
    var title: String
    var pageCount: Int64
}

@Persistable(type: "FDBPolymorphicMigrationReport")
struct FDBPolymorphicMigrationReportV3: FDBPolymorphicMigrationDocumentV3 {
    #Directory<FDBPolymorphicMigrationReportV3>("polymorphic_migration_fdb_reports")

    var id: String = UUID().uuidString
    var title: String
    var pageCount: Int64
}

@Persistable(type: "FDBPolymorphicMigrationReport")
struct FDBPolymorphicMigrationReportV4: FDBPolymorphicMigrationDocumentV4 {
    #Directory<FDBPolymorphicMigrationReportV4>("polymorphic_migration_fdb_reports")

    var id: String = UUID().uuidString
    var title: String
    var pageCount: Int64
}

enum FDBPolymorphicMigrationSchemaV1: VersionedSchema {
    static let versionIdentifier = Schema.Version(1, 0, 0)

    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) {
            [
                try FDBPolymorphicMigrationArticleV1.schemaEntity,
                try FDBPolymorphicMigrationReportV1.schemaEntity,
            ]
        }
    }
}

enum FDBPolymorphicMigrationSchemaV2: VersionedSchema {
    static let versionIdentifier = Schema.Version(2, 0, 0)

    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) {
            [
                try FDBPolymorphicMigrationArticleV2.schemaEntity,
                try FDBPolymorphicMigrationReportV2.schemaEntity,
            ]
        }
    }
}

enum FDBPolymorphicMigrationSchemaV3: VersionedSchema {
    static let versionIdentifier = Schema.Version(3, 0, 0)

    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) {
            [
                try FDBPolymorphicMigrationArticleV3.schemaEntity,
                try FDBPolymorphicMigrationReportV3.schemaEntity,
            ]
        }
    }
}

enum FDBPolymorphicMigrationSchemaV4: VersionedSchema {
    static let versionIdentifier = Schema.Version(4, 0, 0)

    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) {
            [
                try FDBPolymorphicMigrationArticleV4.schemaEntity,
                try FDBPolymorphicMigrationReportV4.schemaEntity,
            ]
        }
    }
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

@Suite("Polymorphic Migration FDB Tests", .foundationDBScenario, .serialized, .heartbeat)
struct PolymorphicMigrationFDBTests {
    @Test("FDB migration backfills added polymorphic indexes and keeps them maintained")
    func fdbMigrationBackfillsAddedPolymorphicIndexesAndKeepsThemMaintained() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let engine = try await Self.makeSystemPriorityEngine()
            try await Self.clearState(in: engine)

            let initialContainer = try await DBContainer.open(
                for: FDBPolymorphicMigrationSchemaV1.makeSchema(),
                configuration: .testing(storageEngine: engine),
                runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(FDBPolymorphicMigrationArticleV1.self), try DatabaseFrameworkRuntime.entity(FDBPolymorphicMigrationReportV1.self)]),
                security: .testingDisabled
            )
            let initialContext = initialContainer.testBaseContext()

            var article = FDBPolymorphicMigrationArticleV1(title: "Legacy Needle Article", body: "Body")
            article.id = "fdb-polymorphic-migration-article"
            var report = FDBPolymorphicMigrationReportV1(title: "Legacy Needle Report", pageCount: 8)
            report.id = "fdb-polymorphic-migration-report"

            try initialContext.insert(article)
            try initialContext.insert(report)
            try await initialContext.save()
            try await initialContainer.installTestBaseSchemaSnapshot(for: Schema.Version(1, 0, 0))

            let migratedContainer = try await DBContainer.open(
                for: FDBPolymorphicMigrationSchemaV2.self,
                migrationPlan: FDBPolymorphicMigrationPlan.self,
                configuration: .testing(storageEngine: engine),
                runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(FDBPolymorphicMigrationArticleV2.self), try DatabaseFrameworkRuntime.entity(FDBPolymorphicMigrationReportV2.self)]),
                security: .testingDisabled
            )
            try await migratedContainer.testBaseAdmin().migrateIfNeeded()

            let verificationContainer = try await DBContainer.open(
                for: FDBPolymorphicMigrationSchemaV2.makeSchema(),
                configuration: .testing(storageEngine: engine),
                runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(FDBPolymorphicMigrationArticleV2.self), try DatabaseFrameworkRuntime.entity(FDBPolymorphicMigrationReportV2.self)]),
                security: .testingDisabled
            )
            let verificationContext = verificationContainer.testBaseContext()
            let migratedResults = try await verificationContext
                .findPolymorphic(FDBPolymorphicMigrationArticleV2.self)
                .fullText(FDBPolymorphicMigrationArticleV2.fields.title)
                .term("needle")
                .execute()
            let migratedIDs = try Set(migratedResults.compactMap(Self.resultID))

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
            try verificationContext.upsert(updatedReport)
            try await verificationContext.save()

            let afterUpdateNeedle = try await verificationContext
                .findPolymorphic(FDBPolymorphicMigrationArticleV2.self)
                .fullText(FDBPolymorphicMigrationArticleV2.fields.title)
                .term("needle")
                .execute()
            let afterUpdateBeacon = try await verificationContext
                .findPolymorphic(FDBPolymorphicMigrationArticleV2.self)
                .fullText(FDBPolymorphicMigrationArticleV2.fields.title)
                .term("beacon")
                .execute()

            #expect(try Set(afterUpdateNeedle.compactMap(Self.resultID)) == Set([article.id]))
            #expect(try Set(afterUpdateBeacon.compactMap(Self.resultID)) == Set([report.id]))
            #expect(try await Self.countPolymorphicIndexEntries(
                container: verificationContainer,
                indexName: "FDBPolymorphicMigrationDocument_title"
            ) == 2)
        }
    }

    @Test("FDB migration removes polymorphic index data and disables index state")
    func fdbMigrationRemovesPolymorphicIndexDataAndDisablesIndexState() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let engine = try await Self.makeSystemPriorityEngine()
            try await Self.clearState(in: engine)

            let initialContainer = try await DBContainer.open(
                for: FDBPolymorphicMigrationSchemaV2.makeSchema(),
                configuration: .testing(storageEngine: engine),
                runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(FDBPolymorphicMigrationArticleV2.self), try DatabaseFrameworkRuntime.entity(FDBPolymorphicMigrationReportV2.self)]),
                security: .testingDisabled
            )
            let initialContext = initialContainer.testBaseContext()

            var article = FDBPolymorphicMigrationArticleV2(title: "Removal Needle Article", body: "Body")
            article.id = "fdb-polymorphic-removal-article"
            var report = FDBPolymorphicMigrationReportV2(title: "Removal Needle Report", pageCount: 8)
            report.id = "fdb-polymorphic-removal-report"

            try initialContext.upsert(article)
            try initialContext.upsert(report)
            try await initialContext.save()
            try await initialContainer.installTestBaseSchemaSnapshot(for: Schema.Version(2, 0, 0))

            #expect(try await Self.countPolymorphicIndexEntries(
                container: initialContainer,
                indexName: "FDBPolymorphicMigrationDocument_title"
            ) == 2)

            let migratedContainer = try await DBContainer.open(
                for: FDBPolymorphicMigrationSchemaV3.self,
                migrationPlan: FDBPolymorphicRemovalMigrationPlan.self,
                configuration: .testing(storageEngine: engine),
                runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(FDBPolymorphicMigrationArticleV3.self), try DatabaseFrameworkRuntime.entity(FDBPolymorphicMigrationReportV3.self)]),
                security: .testingDisabled
            )
            try await migratedContainer.testBaseAdmin().migrateIfNeeded()

            #expect(try await Self.countPolymorphicIndexEntries(
                container: migratedContainer,
                indexName: "FDBPolymorphicMigrationDocument_title"
            ) == 0)
            #expect(try await Self.polymorphicIndexState(
                container: migratedContainer,
                indexName: "FDBPolymorphicMigrationDocument_title"
            ) == .disabled)

            let postRemovalContext = migratedContainer.testBaseContext()
            var postRemovalArticle = FDBPolymorphicMigrationArticleV3(
                title: "Removal Needle After",
                body: "Body"
            )
            postRemovalArticle.id = "fdb-polymorphic-removal-after"
            try postRemovalContext.upsert(postRemovalArticle)
            try await postRemovalContext.save()

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
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let engine = try await Self.makeSystemPriorityEngine()
            try await Self.clearState(in: engine)

            let initialContainer = try await DBContainer.open(
                for: FDBPolymorphicMigrationSchemaV2.makeSchema(),
                configuration: .testing(storageEngine: engine),
                runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(FDBPolymorphicMigrationArticleV2.self), try DatabaseFrameworkRuntime.entity(FDBPolymorphicMigrationReportV2.self)]),
                security: .testingDisabled
            )
            let initialContext = initialContainer.testBaseContext()

            var article = FDBPolymorphicMigrationArticleV2(title: "Rebuild Needle Article", body: "Body")
            article.id = "fdb-polymorphic-rebuild-article"
            var report = FDBPolymorphicMigrationReportV2(title: "Rebuild Needle Report", pageCount: 5)
            report.id = "fdb-polymorphic-rebuild-report"

            try initialContext.upsert(article)
            try initialContext.upsert(report)
            try await initialContext.save()
            try await initialContainer.installTestBaseSchemaSnapshot(for: Schema.Version(2, 0, 0))

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

            let migratedContainer = try await DBContainer.open(
                for: FDBPolymorphicMigrationSchemaV4.self,
                migrationPlan: FDBPolymorphicRebuildMigrationPlan.self,
                configuration: .testing(storageEngine: engine),
                runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(FDBPolymorphicMigrationArticleV4.self), try DatabaseFrameworkRuntime.entity(FDBPolymorphicMigrationReportV4.self)]),
                security: .testingDisabled
            )
            try await migratedContainer.testBaseAdmin().migrateIfNeeded()

            let verificationContext = migratedContainer.testBaseContext()
            let rebuiltResults = try await verificationContext
                .findPolymorphic(FDBPolymorphicMigrationArticleV4.self)
                .fullText(FDBPolymorphicMigrationArticleV4.fields.title)
                .term("needle")
                .execute()
            let rebuiltIDs = try Set(rebuiltResults.compactMap(Self.resultIDV4))

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
        try await FoundationDBScenarioEnvironment.shared.ensureInitialized()
        let database = try FDBSystemPriorityDatabase()
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
                try await database.removeNamespace(path: path)
            } catch {
            }
        }

        try await database.withTransaction { transaction in
            for typeName in [
                FDBPolymorphicMigrationArticleV1.persistableType,
                FDBPolymorphicMigrationReportV1.persistableType,
            ] {
                try transaction.clear(key: Tuple(["_schema", typeName]).pack())
            }
        }
    }

    private static func resultID(_ result: PolymorphicQueryResult) throws -> String? {
        if let article = try result.decodedModel(as: FDBPolymorphicMigrationArticleV2.self) {
            return article.id
        }
        if let report = try result.decodedModel(as: FDBPolymorphicMigrationReportV2.self) {
            return report.id
        }
        return nil
    }

    private static func resultIDV4(_ result: PolymorphicQueryResult) throws -> String? {
        if let article = try result.decodedModel(as: FDBPolymorphicMigrationArticleV4.self) {
            return article.id
        }
        if let report = try result.decodedModel(as: FDBPolymorphicMigrationReportV4.self) {
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
        let groupSubspace = try await container.testBasePolymorphicDirectory(for: group.identifier)
        let indexSubspace = groupSubspace
            .subspace(SubspaceKey.indexes)
            .subspace(indexName)

        return try await container.engine.withTransaction { transaction -> Int in
            let (begin, end) = indexSubspace.range()
            return try await transaction.collectRange(
                begin: begin,
                end: end,
                snapshot: true
            ).count
        }
    }

    private static func clearPolymorphicIndexEntries(
        container: DBContainer,
        indexName: String
    ) async throws {
        let group = try container.polymorphicGroup(
            identifier: FDBPolymorphicMigrationArticleV2.polymorphableType
        )
        let groupSubspace = try await container.testBasePolymorphicDirectory(for: group.identifier)
        let indexSubspace = groupSubspace
            .subspace(SubspaceKey.indexes)
            .subspace(indexName)
        let range = indexSubspace.range()

        try await container.engine.withTransaction { transaction in
            try transaction.clearRange(beginKey: range.begin, endKey: range.end)
        }
    }

    private static func polymorphicIndexState(
        container: DBContainer,
        indexName: String
    ) async throws -> IndexState {
        let group = try container.polymorphicGroup(
            identifier: FDBPolymorphicMigrationArticleV2.polymorphableType
        )
        let groupSubspace = try await container.testBasePolymorphicDirectory(for: group.identifier)
        let lifecycleStore = IndexLifecycleStore(container: container, subspace: groupSubspace)
        return try await lifecycleStore.state(of: indexName)
    }
}
#endif

#endif
