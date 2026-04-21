#if SQLITE
import Testing
import Foundation
import Database
import StorageKit
import TestHeartbeat

protocol SQLitePolymorphicMigrationDocumentV1: Polymorphable {
    var id: String { get }
    var title: String { get }
}

extension SQLitePolymorphicMigrationDocumentV1 {
    public static var polymorphableType: String { "SQLitePolymorphicMigrationDocument" }

    public static var polymorphicDirectoryPathComponents: [any DirectoryPathElement] {
        [Path("sqlite_polymorphic_migration_shared")]
    }
}

protocol SQLitePolymorphicMigrationDocumentV2: Polymorphable {
    var id: String { get }
    var title: String { get }
}

extension SQLitePolymorphicMigrationDocumentV2 {
    public static var polymorphableType: String { "SQLitePolymorphicMigrationDocument" }

    public static var polymorphicDirectoryPathComponents: [any DirectoryPathElement] {
        [Path("sqlite_polymorphic_migration_shared")]
    }

    public static var polymorphicIndexDescriptors: [IndexDescriptor] {
        [
            IndexDescriptor(
                name: "SQLitePolymorphicMigrationDocument_title",
                keyPaths: [\Self.title],
                kind: ScalarIndexKind<Self>(fields: [\Self.title])
            ),
            IndexDescriptor(
                name: "SQLitePolymorphicMigrationDocument_title_fulltext",
                keyPaths: [\Self.title],
                kind: FullTextIndexKind<Self>(fields: [\Self.title], tokenizer: .simple)
            ),
        ]
    }
}

@Persistable(type: "SQLitePolymorphicMigrationArticle")
struct SQLitePolymorphicMigrationArticleV1: SQLitePolymorphicMigrationDocumentV1 {
    #Directory<SQLitePolymorphicMigrationArticleV1>("sqlite_polymorphic_migration_articles")

    var id: String = ULID().ulidString
    var title: String
    var body: String
}

@Persistable(type: "SQLitePolymorphicMigrationArticle")
struct SQLitePolymorphicMigrationArticleV2: SQLitePolymorphicMigrationDocumentV2 {
    #Directory<SQLitePolymorphicMigrationArticleV2>("sqlite_polymorphic_migration_articles")

    var id: String = ULID().ulidString
    var title: String
    var body: String
}

@Persistable(type: "SQLitePolymorphicMigrationReport")
struct SQLitePolymorphicMigrationReportV1: SQLitePolymorphicMigrationDocumentV1 {
    #Directory<SQLitePolymorphicMigrationReportV1>("sqlite_polymorphic_migration_reports")

    var id: String = ULID().ulidString
    var title: String
    var pageCount: Int
}

@Persistable(type: "SQLitePolymorphicMigrationReport")
struct SQLitePolymorphicMigrationReportV2: SQLitePolymorphicMigrationDocumentV2 {
    #Directory<SQLitePolymorphicMigrationReportV2>("sqlite_polymorphic_migration_reports")

    var id: String = ULID().ulidString
    var title: String
    var pageCount: Int
}

enum SQLitePolymorphicMigrationSchemaV1: VersionedSchema {
    static let versionIdentifier = Schema.Version(1, 0, 0)

    static let models: [any Persistable.Type] = [
        SQLitePolymorphicMigrationArticleV1.self,
        SQLitePolymorphicMigrationReportV1.self,
    ]
}

enum SQLitePolymorphicMigrationSchemaV2: VersionedSchema {
    static let versionIdentifier = Schema.Version(2, 0, 0)

    static let models: [any Persistable.Type] = [
        SQLitePolymorphicMigrationArticleV2.self,
        SQLitePolymorphicMigrationReportV2.self,
    ]
}

enum SQLitePolymorphicMigrationPlan: SchemaMigrationPlan {
    static var schemas: [any VersionedSchema.Type] {
        [
            SQLitePolymorphicMigrationSchemaV1.self,
            SQLitePolymorphicMigrationSchemaV2.self,
        ]
    }

    static var stages: [MigrationStage] {
        [
            .lightweight(
                fromVersion: SQLitePolymorphicMigrationSchemaV1.self,
                toVersion: SQLitePolymorphicMigrationSchemaV2.self
            )
        ]
    }
}

@Suite("Polymorphic Migration SQLite Tests", .serialized, .heartbeat)
struct PolymorphicMigrationSQLiteTests {
    @Test("VersionedSchema exposes polymorphic descriptors for migration diffing")
    func versionedSchemaExposesPolymorphicDescriptorsForMigrationDiffing() throws {
        let changes = SQLitePolymorphicMigrationSchemaV2.indexChanges(
            from: SQLitePolymorphicMigrationSchemaV1.self
        )
        let expectedAdded = Set([
            "SQLitePolymorphicMigrationDocument_title",
            "SQLitePolymorphicMigrationDocument_title_fulltext",
        ])
        let stage = MigrationStage.lightweight(
            fromVersion: SQLitePolymorphicMigrationSchemaV1.self,
            toVersion: SQLitePolymorphicMigrationSchemaV2.self
        )
        let schema = SQLitePolymorphicMigrationSchemaV2.makeSchema()
        let articleDescriptors = schema.polymorphicIndexDescriptors(
            identifier: SQLitePolymorphicMigrationArticleV2.polymorphableType,
            memberType: SQLitePolymorphicMigrationArticleV2.self
        )
        let reportDescriptors = schema.polymorphicIndexDescriptors(
            identifier: SQLitePolymorphicMigrationArticleV2.polymorphableType,
            memberType: SQLitePolymorphicMigrationReportV2.self
        )

        #expect(changes.added == expectedAdded)
        #expect(Set(stage.addedIndexDescriptors.map(\.name)) == expectedAdded)
        #expect(articleDescriptors.map(\.name) == reportDescriptors.map(\.name))
        #expect(articleDescriptors.first?.keyPaths.first as? PartialKeyPath<SQLitePolymorphicMigrationArticleV2> != nil)
        #expect(articleDescriptors.first?.keyPaths.first as? PartialKeyPath<SQLitePolymorphicMigrationReportV2> == nil)
        #expect(reportDescriptors.first?.keyPaths.first as? PartialKeyPath<SQLitePolymorphicMigrationReportV2> != nil)
        #expect(reportDescriptors.first?.keyPaths.first as? PartialKeyPath<SQLitePolymorphicMigrationArticleV2> == nil)
    }

    @Test("SQLite migration backfills added polymorphic indexes and keeps them maintained")
    func sqliteMigrationBackfillsAddedPolymorphicIndexesAndKeepsThemMaintained() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        let initialContainer = try await DBContainer(
            for: SQLitePolymorphicMigrationSchemaV1.makeSchema(),
            configuration: .init(backend: .custom(engine)),
            security: .disabled
        )
        let initialContext = initialContainer.newContext()

        var article = SQLitePolymorphicMigrationArticleV1(title: "Legacy Needle Article", body: "Body")
        article.id = "sqlite-polymorphic-migration-article"
        var report = SQLitePolymorphicMigrationReportV1(title: "Legacy Needle Report", pageCount: 8)
        report.id = "sqlite-polymorphic-migration-report"

        initialContext.insert(article)
        initialContext.insert(report)
        try await initialContext.save()
        try await initialContainer.setCurrentSchemaVersion(Schema.Version(1, 0, 0))

        let migratedContainer = try await DBContainer(
            for: SQLitePolymorphicMigrationSchemaV2.self,
            migrationPlan: SQLitePolymorphicMigrationPlan.self,
            configuration: .init(backend: .custom(engine)),
            security: .disabled
        )
        try await migratedContainer.migrateIfNeeded()

        let verificationContainer = try await DBContainer(
            for: SQLitePolymorphicMigrationSchemaV2.makeSchema(),
            configuration: .init(backend: .custom(engine)),
            security: .disabled
        )
        let verificationContext = verificationContainer.newContext()
        let migratedResults = try await verificationContext
            .findPolymorphic(SQLitePolymorphicMigrationArticleV2.self)
            .fullText(\.title)
            .term("needle")
            .execute()
        let migratedIDs = Set(migratedResults.compactMap(Self.resultID))

        #expect(migratedIDs == Set([article.id, report.id]))
        #expect(try await Self.countPolymorphicIndexEntries(
            container: verificationContainer,
            indexName: "SQLitePolymorphicMigrationDocument_title"
        ) == 2)

        var updatedReport = SQLitePolymorphicMigrationReportV2(
            title: "Migrated Beacon Report",
            pageCount: report.pageCount
        )
        updatedReport.id = report.id
        try await verificationContext.savePolymorphic(
            updatedReport,
            as: SQLitePolymorphicMigrationArticleV2.self
        )

        let afterUpdateNeedle = try await verificationContext
            .findPolymorphic(SQLitePolymorphicMigrationArticleV2.self)
            .fullText(\.title)
            .term("needle")
            .execute()
        let afterUpdateBeacon = try await verificationContext
            .findPolymorphic(SQLitePolymorphicMigrationArticleV2.self)
            .fullText(\.title)
            .term("beacon")
            .execute()

        #expect(Set(afterUpdateNeedle.compactMap(Self.resultID)) == Set([article.id]))
        #expect(Set(afterUpdateBeacon.compactMap(Self.resultID)) == Set([report.id]))
        #expect(try await Self.countPolymorphicIndexEntries(
            container: verificationContainer,
            indexName: "SQLitePolymorphicMigrationDocument_title"
        ) == 2)
    }

    private static func resultID(_ result: PolymorphicQueryResult) -> String? {
        if let article = result.item(as: SQLitePolymorphicMigrationArticleV2.self) {
            return article.id
        }
        if let report = result.item(as: SQLitePolymorphicMigrationReportV2.self) {
            return report.id
        }
        return nil
    }

    private static func countPolymorphicIndexEntries(
        container: DBContainer,
        indexName: String
    ) async throws -> Int {
        let group = try container.polymorphicGroup(
            identifier: SQLitePolymorphicMigrationArticleV2.polymorphableType
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
}
#endif
