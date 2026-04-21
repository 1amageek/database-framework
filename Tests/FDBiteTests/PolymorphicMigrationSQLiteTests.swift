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

protocol SQLitePolymorphicMigrationDocumentV3: Polymorphable {
    var id: String { get }
    var title: String { get }
}

protocol SQLitePolymorphicMigrationDocumentV4: Polymorphable {
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

extension SQLitePolymorphicMigrationDocumentV3 {
    public static var polymorphableType: String { "SQLitePolymorphicMigrationDocument" }

    public static var polymorphicDirectoryPathComponents: [any DirectoryPathElement] {
        [Path("sqlite_polymorphic_migration_shared")]
    }
}

extension SQLitePolymorphicMigrationDocumentV4 {
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

@Persistable(type: "SQLitePolymorphicMigrationArticle")
struct SQLitePolymorphicMigrationArticleV3: SQLitePolymorphicMigrationDocumentV3 {
    #Directory<SQLitePolymorphicMigrationArticleV3>("sqlite_polymorphic_migration_articles")

    var id: String = ULID().ulidString
    var title: String
    var body: String
}

@Persistable(type: "SQLitePolymorphicMigrationArticle")
struct SQLitePolymorphicMigrationArticleV4: SQLitePolymorphicMigrationDocumentV4 {
    #Directory<SQLitePolymorphicMigrationArticleV4>("sqlite_polymorphic_migration_articles")

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

@Persistable(type: "SQLitePolymorphicMigrationReport")
struct SQLitePolymorphicMigrationReportV3: SQLitePolymorphicMigrationDocumentV3 {
    #Directory<SQLitePolymorphicMigrationReportV3>("sqlite_polymorphic_migration_reports")

    var id: String = ULID().ulidString
    var title: String
    var pageCount: Int
}

@Persistable(type: "SQLitePolymorphicMigrationReport")
struct SQLitePolymorphicMigrationReportV4: SQLitePolymorphicMigrationDocumentV4 {
    #Directory<SQLitePolymorphicMigrationReportV4>("sqlite_polymorphic_migration_reports")

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

enum SQLitePolymorphicMigrationSchemaV3: VersionedSchema {
    static let versionIdentifier = Schema.Version(3, 0, 0)

    static let models: [any Persistable.Type] = [
        SQLitePolymorphicMigrationArticleV3.self,
        SQLitePolymorphicMigrationReportV3.self,
    ]
}

enum SQLitePolymorphicMigrationSchemaV4: VersionedSchema {
    static let versionIdentifier = Schema.Version(4, 0, 0)

    static let models: [any Persistable.Type] = [
        SQLitePolymorphicMigrationArticleV4.self,
        SQLitePolymorphicMigrationReportV4.self,
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

enum SQLitePolymorphicRemovalMigrationPlan: SchemaMigrationPlan {
    static var schemas: [any VersionedSchema.Type] {
        [
            SQLitePolymorphicMigrationSchemaV2.self,
            SQLitePolymorphicMigrationSchemaV3.self,
        ]
    }

    static var stages: [MigrationStage] {
        [
            .lightweight(
                fromVersion: SQLitePolymorphicMigrationSchemaV2.self,
                toVersion: SQLitePolymorphicMigrationSchemaV3.self
            )
        ]
    }
}

enum SQLitePolymorphicRebuildMigrationPlan: SchemaMigrationPlan {
    static var schemas: [any VersionedSchema.Type] {
        [
            SQLitePolymorphicMigrationSchemaV2.self,
            SQLitePolymorphicMigrationSchemaV4.self,
        ]
    }

    static var stages: [MigrationStage] {
        [
            .custom(
                fromVersion: SQLitePolymorphicMigrationSchemaV2.self,
                toVersion: SQLitePolymorphicMigrationSchemaV4.self,
                willMigrate: rebuildPolymorphicIndexes,
                didMigrate: nil
            )
        ]
    }

    static func rebuildPolymorphicIndexes(context: MigrationContext) async throws {
        try await context.rebuildIndex(
            indexName: "SQLitePolymorphicMigrationDocument_title",
            batchSize: 1
        )
        try await context.rebuildIndex(
            indexName: "SQLitePolymorphicMigrationDocument_title_fulltext",
            batchSize: 1
        )
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

    @Test("VersionedSchema detects removed polymorphic descriptors")
    func versionedSchemaDetectsRemovedPolymorphicDescriptors() {
        let changes = SQLitePolymorphicMigrationSchemaV3.indexChanges(
            from: SQLitePolymorphicMigrationSchemaV2.self
        )
        let expectedRemoved = Set([
            "SQLitePolymorphicMigrationDocument_title",
            "SQLitePolymorphicMigrationDocument_title_fulltext",
        ])
        let stage = MigrationStage.lightweight(
            fromVersion: SQLitePolymorphicMigrationSchemaV2.self,
            toVersion: SQLitePolymorphicMigrationSchemaV3.self
        )

        #expect(changes.added.isEmpty)
        #expect(changes.removed == expectedRemoved)
        #expect(stage.removedIndexNames == expectedRemoved)
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

    @Test("SQLite migration backfills polymorphic indexes across batch boundaries")
    func sqliteMigrationBackfillsPolymorphicIndexesAcrossBatchBoundaries() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        let initialContainer = try await DBContainer(
            for: SQLitePolymorphicMigrationSchemaV1.makeSchema(),
            configuration: .init(backend: .custom(engine)),
            security: .disabled
        )
        let initialContext = initialContainer.newContext()

        for offset in 0..<105 {
            var article = SQLitePolymorphicMigrationArticleV1(
                title: "Batch Needle Article \(offset)",
                body: "Body \(offset)"
            )
            article.id = "sqlite-polymorphic-batch-article-\(offset)"
            initialContext.insert(article)
        }
        var report = SQLitePolymorphicMigrationReportV1(
            title: "Batch Needle Report",
            pageCount: 3
        )
        report.id = "sqlite-polymorphic-batch-report"
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
        let results = try await verificationContext
            .findPolymorphic(SQLitePolymorphicMigrationArticleV2.self)
            .fullText(\.title)
            .term("needle")
            .execute()

        #expect(results.count == 106)
        #expect(try await Self.countPolymorphicIndexEntries(
            container: verificationContainer,
            indexName: "SQLitePolymorphicMigrationDocument_title"
        ) == 106)
    }

    @Test("SQLite migration removes polymorphic index data and disables index state")
    func sqliteMigrationRemovesPolymorphicIndexDataAndDisablesIndexState() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        let initialContainer = try await DBContainer(
            for: SQLitePolymorphicMigrationSchemaV2.makeSchema(),
            configuration: .init(backend: .custom(engine)),
            security: .disabled
        )
        let initialContext = initialContainer.newContext()

        var article = SQLitePolymorphicMigrationArticleV2(title: "Removal Needle Article", body: "Body")
        article.id = "sqlite-polymorphic-removal-article"
        var report = SQLitePolymorphicMigrationReportV2(title: "Removal Needle Report", pageCount: 8)
        report.id = "sqlite-polymorphic-removal-report"

        try await initialContext.savePolymorphic(article, as: SQLitePolymorphicMigrationArticleV2.self)
        try await initialContext.savePolymorphic(report, as: SQLitePolymorphicMigrationArticleV2.self)
        try await initialContainer.setCurrentSchemaVersion(Schema.Version(2, 0, 0))

        #expect(try await Self.countPolymorphicIndexEntries(
            container: initialContainer,
            indexName: "SQLitePolymorphicMigrationDocument_title"
        ) == 2)

        let migratedContainer = try await DBContainer(
            for: SQLitePolymorphicMigrationSchemaV3.self,
            migrationPlan: SQLitePolymorphicRemovalMigrationPlan.self,
            configuration: .init(backend: .custom(engine)),
            security: .disabled
        )
        try await migratedContainer.migrateIfNeeded()

        #expect(try await Self.countPolymorphicIndexEntries(
            container: migratedContainer,
            indexName: "SQLitePolymorphicMigrationDocument_title"
        ) == 0)
        #expect(try await Self.polymorphicIndexState(
            container: migratedContainer,
            indexName: "SQLitePolymorphicMigrationDocument_title"
        ) == .disabled)

        let postRemovalContext = migratedContainer.newContext()
        var postRemovalArticle = SQLitePolymorphicMigrationArticleV3(
            title: "Removal Needle After",
            body: "Body"
        )
        postRemovalArticle.id = "sqlite-polymorphic-removal-after"
        try await postRemovalContext.savePolymorphic(
            postRemovalArticle,
            as: SQLitePolymorphicMigrationArticleV3.self
        )

        let fetched = try await postRemovalContext.fetchPolymorphic(
            SQLitePolymorphicMigrationArticleV3.self
        )
        #expect(fetched.count == 3)
        #expect(try await Self.countPolymorphicIndexEntries(
            container: migratedContainer,
            indexName: "SQLitePolymorphicMigrationDocument_title"
        ) == 0)
    }

    @Test("SQLite custom migration rebuilds corrupted polymorphic indexes")
    func sqliteCustomMigrationRebuildsCorruptedPolymorphicIndexes() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        let initialContainer = try await DBContainer(
            for: SQLitePolymorphicMigrationSchemaV2.makeSchema(),
            configuration: .init(backend: .custom(engine)),
            security: .disabled
        )
        let initialContext = initialContainer.newContext()

        var article = SQLitePolymorphicMigrationArticleV2(title: "Rebuild Needle Article", body: "Body")
        article.id = "sqlite-polymorphic-rebuild-article"
        var report = SQLitePolymorphicMigrationReportV2(title: "Rebuild Needle Report", pageCount: 5)
        report.id = "sqlite-polymorphic-rebuild-report"

        try await initialContext.savePolymorphic(article, as: SQLitePolymorphicMigrationArticleV2.self)
        try await initialContext.savePolymorphic(report, as: SQLitePolymorphicMigrationArticleV2.self)
        try await initialContainer.setCurrentSchemaVersion(Schema.Version(2, 0, 0))

        try await Self.clearPolymorphicIndexEntries(
            container: initialContainer,
            indexName: "SQLitePolymorphicMigrationDocument_title"
        )
        try await Self.clearPolymorphicIndexEntries(
            container: initialContainer,
            indexName: "SQLitePolymorphicMigrationDocument_title_fulltext"
        )
        #expect(try await Self.countPolymorphicIndexEntries(
            container: initialContainer,
            indexName: "SQLitePolymorphicMigrationDocument_title"
        ) == 0)

        let migratedContainer = try await DBContainer(
            for: SQLitePolymorphicMigrationSchemaV4.self,
            migrationPlan: SQLitePolymorphicRebuildMigrationPlan.self,
            configuration: .init(backend: .custom(engine)),
            security: .disabled
        )
        try await migratedContainer.migrateIfNeeded()

        let verificationContext = migratedContainer.newContext()
        let rebuiltResults = try await verificationContext
            .findPolymorphic(SQLitePolymorphicMigrationArticleV4.self)
            .fullText(\.title)
            .term("needle")
            .execute()
        let rebuiltIDs = Set(rebuiltResults.compactMap(Self.resultIDV4))

        #expect(rebuiltIDs == Set([article.id, report.id]))
        #expect(try await Self.countPolymorphicIndexEntries(
            container: migratedContainer,
            indexName: "SQLitePolymorphicMigrationDocument_title"
        ) == 2)
        #expect(try await Self.polymorphicIndexState(
            container: migratedContainer,
            indexName: "SQLitePolymorphicMigrationDocument_title"
        ) == .readable)
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

    private static func resultIDV4(_ result: PolymorphicQueryResult) -> String? {
        if let article = result.item(as: SQLitePolymorphicMigrationArticleV4.self) {
            return article.id
        }
        if let report = result.item(as: SQLitePolymorphicMigrationReportV4.self) {
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

    private static func clearPolymorphicIndexEntries(
        container: DBContainer,
        indexName: String
    ) async throws {
        let group = try container.polymorphicGroup(
            identifier: SQLitePolymorphicMigrationArticleV2.polymorphableType
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
            identifier: SQLitePolymorphicMigrationArticleV2.polymorphableType
        )
        let groupSubspace = try await container.resolvePolymorphicDirectory(for: group.identifier)
        let stateManager = IndexStateManager(container: container, subspace: groupSubspace)
        return try await stateManager.state(of: indexName)
    }
}
#endif
