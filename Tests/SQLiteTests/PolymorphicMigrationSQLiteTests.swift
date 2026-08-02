#if SQLITE
import Testing
import TestSupport
import Foundation
import Database
import StorageKit
import TestHeartbeat
import DatabaseRuntime

@Polymorphable(identifier: "SQLitePolymorphicMigrationDocument")
@PolymorphicDirectory("sqlite_polymorphic_migration_shared")
protocol SQLitePolymorphicMigrationDocumentV1:
    Polymorphable<SQLitePolymorphicMigrationDocumentV1PolymorphicGroup>
{
    var id: String { get }
    var title: String { get }
}

@Polymorphable(identifier: "SQLitePolymorphicMigrationDocument")
@PolymorphicDirectory("sqlite_polymorphic_migration_shared")
@PolymorphicIndex(
    .scalar,
    fields: ["title"],
    name: "SQLitePolymorphicMigrationDocument_title"
)
@PolymorphicIndex(
    .fullText(tokenizer: .simple),
    fields: ["title"],
    name: "SQLitePolymorphicMigrationDocument_title_fulltext"
)
protocol SQLitePolymorphicMigrationDocumentV2:
    Polymorphable<SQLitePolymorphicMigrationDocumentV2PolymorphicGroup>
{
    var id: String { get }
    var title: String { get }
}

@Polymorphable(identifier: "SQLitePolymorphicMigrationDocument")
@PolymorphicDirectory("sqlite_polymorphic_migration_shared")
protocol SQLitePolymorphicMigrationDocumentV3:
    Polymorphable<SQLitePolymorphicMigrationDocumentV3PolymorphicGroup>
{
    var id: String { get }
    var title: String { get }
}

@Polymorphable(identifier: "SQLitePolymorphicMigrationDocument")
@PolymorphicDirectory("sqlite_polymorphic_migration_shared")
@PolymorphicIndex(
    .scalar,
    fields: ["title"],
    name: "SQLitePolymorphicMigrationDocument_title"
)
@PolymorphicIndex(
    .fullText(tokenizer: .simple),
    fields: ["title"],
    name: "SQLitePolymorphicMigrationDocument_title_fulltext"
)
protocol SQLitePolymorphicMigrationDocumentV4:
    Polymorphable<SQLitePolymorphicMigrationDocumentV4PolymorphicGroup>
{
    var id: String { get }
    var title: String { get }
}

@Persistable(type: "SQLitePolymorphicMigrationArticle")
struct SQLitePolymorphicMigrationArticleV1: SQLitePolymorphicMigrationDocumentV1 {
    #Directory<SQLitePolymorphicMigrationArticleV1>("sqlite_polymorphic_migration_articles")

    var id: String = UUID().uuidString
    var title: String
    var body: String
}

@Persistable(type: "SQLitePolymorphicMigrationArticle")
struct SQLitePolymorphicMigrationArticleV2: SQLitePolymorphicMigrationDocumentV2 {
    #Directory<SQLitePolymorphicMigrationArticleV2>("sqlite_polymorphic_migration_articles")

    var id: String = UUID().uuidString
    var title: String
    var body: String
}

@Persistable(type: "SQLitePolymorphicMigrationArticle")
struct SQLitePolymorphicMigrationArticleV3: SQLitePolymorphicMigrationDocumentV3 {
    #Directory<SQLitePolymorphicMigrationArticleV3>("sqlite_polymorphic_migration_articles")

    var id: String = UUID().uuidString
    var title: String
    var body: String
}

@Persistable(type: "SQLitePolymorphicMigrationArticle")
struct SQLitePolymorphicMigrationArticleV4: SQLitePolymorphicMigrationDocumentV4 {
    #Directory<SQLitePolymorphicMigrationArticleV4>("sqlite_polymorphic_migration_articles")

    var id: String = UUID().uuidString
    var title: String
    var body: String
}

@Persistable(type: "SQLitePolymorphicMigrationReport")
struct SQLitePolymorphicMigrationReportV1: SQLitePolymorphicMigrationDocumentV1 {
    #Directory<SQLitePolymorphicMigrationReportV1>("sqlite_polymorphic_migration_reports")

    var id: String = UUID().uuidString
    var title: String
    var pageCount: Int64
}

@Persistable(type: "SQLitePolymorphicMigrationReport")
struct SQLitePolymorphicMigrationReportV2: SQLitePolymorphicMigrationDocumentV2 {
    #Directory<SQLitePolymorphicMigrationReportV2>("sqlite_polymorphic_migration_reports")

    var id: String = UUID().uuidString
    var title: String
    var pageCount: Int64
}

@Persistable(type: "SQLitePolymorphicMigrationReport")
struct SQLitePolymorphicMigrationReportV3: SQLitePolymorphicMigrationDocumentV3 {
    #Directory<SQLitePolymorphicMigrationReportV3>("sqlite_polymorphic_migration_reports")

    var id: String = UUID().uuidString
    var title: String
    var pageCount: Int64
}

@Persistable(type: "SQLitePolymorphicMigrationReport")
struct SQLitePolymorphicMigrationReportV4: SQLitePolymorphicMigrationDocumentV4 {
    #Directory<SQLitePolymorphicMigrationReportV4>("sqlite_polymorphic_migration_reports")

    var id: String = UUID().uuidString
    var title: String
    var pageCount: Int64
}

enum SQLitePolymorphicMigrationSchemaV1: VersionedSchema {
    static let versionIdentifier = Schema.Version(1, 0, 0)

    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) {
            [
                try SQLitePolymorphicMigrationArticleV1.schemaEntity,
                try SQLitePolymorphicMigrationReportV1.schemaEntity,
            ]
        }
    }
}

enum SQLitePolymorphicMigrationSchemaV2: VersionedSchema {
    static let versionIdentifier = Schema.Version(2, 0, 0)

    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) {
            [
                try SQLitePolymorphicMigrationArticleV2.schemaEntity,
                try SQLitePolymorphicMigrationReportV2.schemaEntity,
            ]
        }
    }
}

enum SQLitePolymorphicMigrationSchemaV3: VersionedSchema {
    static let versionIdentifier = Schema.Version(3, 0, 0)

    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) {
            [
                try SQLitePolymorphicMigrationArticleV3.schemaEntity,
                try SQLitePolymorphicMigrationReportV3.schemaEntity,
            ]
        }
    }
}

enum SQLitePolymorphicMigrationSchemaV4: VersionedSchema {
    static let versionIdentifier = Schema.Version(4, 0, 0)

    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) {
            [
                try SQLitePolymorphicMigrationArticleV4.schemaEntity,
                try SQLitePolymorphicMigrationReportV4.schemaEntity,
            ]
        }
    }
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
    @Test("VersionedSchema exposes polymorphic logical indexes for migration diffing")
    func versionedSchemaExposesPolymorphicLogicalIndexesForMigrationDiffing() throws {
        let changes = try SQLitePolymorphicMigrationSchemaV2.indexChanges(
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
        let schema = try SQLitePolymorphicMigrationSchemaV2.makeSchema()
        let stageIndexChanges = try stage.indexChanges
        let addedIndexDescriptors = try stage.addedIndexDescriptors
        let logicalDescriptors = schema.polymorphicIndexCatalog(
            identifier: SQLitePolymorphicMigrationArticleV2.polymorphableType
        )
        let articleDescriptors = schema.polymorphicIndexDescriptors(
            identifier: SQLitePolymorphicMigrationArticleV2.polymorphableType,
            memberType: SQLitePolymorphicMigrationArticleV2.self
        )
        let reportDescriptors = schema.polymorphicIndexDescriptors(
            identifier: SQLitePolymorphicMigrationArticleV2.polymorphableType,
            memberType: SQLitePolymorphicMigrationReportV2.self
        )

        #expect(changes.added == expectedAdded)
        #expect(stageIndexChanges.added == expectedAdded)
        #expect(addedIndexDescriptors.isEmpty)
        #expect(Set(logicalDescriptors.map(\.name)) == expectedAdded)
        #expect(schema.indexDescriptor(named: "SQLitePolymorphicMigrationDocument_title") == nil)
        #expect(schema.polymorphicGroup(containingIndexNamed: "SQLitePolymorphicMigrationDocument_title") != nil)
        #expect(articleDescriptors.map(\.name) == reportDescriptors.map(\.name))
        #expect(articleDescriptors.first?.fieldNames == ["title"])
        #expect(reportDescriptors.first?.fieldNames == ["title"])
        #expect(articleDescriptors.first?.kindIdentifier == reportDescriptors.first?.kindIdentifier)
    }

    @Test("VersionedSchema detects removed polymorphic descriptors")
    func versionedSchemaDetectsRemovedPolymorphicDescriptors() throws {
        let changes = try SQLitePolymorphicMigrationSchemaV3.indexChanges(
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
        let removedIndexNames = try stage.removedIndexNames

        #expect(changes.added.isEmpty)
        #expect(changes.removed == expectedRemoved)
        #expect(removedIndexNames == expectedRemoved)
    }

    @Test("SQLite migration backfills added polymorphic indexes and keeps them maintained")
    func sqliteMigrationBackfillsAddedPolymorphicIndexesAndKeepsThemMaintained() async throws {
        let database = try SQLiteTestDatabase(prefix: "polymorphic-migration-backfill")
        defer { database.remove() }
        let initialContainer = try await DBContainer.open(
            for: SQLitePolymorphicMigrationSchemaV1.makeSchema(),
            configuration: .file(database.path),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLitePolymorphicMigrationArticleV1.self), try DatabaseFrameworkRuntime.entity(SQLitePolymorphicMigrationReportV1.self)]),
            security: .disabled
        )
        defer { await initialContainer.shutdown() }
        let initialContext = initialContainer.newContext()

        var article = SQLitePolymorphicMigrationArticleV1(title: "Legacy Needle Article", body: "Body")
        article.id = "sqlite-polymorphic-migration-article"
        var report = SQLitePolymorphicMigrationReportV1(title: "Legacy Needle Report", pageCount: 8)
        report.id = "sqlite-polymorphic-migration-report"

        try initialContext.insert(article)
        try initialContext.insert(report)
        try await initialContext.save()
        try await initialContainer.installSchemaSnapshot(for: Schema.Version(1, 0, 0))
        await initialContainer.shutdown()

        let migratedContainer = try await DBContainer.open(
            for: SQLitePolymorphicMigrationSchemaV2.self,
            migrationPlan: SQLitePolymorphicMigrationPlan.self,
            configuration: .file(database.path),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLitePolymorphicMigrationArticleV2.self), try DatabaseFrameworkRuntime.entity(SQLitePolymorphicMigrationReportV2.self)]),
            security: .disabled
        )
        defer { await migratedContainer.shutdown() }
        try await migratedContainer.migrateIfNeeded()
        await migratedContainer.shutdown()

        let verificationContainer = try await DBContainer.open(
            for: SQLitePolymorphicMigrationSchemaV2.makeSchema(),
            configuration: .file(database.path),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLitePolymorphicMigrationArticleV2.self), try DatabaseFrameworkRuntime.entity(SQLitePolymorphicMigrationReportV2.self)]),
            security: .disabled
        )
        defer { await verificationContainer.shutdown() }
        let verificationContext = verificationContainer.newContext()
        let migratedResults = try await verificationContext
            .findPolymorphic(SQLitePolymorphicMigrationArticleV2.self)
            .fullText(SQLitePolymorphicMigrationArticleV2.fields.title)
            .term("needle")
            .execute()
        let migratedIDs = try Set(migratedResults.compactMap(Self.resultID))

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
        try verificationContext.upsert(updatedReport)
        try await verificationContext.save()

        let afterUpdateNeedle = try await verificationContext
            .findPolymorphic(SQLitePolymorphicMigrationArticleV2.self)
            .fullText(SQLitePolymorphicMigrationArticleV2.fields.title)
            .term("needle")
            .execute()
        let afterUpdateBeacon = try await verificationContext
            .findPolymorphic(SQLitePolymorphicMigrationArticleV2.self)
            .fullText(SQLitePolymorphicMigrationArticleV2.fields.title)
            .term("beacon")
            .execute()

        #expect(try Set(afterUpdateNeedle.compactMap(Self.resultID)) == Set([article.id]))
        #expect(try Set(afterUpdateBeacon.compactMap(Self.resultID)) == Set([report.id]))
        #expect(try await Self.countPolymorphicIndexEntries(
            container: verificationContainer,
            indexName: "SQLitePolymorphicMigrationDocument_title"
        ) == 2)
    }

    @Test("SQLite migration backfills polymorphic indexes across batch boundaries")
    func sqliteMigrationBackfillsPolymorphicIndexesAcrossBatchBoundaries() async throws {
        let database = try SQLiteTestDatabase(prefix: "polymorphic-migration-batches")
        defer { database.remove() }
        let initialContainer = try await DBContainer.open(
            for: SQLitePolymorphicMigrationSchemaV1.makeSchema(),
            configuration: .file(database.path),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLitePolymorphicMigrationArticleV1.self), try DatabaseFrameworkRuntime.entity(SQLitePolymorphicMigrationReportV1.self)]),
            security: .disabled
        )
        defer { await initialContainer.shutdown() }
        let initialContext = initialContainer.newContext()

        for offset in 0..<105 {
            var article = SQLitePolymorphicMigrationArticleV1(
                title: "Batch Needle Article \(offset)",
                body: "Body \(offset)"
            )
            article.id = "sqlite-polymorphic-batch-article-\(offset)"
            try initialContext.insert(article)
        }
        var report = SQLitePolymorphicMigrationReportV1(
            title: "Batch Needle Report",
            pageCount: 3
        )
        report.id = "sqlite-polymorphic-batch-report"
        try initialContext.insert(report)

        try await initialContext.save()
        try await initialContainer.installSchemaSnapshot(for: Schema.Version(1, 0, 0))
        await initialContainer.shutdown()

        let migratedContainer = try await DBContainer.open(
            for: SQLitePolymorphicMigrationSchemaV2.self,
            migrationPlan: SQLitePolymorphicMigrationPlan.self,
            configuration: .file(database.path),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLitePolymorphicMigrationArticleV2.self), try DatabaseFrameworkRuntime.entity(SQLitePolymorphicMigrationReportV2.self)]),
            security: .disabled
        )
        defer { await migratedContainer.shutdown() }
        try await migratedContainer.migrateIfNeeded()
        await migratedContainer.shutdown()

        let verificationContainer = try await DBContainer.open(
            for: SQLitePolymorphicMigrationSchemaV2.makeSchema(),
            configuration: .file(database.path),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLitePolymorphicMigrationArticleV2.self), try DatabaseFrameworkRuntime.entity(SQLitePolymorphicMigrationReportV2.self)]),
            security: .disabled
        )
        defer { await verificationContainer.shutdown() }
        let verificationContext = verificationContainer.newContext()
        let results = try await verificationContext
            .findPolymorphic(SQLitePolymorphicMigrationArticleV2.self)
            .fullText(SQLitePolymorphicMigrationArticleV2.fields.title)
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
        let database = try SQLiteTestDatabase(prefix: "polymorphic-migration-removal")
        defer { database.remove() }
        let initialContainer = try await DBContainer.open(
            for: SQLitePolymorphicMigrationSchemaV2.makeSchema(),
            configuration: .file(database.path),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLitePolymorphicMigrationArticleV2.self), try DatabaseFrameworkRuntime.entity(SQLitePolymorphicMigrationReportV2.self)]),
            security: .disabled
        )
        defer { await initialContainer.shutdown() }
        let initialContext = initialContainer.newContext()

        var article = SQLitePolymorphicMigrationArticleV2(title: "Removal Needle Article", body: "Body")
        article.id = "sqlite-polymorphic-removal-article"
        var report = SQLitePolymorphicMigrationReportV2(title: "Removal Needle Report", pageCount: 8)
        report.id = "sqlite-polymorphic-removal-report"

        try initialContext.upsert(article)
        try initialContext.upsert(report)
        try await initialContext.save()
        try await initialContainer.installSchemaSnapshot(for: Schema.Version(2, 0, 0))

        #expect(try await Self.countPolymorphicIndexEntries(
            container: initialContainer,
            indexName: "SQLitePolymorphicMigrationDocument_title"
        ) == 2)
        await initialContainer.shutdown()

        let migratedContainer = try await DBContainer.open(
            for: SQLitePolymorphicMigrationSchemaV3.self,
            migrationPlan: SQLitePolymorphicRemovalMigrationPlan.self,
            configuration: .file(database.path),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLitePolymorphicMigrationArticleV3.self), try DatabaseFrameworkRuntime.entity(SQLitePolymorphicMigrationReportV3.self)]),
            security: .disabled
        )
        defer { await migratedContainer.shutdown() }
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
        try postRemovalContext.upsert(postRemovalArticle)
        try await postRemovalContext.save()

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
        let database = try SQLiteTestDatabase(prefix: "polymorphic-migration-rebuild")
        defer { database.remove() }
        let initialContainer = try await DBContainer.open(
            for: SQLitePolymorphicMigrationSchemaV2.makeSchema(),
            configuration: .file(database.path),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLitePolymorphicMigrationArticleV2.self), try DatabaseFrameworkRuntime.entity(SQLitePolymorphicMigrationReportV2.self)]),
            security: .disabled
        )
        defer { await initialContainer.shutdown() }
        let initialContext = initialContainer.newContext()

        var article = SQLitePolymorphicMigrationArticleV2(title: "Rebuild Needle Article", body: "Body")
        article.id = "sqlite-polymorphic-rebuild-article"
        var report = SQLitePolymorphicMigrationReportV2(title: "Rebuild Needle Report", pageCount: 5)
        report.id = "sqlite-polymorphic-rebuild-report"

        try initialContext.upsert(article)
        try initialContext.upsert(report)
        try await initialContext.save()
        try await initialContainer.installSchemaSnapshot(for: Schema.Version(2, 0, 0))

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
        await initialContainer.shutdown()

        let migratedContainer = try await DBContainer.open(
            for: SQLitePolymorphicMigrationSchemaV4.self,
            migrationPlan: SQLitePolymorphicRebuildMigrationPlan.self,
            configuration: .file(database.path),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLitePolymorphicMigrationArticleV4.self), try DatabaseFrameworkRuntime.entity(SQLitePolymorphicMigrationReportV4.self)]),
            security: .disabled
        )
        defer { await migratedContainer.shutdown() }
        try await migratedContainer.migrateIfNeeded()

        let verificationContext = migratedContainer.newContext()
        let rebuiltResults = try await verificationContext
            .findPolymorphic(SQLitePolymorphicMigrationArticleV4.self)
            .fullText(SQLitePolymorphicMigrationArticleV4.fields.title)
            .term("needle")
            .execute()
        let rebuiltIDs = try Set(rebuiltResults.compactMap(Self.resultIDV4))

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

    private static func resultID(_ result: PolymorphicQueryResult) throws -> String? {
        if let article = try result.decodedModel(as: SQLitePolymorphicMigrationArticleV2.self) {
            return article.id
        }
        if let report = try result.decodedModel(as: SQLitePolymorphicMigrationReportV2.self) {
            return report.id
        }
        return nil
    }

    private static func resultIDV4(_ result: PolymorphicQueryResult) throws -> String? {
        if let article = try result.decodedModel(as: SQLitePolymorphicMigrationArticleV4.self) {
            return article.id
        }
        if let report = try result.decodedModel(as: SQLitePolymorphicMigrationReportV4.self) {
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
            identifier: SQLitePolymorphicMigrationArticleV2.polymorphableType
        )
        let groupSubspace = try await container.resolvePolymorphicDirectory(for: group.identifier)
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
            identifier: SQLitePolymorphicMigrationArticleV2.polymorphableType
        )
        let groupSubspace = try await container.resolvePolymorphicDirectory(for: group.identifier)
        let lifecycleStore = IndexLifecycleStore(container: container, subspace: groupSubspace)
        return try await lifecycleStore.state(of: indexName)
    }
}
#endif
