#if SQLITE
import Testing
import TestSupport
import Foundation
import Database
import DatabaseTypes
import StorageKit
import TestHeartbeat
import DatabaseRuntime

// MARK: - Test Types

@Polymorphable(identifier: "SQLitePolymorphicDocument")
@PolymorphicDirectory("sqlite_polymorphic_fetch_shared")
@PolymorphicIndex(
    .scalar,
    fields: ["title"],
    name: "SQLitePolymorphicDocument_title"
)
@PolymorphicIndex(
    .scalar,
    fields: ["id"],
    name: "SQLitePolymorphicDocument_id"
)
@PolymorphicIndex(
    .fullText(tokenizer: .simple),
    fields: ["title"],
    name: "SQLitePolymorphicDocument_title_fulltext"
)
protocol SQLitePolymorphicDocument:
    Polymorphable<SQLitePolymorphicDocumentPolymorphicGroup>
{
    var id: String { get }
    var title: String { get }
}

@Persistable
struct SQLitePolymorphicArticle: SQLitePolymorphicDocument {
    #Directory<SQLitePolymorphicArticle>("sqlite_polymorphic_fetch_articles")

    var id: String = UUID().uuidString
    var title: String
    var body: String
}

@Persistable
struct SQLitePolymorphicReport: SQLitePolymorphicDocument {
    #Directory<SQLitePolymorphicReport>("sqlite_polymorphic_fetch_reports")

    var id: String = UUID().uuidString
    var title: String
    var pageCount: Int64
}

@Polymorphable(identifier: "SQLiteSecurePolymorphicDocument")
@PolymorphicDirectory("sqlite_secure_polymorphic_shared")
@PolymorphicIndex(
    .scalar,
    fields: ["title"],
    name: "SQLiteSecurePolymorphicDocument_title"
)
protocol SQLiteSecurePolymorphicDocument:
    Polymorphable<SQLiteSecurePolymorphicDocumentPolymorphicGroup>
{
    var id: String { get }
    var title: String { get }
    var ownerID: String { get }
}

@Persistable
struct SQLiteSecurePolymorphicArticle: SQLiteSecurePolymorphicDocument, SecurityPolicy {
    #Directory<SQLiteSecurePolymorphicArticle>("sqlite_secure_polymorphic_articles")

    var id: String = UUID().uuidString
    var title: String
    var ownerID: String
    var body: String

    static func permitsRead(
        of resource: borrowing SQLiteSecurePolymorphicArticle,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        resource.ownerID == context.principal?.identifier
    }

    static func permitsQuery(
        _ query: borrowing SecurityQuery,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        context.isAuthenticated
    }

    static func permitsCreate(
        _ newResource: borrowing SQLiteSecurePolymorphicArticle,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        newResource.ownerID == context.principal?.identifier
    }

    static func permitsUpdate(
        from resource: borrowing SQLiteSecurePolymorphicArticle,
        to newResource: borrowing SQLiteSecurePolymorphicArticle,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        resource.ownerID == context.principal?.identifier
    }

    static func permitsDelete(
        _ resource: borrowing SQLiteSecurePolymorphicArticle,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        resource.ownerID == context.principal?.identifier
    }
}

@Polymorphable(identifier: "SQLitePolymorphicVectorDocument")
@PolymorphicDirectory("sqlite_polymorphic_vector_shared")
@PolymorphicIndex(
    .vector(dimensions: 3, metric: .cosine),
    embedding: "embedding",
    name: "SQLitePolymorphicVectorDocument_embedding"
)
protocol SQLitePolymorphicVectorDocument:
    Polymorphable<SQLitePolymorphicVectorDocumentPolymorphicGroup>
{
    var id: String { get }
    var title: String { get }
    var embedding: Vector { get }
}

@Persistable
struct SQLitePolymorphicVectorArticle: SQLitePolymorphicVectorDocument {
    #Directory<SQLitePolymorphicVectorArticle>("sqlite_polymorphic_vector_articles")

    var id: String = UUID().uuidString
    var title: String
    var embedding: Vector
    var body: String
}

@Persistable
struct SQLitePolymorphicVectorReport: SQLitePolymorphicVectorDocument {
    #Directory<SQLitePolymorphicVectorReport>("sqlite_polymorphic_vector_reports")

    var id: String = UUID().uuidString
    var title: String
    var embedding: Vector
    var pageCount: Int64
}

@Polymorphable(identifier: "SQLitePolymorphicVectorNoIndexDocument")
@PolymorphicDirectory("sqlite_polymorphic_vector_no_index_shared")
protocol SQLitePolymorphicVectorNoIndexDocument:
    Polymorphable<SQLitePolymorphicVectorNoIndexDocumentPolymorphicGroup>
{
    var id: String { get }
    var title: String { get }
    var embedding: Vector { get }
}

@Persistable
struct SQLitePolymorphicVectorNoIndexArticle: SQLitePolymorphicVectorNoIndexDocument {
    #Directory<SQLitePolymorphicVectorNoIndexArticle>("sqlite_polymorphic_vector_no_index_articles")

    var id: String = UUID().uuidString
    var title: String
    var embedding: Vector
    var body: String
}

@Polymorphable(identifier: "SQLitePolymorphicOptionalVectorDocument")
@PolymorphicDirectory("sqlite_polymorphic_optional_vector_shared")
@PolymorphicIndex(
    .vector(dimensions: 3, metric: .cosine),
    embedding: "embedding",
    name: "SQLitePolymorphicOptionalVectorDocument_embedding"
)
protocol SQLitePolymorphicOptionalVectorDocument:
    Polymorphable<SQLitePolymorphicOptionalVectorDocumentPolymorphicGroup>
{
    var id: String { get }
    var title: String { get }
    var embedding: Vector? { get }
}

@Persistable
struct SQLitePolymorphicOptionalVectorArticle: SQLitePolymorphicOptionalVectorDocument {
    #Directory<SQLitePolymorphicOptionalVectorArticle>("sqlite_polymorphic_optional_vector_articles")

    var id: String = UUID().uuidString
    var title: String
    var embedding: Vector?
    var body: String
}

@Persistable
struct SQLitePolymorphicOptionalVectorReport: SQLitePolymorphicOptionalVectorDocument {
    #Directory<SQLitePolymorphicOptionalVectorReport>("sqlite_polymorphic_optional_vector_reports")

    var id: String = UUID().uuidString
    var title: String
    var embedding: Vector?
    var pageCount: Int64
}

@Suite("Polymorphic Fetch SQLite Tests", .serialized, .heartbeat)
struct PolymorphicFetchSQLiteTests {

    private func setupContainer() async throws -> DBContainer {
        let schema = try Schema(
            entities: [
                try SQLitePolymorphicArticle.schemaEntity,
                try SQLitePolymorphicReport.schemaEntity,
            ],
            version: Schema.Version(1, 0, 0)
        )

        return try await DBContainer.inMemory(
            for: schema,
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
            entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLitePolymorphicArticle.self), try DatabaseFrameworkRuntime.entity(SQLitePolymorphicReport.self)]
            ),
            security: .disabled
        )
    }

    private func setupVectorContainer() async throws -> DBContainer {
        let schema = try Schema(
            entities: [
                try SQLitePolymorphicVectorArticle.schemaEntity,
                try SQLitePolymorphicVectorReport.schemaEntity,
            ],
            version: Schema.Version(1, 0, 0)
        )
        let engine = try SQLiteStorageEngine(configuration: .inMemory)

        return try await DBContainer.open(
            for: schema,
            configuration: .testing(storageEngine: engine),
            runtimeConfiguration: try vectorRuntimeConfiguration(
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLitePolymorphicVectorArticle.self), try DatabaseFrameworkRuntime.entity(SQLitePolymorphicVectorReport.self)]
            ),
            security: .disabled
        )
    }

    private func setupOptionalVectorContainer() async throws -> DBContainer {
        let schema = try Schema(
            entities: [
                try SQLitePolymorphicOptionalVectorArticle.schemaEntity,
                try SQLitePolymorphicOptionalVectorReport.schemaEntity,
            ],
            version: Schema.Version(1, 0, 0)
        )
        let engine = try SQLiteStorageEngine(configuration: .inMemory)

        return try await DBContainer.open(
            for: schema,
            configuration: .testing(storageEngine: engine),
            runtimeConfiguration: try vectorRuntimeConfiguration(
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLitePolymorphicOptionalVectorArticle.self), try DatabaseFrameworkRuntime.entity(SQLitePolymorphicOptionalVectorReport.self)]
            ),
            security: .disabled
        )
    }

    private func vectorRuntimeConfiguration(
        entityRuntimes: [EntityRuntimeRegistration]
    ) throws -> DatabaseRuntimeConfiguration {
        try DatabaseRuntimeConfiguration(
            indexMaintainerProviderDescriptors: [
                .init(describing: VectorIndexMaintainerProvider())
            ],
            polymorphicIndexReadExecutors: [VectorReadExecutors.polymorphicIndexExecutor()],
            entityRuntimes: entityRuntimes
        )
    }

    private func setupNoIndexVectorContainer() async throws -> DBContainer {
        let schema = try Schema(
            entities: [try SQLitePolymorphicVectorNoIndexArticle.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )

        return try await DBContainer.inMemory(
            for: schema,
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
            entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLitePolymorphicVectorNoIndexArticle.self)]
            ),
            security: .disabled
        )
    }

    private func countPolymorphicIndexEntries(
        container: DBContainer,
        indexName: String,
        valuePrefix: String? = nil
    ) async throws -> Int {
        let group = try container.polymorphicGroup(identifier: SQLitePolymorphicArticle.polymorphableType)
        let groupSubspace = try await container.resolvePolymorphicDirectory(for: group.identifier)
        let baseIndexSubspace = groupSubspace
            .subspace(SubspaceKey.indexes)
            .subspace(indexName)
        let indexSubspace: Subspace
        if let valuePrefix {
            indexSubspace = Subspace(
                prefix: baseIndexSubspace.prefix.appending(contentsOf: Tuple(
                    try FieldValue.string(valuePrefix).toTupleElement()
                ).pack())
            )
        } else {
            indexSubspace = baseIndexSubspace
        }

        return try await container.engine.withTransaction { transaction -> Int in
            let (begin, end) = indexSubspace.range()
            return try await transaction.collectRange(
                begin: begin,
                end: end,
                snapshot: true
            ).count
        }
    }

    private func countPolymorphicVectorIndexEntries(container: DBContainer) async throws -> Int {
        let group = try container.polymorphicGroup(identifier: SQLitePolymorphicVectorArticle.polymorphableType)
        let groupSubspace = try await container.resolvePolymorphicDirectory(for: group.identifier)
        let indexSubspace = groupSubspace
            .subspace(SubspaceKey.indexes)
            .subspace("SQLitePolymorphicVectorDocument_embedding")

        return try await container.engine.withTransaction { transaction -> Int in
            let (begin, end) = indexSubspace.range()
            return try await transaction.collectRange(
                begin: begin,
                end: end,
                snapshot: true
            ).count
        }
    }

    private func countSecurePolymorphicIndexEntries(
        container: DBContainer,
        valuePrefix: String? = nil
    ) async throws -> Int {
        let group = try container.polymorphicGroup(
            identifier: SQLiteSecurePolymorphicArticle.polymorphableType
        )
        let groupSubspace = try await container.resolvePolymorphicDirectory(for: group.identifier)
        let baseIndexSubspace = groupSubspace
            .subspace(SubspaceKey.indexes)
            .subspace("SQLiteSecurePolymorphicDocument_title")
        let indexSubspace: Subspace
        if let valuePrefix {
            indexSubspace = Subspace(
                prefix: baseIndexSubspace.prefix.appending(contentsOf: Tuple(
                    try FieldValue.string(valuePrefix).toTupleElement()
                ).pack())
            )
        } else {
            indexSubspace = baseIndexSubspace
        }

        return try await container.engine.withTransaction { transaction -> Int in
            let (begin, end) = indexSubspace.range()
            return try await transaction.collectRange(
                begin: begin,
                end: end,
                snapshot: true
            ).count
        }
    }

    private func countPolymorphicOptionalVectorIndexEntries(container: DBContainer) async throws -> Int {
        let group = try container.polymorphicGroup(
            identifier: SQLitePolymorphicOptionalVectorArticle.polymorphableType
        )
        let groupSubspace = try await container.resolvePolymorphicDirectory(for: group.identifier)
        let indexSubspace = groupSubspace
            .subspace(SubspaceKey.indexes)
            .subspace("SQLitePolymorphicOptionalVectorDocument_embedding")

        return try await container.engine.withTransaction { transaction -> Int in
            let (begin, end) = indexSubspace.range()
            return try await transaction.collectRange(
                begin: begin,
                end: end,
                snapshot: true
            ).count
        }
    }

    private func sqliteVectorResultID(_ result: PolymorphicQueryResult) throws -> String? {
        if let article = try result.decodedModel(as: SQLitePolymorphicVectorArticle.self) {
            return article.id
        }
        if let report = try result.decodedModel(as: SQLitePolymorphicVectorReport.self) {
            return report.id
        }
        return nil
    }

    private func sqliteOptionalVectorResultID(_ result: PolymorphicQueryResult) throws -> String? {
        if let article = try result.decodedModel(as: SQLitePolymorphicOptionalVectorArticle.self) {
            return article.id
        }
        if let report = try result.decodedModel(as: SQLitePolymorphicOptionalVectorReport.self) {
            return report.id
        }
        return nil
    }

    private func sqlitePolymorphicResultID(_ result: PolymorphicQueryResult) throws -> String? {
        if let article = try result.decodedModel(as: SQLitePolymorphicArticle.self) {
            return article.id
        }
        if let report = try result.decodedModel(as: SQLitePolymorphicReport.self) {
            return report.id
        }
        return nil
    }

    @Test("public SQLite container reopen keeps polymorphic data and shared indexes queryable")
    func publicSQLiteContainerReopenKeepsPolymorphicDataAndSharedIndexesQueryable() async throws {
        let database = try SQLiteTestDatabase(
            prefix: "polymorphic-fetch-reopen"
        )
        defer { database.remove() }
        let schema = try Schema(
            entities: [
                try SQLitePolymorphicArticle.schemaEntity,
                try SQLitePolymorphicReport.schemaEntity,
            ],
            version: Schema.Version(1, 0, 0)
        )
        let runtimeConfiguration = try DatabaseFrameworkRuntime.configuration(
            entityRuntimes: [
                try DatabaseFrameworkRuntime.entity(SQLitePolymorphicArticle.self),
                try DatabaseFrameworkRuntime.entity(SQLitePolymorphicReport.self),
            ]
        )
        let initialContainer = try await DBContainer.sqlite(
            for: schema,
            path: database.path,
            runtimeConfiguration: runtimeConfiguration,
            security: .disabled
        )
        let initialContext = initialContainer.newContext()

        var article = SQLitePolymorphicArticle(title: "Catalog Needle Article", body: "Body")
        article.id = "sqlite-polymorphic-reopen-article"
        var report = SQLitePolymorphicReport(title: "Catalog Needle Report", pageCount: 3)
        report.id = "sqlite-polymorphic-reopen-report"

        try initialContext.insert(article)
        try initialContext.insert(report)
        try await initialContext.save()

        let registry = SchemaRegistry(
            database: initialContainer.engine,
            clock: TestProcessMonotonicClock()
        )
        let persistedEntities = try await registry.loadAll()
        let persistedEntityNames = persistedEntities.map(\.name)
        #expect(persistedEntityNames.contains(SQLitePolymorphicArticle.persistableType))
        #expect(persistedEntityNames.contains(SQLitePolymorphicReport.persistableType))
        await initialContainer.shutdown()

        let reopenedContainer = try await DBContainer.sqlite(
            for: schema,
            path: database.path,
            runtimeConfiguration: runtimeConfiguration,
            security: .disabled
        )
        defer { await reopenedContainer.shutdown() }
        let reopenedContext = reopenedContainer.newContext()
        let fetched = try await reopenedContext.fetchPolymorphic(SQLitePolymorphicArticle.self)
        let fullTextResults = try await reopenedContext.findPolymorphic(SQLitePolymorphicArticle.self)
            .fullText(SQLitePolymorphicArticle.fields.title)
            .term("needle")
            .execute()
        let fullTextIDs = try Set(fullTextResults.compactMap(sqlitePolymorphicResultID))

        #expect(fetched.count == 2)
        #expect(fullTextIDs == Set([article.id, report.id]))
        #expect(try await countPolymorphicIndexEntries(
            container: reopenedContainer,
            indexName: "SQLitePolymorphicDocument_title"
        ) == 2)
    }

    @Test("fetchPolymorphic returns SQLite transaction-maintained projections")
    func fetchPolymorphicScansMaintainedProjections() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let article = SQLitePolymorphicArticle(title: "Hello", body: "World")
        let report = SQLitePolymorphicReport(title: "Quarterly", pageCount: 42)

        try context.insert(article)
        try context.insert(report)
        try await context.save()

        let items = try await context.fetchPolymorphic(SQLitePolymorphicArticle.self)

        #expect(items.count == 2)
        #expect(Set(items.map(\.entity)) == Set([
            SQLitePolymorphicArticle.persistableType,
            SQLitePolymorphicReport.persistableType,
        ]))
    }

    @Test("fetchPolymorphic(id:) retrieves SQLite items across concrete types")
    func fetchPolymorphicByIDAcrossTypes() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let article = SQLitePolymorphicArticle(title: "Headline", body: "Body text")
        let report = SQLitePolymorphicReport(title: "Audit", pageCount: 7)

        try context.insert(article)
        try context.insert(report)
        try await context.save()

        let fetchedArticle = try await context.fetchPolymorphic(SQLitePolymorphicArticle.self, id: article.id)
        let fetchedReport = try await context.fetchPolymorphic(SQLitePolymorphicArticle.self, id: report.id)
        let missing = try await context.fetchPolymorphic(SQLitePolymorphicArticle.self, id: "does-not-exist")

        #expect(try fetchedArticle?.decode(as: SQLitePolymorphicArticle.self).title == "Headline")
        #expect(try fetchedReport?.decode(as: SQLitePolymorphicReport.self).pageCount == 7)
        #expect(missing == nil)
    }

    @Test("findPolymorphic decodes mixed SQLite rows with ordering and continuation")
    func findPolymorphicDecodesMixedSQLiteRowsWithContinuation() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let gamma = SQLitePolymorphicArticle(title: "Gamma", body: "third")
        let alpha = SQLitePolymorphicReport(title: "Alpha", pageCount: 1)
        let beta = SQLitePolymorphicArticle(title: "Beta", body: "second")

        try context.insert(gamma)
        try context.insert(alpha)
        try context.insert(beta)
        try await context.save()

        let firstPage = try await context.findPolymorphic(SQLitePolymorphicArticle.self)
            .orderBy(SQLitePolymorphicArticle.fields.title)
            .pageSize(2)
            .executePage()

        #expect(firstPage.results.map { $0.fields["title"]?.stringValue } == ["Alpha", "Beta"])
        #expect(try firstPage.results.first?.decodedModel(as: SQLitePolymorphicReport.self)?.id == alpha.id)
        #expect(try firstPage.results.dropFirst().first?.decodedModel(as: SQLitePolymorphicArticle.self)?.id == beta.id)
        #expect(firstPage.continuation != nil)

        let secondPage = try await context.findPolymorphic(SQLitePolymorphicArticle.self)
            .orderBy(SQLitePolymorphicArticle.fields.title)
            .pageSize(2)
            .continuing(from: firstPage.continuation)
            .executePage()

        #expect(secondPage.results.map { $0.fields["title"]?.stringValue } == ["Gamma"])
        #expect(try secondPage.results.first?.decodedModel(as: SQLitePolymorphicArticle.self)?.id == gamma.id)
        #expect(secondPage.continuation == nil)
    }

    @Test("Projection maintenance updates SQLite shared polymorphic scalar indexes")
    func projectionMaintenanceUpdatesSharedScalarIndexes() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let article = SQLitePolymorphicArticle(title: "Indexed Article", body: "Body")
        let report = SQLitePolymorphicReport(title: "Indexed Report", pageCount: 4)

        try context.insert(article)
        try context.insert(report)
        try await context.save()

        #expect(try await countPolymorphicIndexEntries(
            container: container,
            indexName: "SQLitePolymorphicDocument_title"
        ) == 2)
        #expect(try await countPolymorphicIndexEntries(
            container: container,
            indexName: "SQLitePolymorphicDocument_id"
        ) == 2)
        #expect(try await countPolymorphicIndexEntries(
            container: container,
            indexName: "SQLitePolymorphicDocument_title",
            valuePrefix: "Indexed Article"
        ) == 1)
        #expect(try await countPolymorphicIndexEntries(
            container: container,
            indexName: "SQLitePolymorphicDocument_title",
            valuePrefix: "Indexed Report"
        ) == 1)
    }

    @Test("staged update and delete maintain SQLite shared scalar indexes")
    func stagedUpdateAndDeleteMaintainSharedScalarIndexes() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        var article = SQLitePolymorphicArticle(title: "Direct Indexed", body: "Saved directly")
        try context.upsert(article)
        try await context.save()

        #expect(try await countPolymorphicIndexEntries(
            container: container,
            indexName: "SQLitePolymorphicDocument_title"
        ) == 1)
        #expect(try await countPolymorphicIndexEntries(
            container: container,
            indexName: "SQLitePolymorphicDocument_title",
            valuePrefix: "Direct Indexed"
        ) == 1)

        article.title = "Direct Indexed Updated"
        try context.upsert(article)
        try await context.save()

        #expect(try await countPolymorphicIndexEntries(
            container: container,
            indexName: "SQLitePolymorphicDocument_title"
        ) == 1)
        #expect(try await countPolymorphicIndexEntries(
            container: container,
            indexName: "SQLitePolymorphicDocument_title",
            valuePrefix: "Direct Indexed"
        ) == 0)
        #expect(try await countPolymorphicIndexEntries(
            container: container,
            indexName: "SQLitePolymorphicDocument_title",
            valuePrefix: "Direct Indexed Updated"
        ) == 1)

        try context.delete(article)
        try await context.save()

        #expect(try await countPolymorphicIndexEntries(
            container: container,
            indexName: "SQLitePolymorphicDocument_title"
        ) == 0)
        #expect(try await countPolymorphicIndexEntries(
            container: container,
            indexName: "SQLitePolymorphicDocument_id"
        ) == 0)
    }

    @Test("context stale delete removes current SQLite shared polymorphic scalar index entries")
    func contextStaleDeleteRemovesCurrentSharedPolymorphicScalarIndexEntries() async throws {
        let container = try await setupContainer()

        var original = SQLitePolymorphicArticle(
            title: "Shared Stale Original",
            body: "original body"
        )
        original.id = "sqlite-polymorphic-stale-delete-article"

        let seedContext = container.newContext()
        try seedContext.insert(original)
        try await seedContext.save()

        var current = original
        current.title = "Shared Stale Current"
        current.body = "current body"
        let updateContext = container.newContext()
        try updateContext.update(current)
        try await updateContext.save()

        #expect(try await countPolymorphicIndexEntries(
            container: container,
            indexName: "SQLitePolymorphicDocument_title",
            valuePrefix: "Shared Stale Original"
        ) == 0)
        #expect(try await countPolymorphicIndexEntries(
            container: container,
            indexName: "SQLitePolymorphicDocument_title",
            valuePrefix: "Shared Stale Current"
        ) == 1)

        let deleteContext = container.newContext()
        try deleteContext.delete(original)
        try await deleteContext.save()

        let afterDelete = try await container.newContext()
            .fetchPolymorphic(SQLitePolymorphicArticle.self, id: original.id)

        #expect(afterDelete == nil)
        #expect(try await countPolymorphicIndexEntries(
            container: container,
            indexName: "SQLitePolymorphicDocument_title"
        ) == 0)
        #expect(try await countPolymorphicIndexEntries(
            container: container,
            indexName: "SQLitePolymorphicDocument_title",
            valuePrefix: "Shared Stale Original"
        ) == 0)
        #expect(try await countPolymorphicIndexEntries(
            container: container,
            indexName: "SQLitePolymorphicDocument_title",
            valuePrefix: "Shared Stale Current"
        ) == 0)
    }

    @Test("deleteAll removes only the target concrete type from SQLite shared polymorphic indexes")
    func deleteAllRemovesOnlyTargetConcreteTypeFromSharedPolymorphicIndexes() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        var article = SQLitePolymorphicArticle(
            title: "Clear Target Article",
            body: "Body"
        )
        article.id = "sqlite-polymorphic-clear-target-article"
        var report = SQLitePolymorphicReport(
            title: "Clear Survivor Report",
            pageCount: 12
        )
        report.id = "sqlite-polymorphic-clear-survivor-report"

        try context.insert(article)
        try context.insert(report)
        try await context.save()

        #expect(try await countPolymorphicIndexEntries(
            container: container,
            indexName: "SQLitePolymorphicDocument_title"
        ) == 2)

        try await context.deleteAll(SQLitePolymorphicArticle.self)
        try await context.save()

        let remaining = try await context.fetchPolymorphic(SQLitePolymorphicArticle.self)
        let remainingIDs = try Set(remaining.map { item -> String in
            switch item.entity {
            case SQLitePolymorphicArticle.persistableType:
                return try item.decode(as: SQLitePolymorphicArticle.self).id
            case SQLitePolymorphicReport.persistableType:
                return try item.decode(as: SQLitePolymorphicReport.self).id
            default:
                throw PolymorphicQueryError.unknownType(item.entity)
            }
        })

        #expect(remainingIDs == Set([report.id]))
        #expect(try await countPolymorphicIndexEntries(
            container: container,
            indexName: "SQLitePolymorphicDocument_title"
        ) == 1)
        #expect(try await countPolymorphicIndexEntries(
            container: container,
            indexName: "SQLitePolymorphicDocument_title",
            valuePrefix: "Clear Target Article"
        ) == 0)
        #expect(try await countPolymorphicIndexEntries(
            container: container,
            indexName: "SQLitePolymorphicDocument_title",
            valuePrefix: "Clear Survivor Report"
        ) == 1)
    }

    @Test("staged polymorphic writes evaluate security against stored rows")
    func stagedPolymorphicWritesEvaluateSecurityAgainstStoredRows() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        let schema = try Schema(
            entities: [try SQLiteSecurePolymorphicArticle.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )
        let container = try await DBContainer.open(
            for: schema,
            configuration: .testing(storageEngine: engine),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
            entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteSecurePolymorphicArticle.self)],
                authorizationPolicies: [
                    AuthorizationPolicyHandler(
                        SQLiteSecurePolymorphicArticle.self
                    )
                ]
            ),
            security: .enabled()
        )

        var original = SQLiteSecurePolymorphicArticle(
            title: "Secure Original",
            ownerID: "alice",
            body: "Created by Alice"
        )
        original.id = "sqlite-secure-polymorphic-article"
        try await RequestAuthorization.$context.withValue(
            .authenticated(Principal(identifier: "alice"))
        ) {
            let context = container.newContext()
            try context.insert(original)
            try await context.save()
        }

        var transferred = original
        transferred.title = "Secure Transferred"
        transferred.ownerID = "bob"
        transferred.body = "Transferred to Bob"
        try await RequestAuthorization.$context.withValue(
            .authenticated(Principal(identifier: "alice"))
        ) {
            let context = container.newContext()
            try context.upsert(transferred)
            try await context.save()
        }

        var deniedUpdate = transferred
        deniedUpdate.title = "Secure Unauthorized"
        deniedUpdate.body = "Alice should not be able to update after transfer"
        do {
            try await RequestAuthorization.$context.withValue(
                .authenticated(Principal(identifier: "alice"))
            ) {
                let context = container.newContext()
                try context.upsert(deniedUpdate)
                try await context.save()
            }
            Issue.record("Expected transferred polymorphic update to be denied")
        } catch let error as SecurityError {
            #expect(error.operation == .update)
            #expect(error.userID == "alice")
        }

        do {
            try await RequestAuthorization.$context.withValue(
                .authenticated(Principal(identifier: "alice"))
            ) {
                let context = container.newContext()
                try context.delete(transferred, precondition: .exists)
                try await context.save()
            }
            Issue.record("Expected transferred polymorphic delete to be denied")
        } catch let error as SecurityError {
            #expect(error.operation == .delete)
            #expect(error.resource?.id == .string(original.id))
            #expect(error.userID == "alice")
        }

        #expect(try await countSecurePolymorphicIndexEntries(
            container: container,
            valuePrefix: "Secure Original"
        ) == 0)
        #expect(try await countSecurePolymorphicIndexEntries(
            container: container,
            valuePrefix: "Secure Transferred"
        ) == 1)
        #expect(try await countSecurePolymorphicIndexEntries(
            container: container,
            valuePrefix: "Secure Unauthorized"
        ) == 0)

        let fetchedAsBob = try await RequestAuthorization.$context.withValue(
            .authenticated(Principal(identifier: "bob"))
        ) {
            try await container.newContext().fetchPolymorphic(
                SQLiteSecurePolymorphicArticle.self,
                id: original.id
            )
        }
        let decodedForBob = try fetchedAsBob?.decode(
            as: SQLiteSecurePolymorphicArticle.self
        )
        #expect(decodedForBob?.title == "Secure Transferred")
        #expect(decodedForBob?.ownerID == "bob")

        try await RequestAuthorization.$context.withValue(
            .authenticated(Principal(identifier: "bob"))
        ) {
            let context = container.newContext()
            try context.delete(transferred, precondition: .exists)
            try await context.save()
        }

        #expect(try await countSecurePolymorphicIndexEntries(container: container) == 0)
    }

    @Test("polymorphic SQLite full-text query resolves shared descriptor and maintains indexes")
    func polymorphicSQLiteFullTextQueryResolvesSharedDescriptorAndMaintainsIndexes() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let article = SQLitePolymorphicArticle(title: "Needle Article", body: "Body")
        var report = SQLitePolymorphicReport(title: "Needle Report", pageCount: 4)
        let unrelated = SQLitePolymorphicReport(title: "Haystack", pageCount: 8)

        try context.insert(article)
        try context.insert(report)
        try context.insert(unrelated)
        try await context.save()

        let initial = try await context.findPolymorphic(SQLitePolymorphicArticle.self)
            .fullText(SQLitePolymorphicArticle.fields.title)
            .term("needle")
            .execute()
        let initialIDs = try Set(initial.compactMap(sqlitePolymorphicResultID))

        #expect(initialIDs == Set([article.id, report.id]))

        report.title = "Beacon Report"
        try context.upsert(report)
        try await context.save()

        let afterUpdateNeedle = try await context.findPolymorphic(SQLitePolymorphicArticle.self)
            .fullText(SQLitePolymorphicArticle.fields.title)
            .term("needle")
            .execute()
        let afterUpdateBeacon = try await context.findPolymorphic(SQLitePolymorphicArticle.self)
            .fullText(SQLitePolymorphicArticle.fields.title)
            .term("beacon")
            .execute()

        #expect(afterUpdateNeedle.count == 1)
        #expect(try afterUpdateNeedle.first?.decodedModel(as: SQLitePolymorphicArticle.self)?.id == article.id)
        #expect(afterUpdateBeacon.count == 1)
        #expect(try afterUpdateBeacon.first?.decodedModel(as: SQLitePolymorphicReport.self)?.id == report.id)

        try context.delete(article)
        try await context.save()

        let afterDeleteNeedle = try await context.findPolymorphic(SQLitePolymorphicArticle.self)
            .fullText(SQLitePolymorphicArticle.fields.title)
            .term("needle")
            .execute()

        #expect(afterDeleteNeedle.isEmpty)
    }

    @Test("staged writes are visible to polymorphic fetches on SQLite")
    func stagedWriteIsVisibleToPolymorphicFetches() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let article = SQLitePolymorphicArticle(title: "Direct", body: "Saved via staged upsert")
        try context.upsert(article)
        try await context.save()

        let scanned = try await context.fetchPolymorphic(SQLitePolymorphicArticle.self)
        let fetchedByID = try await context.fetchPolymorphic(SQLitePolymorphicArticle.self, id: article.id)

        #expect(scanned.count == 1)
        #expect(try scanned.first?.decode(as: SQLitePolymorphicArticle.self).title == "Direct")
        #expect(try fetchedByID?.decode(as: SQLitePolymorphicArticle.self).id == article.id)
    }

    @Test("staged delete removes a SQLite item from the shared directory")
    func stagedDeleteRemovesItem() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let article = SQLitePolymorphicArticle(title: "Doomed", body: "Delete me")
        try context.upsert(article)
        try await context.save()

        let beforeDelete = try await context.fetchPolymorphic(SQLitePolymorphicArticle.self, id: article.id)
        #expect(beforeDelete != nil)

        try context.delete(article)
        try await context.save()

        let afterDelete = try await context.fetchPolymorphic(SQLitePolymorphicArticle.self, id: article.id)
        let remaining = try await context.fetchPolymorphic(SQLitePolymorphicArticle.self)

        #expect(afterDelete == nil)
        #expect(remaining.isEmpty)
    }

    @Test("Polymorphic vector query requires a query vector on SQLite")
    func polymorphicVectorQueryRequiresQueryVectorOnSQLite() async throws {
        let container = try await setupVectorContainer()
        let context = container.newContext()

        do {
            _ = try await context.findPolymorphic(SQLitePolymorphicVectorArticle.self)
                .vector(SQLitePolymorphicVectorArticle.fields.embedding, dimensions: 3)
                .executePage()
            Issue.record("Expected VectorQueryError.noQueryVector")
        } catch VectorQueryError.noQueryVector {
        } catch {
            Issue.record("Expected VectorQueryError.noQueryVector, got \(error)")
        }
    }

    @Test("Polymorphic vector query rejects mismatched dimensions on SQLite")
    func polymorphicVectorQueryRejectsMismatchedDimensionsOnSQLite() async throws {
        let container = try await setupVectorContainer()
        let context = container.newContext()

        do {
            _ = try await context.findPolymorphic(SQLitePolymorphicVectorArticle.self)
                .vector(SQLitePolymorphicVectorArticle.fields.embedding, dimensions: 3)
                .query([1.0, 0.0], k: 1)
                .executePage()
            Issue.record("Expected VectorQueryError.dimensionMismatch")
        } catch VectorQueryError.dimensionMismatch(let expected, let actual) {
            #expect(expected == 3)
            #expect(actual == 2)
        } catch {
            Issue.record("Expected VectorQueryError.dimensionMismatch, got \(error)")
        }
    }

    @Test("Polymorphic vector query reports missing shared descriptor on SQLite")
    func polymorphicVectorQueryReportsMissingSharedDescriptorOnSQLite() async throws {
        let container = try await setupNoIndexVectorContainer()
        let context = container.newContext()

        do {
            _ = try await context.findPolymorphic(SQLitePolymorphicVectorNoIndexArticle.self)
                .vector(
                    SQLitePolymorphicVectorNoIndexArticle.fields.embedding,
                    dimensions: 3
                )
                .query([1.0, 0.0, 0.0], k: 1)
                .executePage()
            Issue.record("Expected PolymorphicVectorQueryError.indexNotFound")
        } catch PolymorphicVectorQueryError.indexNotFound(let groupIdentifier, let fieldName) {
            #expect(groupIdentifier == SQLitePolymorphicVectorNoIndexArticle.polymorphableType)
            #expect(fieldName == "embedding")
        } catch {
            Issue.record("Expected PolymorphicVectorQueryError.indexNotFound, got \(error)")
        }
    }

    @Test("Polymorphic optional vector KeyPath overload queries shared index end-to-end on SQLite")
    func polymorphicOptionalVectorKeyPathOverloadQueriesSharedIndexEndToEndOnSQLite() async throws {
        let container = try await setupOptionalVectorContainer()

        let context = container.newContext()
        let article = SQLitePolymorphicOptionalVectorArticle(
            title: "Optional Anchor",
            embedding: try Vector(float32: [1.0, 0.0, 0.0]),
            body: "Article body"
        )
        let report = SQLitePolymorphicOptionalVectorReport(
            title: "Optional Near",
            embedding: try Vector(float32: [0.95, 0.05, 0.0]),
            pageCount: 3
        )

        try context.insert(article)
        try context.insert(report)
        try await context.save()

        #expect(try await countPolymorphicOptionalVectorIndexEntries(container: container) == 2)

        let first = try await context.findPolymorphic(SQLitePolymorphicOptionalVectorArticle.self)
            .vector(
                SQLitePolymorphicOptionalVectorArticle.fields.embedding,
                dimensions: 3
            )
            .query([1.0, 0.0, 0.0], k: 1)
            .first()

        #expect(try first?.decodedModel(as: SQLitePolymorphicOptionalVectorArticle.self)?.id == article.id)

        let results = try await context.findPolymorphic(SQLitePolymorphicOptionalVectorReport.self)
            .vector(
                SQLitePolymorphicOptionalVectorReport.fields.embedding,
                dimensions: 3
            )
            .query([1.0, 0.0, 0.0], k: 2)
            .execute()
        let resultIDs = try Set(results.compactMap(sqliteOptionalVectorResultID))

        #expect(resultIDs == Set([article.id, report.id]))
    }

    @Test("Polymorphic vector index is maintained and queried end-to-end on SQLite")
    func polymorphicVectorIndexIsMaintainedAndQueriedEndToEndOnSQLite() async throws {
        let container = try await setupVectorContainer()

        let context = container.newContext()

        let article = SQLitePolymorphicVectorArticle(
            title: "Anchor",
            embedding: try Vector(float32: [1.0, 0.0, 0.0]),
            body: "Article body"
        )
        var report = SQLitePolymorphicVectorReport(
            title: "Near",
            embedding: try Vector(float32: [0.95, 0.05, 0.0]),
            pageCount: 3
        )
        let farReport = SQLitePolymorphicVectorReport(
            title: "Far",
            embedding: try Vector(float32: [0.0, 1.0, 0.0]),
            pageCount: 9
        )

        try context.insert(article)
        try context.insert(report)
        try context.insert(farReport)
        try await context.save()

        #expect(try await countPolymorphicVectorIndexEntries(container: container) == 3)

        let firstPage = try await context.findPolymorphic(SQLitePolymorphicVectorArticle.self)
            .vector(SQLitePolymorphicVectorArticle.fields.embedding, dimensions: 3)
            .query([1.0, 0.0, 0.0], k: 2)
            .metric(.cosine)
            .executePage()

        #expect(firstPage.results.count == 2)
        #expect(try firstPage.results.first?.decodedModel(as: SQLitePolymorphicVectorArticle.self)?.id == article.id)
        #expect(try firstPage.results.dropFirst().first?.decodedModel(as: SQLitePolymorphicVectorReport.self)?.id == report.id)

        let reportStartedPage = try await context.findPolymorphic(SQLitePolymorphicVectorReport.self)
            .vector(SQLitePolymorphicVectorReport.fields.embedding, dimensions: 3)
            .query([1.0, 0.0, 0.0], k: 2)
            .metric(.cosine)
            .executePage()
        let reportStartedIDs = try Set(reportStartedPage.results.compactMap(sqliteVectorResultID))

        #expect(reportStartedIDs == Set([article.id, report.id]))

        report.embedding = try Vector(float32: [1.0, 0.0, 0.0])
        try context.upsert(report)
        try await context.save()

        #expect(try await countPolymorphicVectorIndexEntries(container: container) == 3)

        let updatedPage = try await context.findPolymorphic(SQLitePolymorphicVectorArticle.self)
            .vector(SQLitePolymorphicVectorArticle.fields.embedding, dimensions: 3)
            .query([1.0, 0.0, 0.0], k: 2)
            .metric(.cosine)
            .executePage()
        let updatedIDs = try Set(updatedPage.results.compactMap(sqliteVectorResultID))

        #expect(updatedIDs == Set([article.id, report.id]))

        try context.delete(article)
        try await context.save()

        #expect(try await countPolymorphicVectorIndexEntries(container: container) == 2)

        let finalPage = try await context.findPolymorphic(SQLitePolymorphicVectorArticle.self)
            .vector(SQLitePolymorphicVectorArticle.fields.embedding, dimensions: 3)
            .query([1.0, 0.0, 0.0], k: 1)
            .metric(.cosine)
            .executePage()

        #expect(finalPage.results.count == 1)
        #expect(try finalPage.results.first?.decodedModel(as: SQLitePolymorphicVectorReport.self)?.id == report.id)
    }
}
#endif
