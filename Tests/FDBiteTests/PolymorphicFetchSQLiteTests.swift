#if SQLITE
import Testing
import Foundation
import Database
import StorageKit
import TestHeartbeat

// MARK: - Test Types

protocol SQLitePolymorphicDocument: Polymorphable {
    var id: String { get }
    var title: String { get }
}

extension SQLitePolymorphicDocument {
    public static var polymorphableType: String { "SQLitePolymorphicDocument" }

    public static var polymorphicDirectoryPathComponents: [any DirectoryPathElement] {
        [Path("sqlite_polymorphic_fetch_shared")]
    }

    public static var polymorphicIndexDescriptors: [IndexDescriptor] {
        [
            IndexDescriptor(
                name: "SQLitePolymorphicDocument_title",
                keyPaths: [\Self.title],
                kind: ScalarIndexKind<Self>(fields: [\Self.title])
            ),
            IndexDescriptor(
                name: "SQLitePolymorphicDocument_id",
                keyPaths: [\Self.id],
                kind: ScalarIndexKind<Self>(fields: [\Self.id])
            ),
            IndexDescriptor(
                name: "SQLitePolymorphicDocument_title_fulltext",
                keyPaths: [\Self.title],
                kind: FullTextIndexKind<Self>(fields: [\Self.title], tokenizer: .simple)
            ),
        ]
    }
}

@Persistable
struct SQLitePolymorphicArticle: SQLitePolymorphicDocument {
    #Directory<SQLitePolymorphicArticle>("sqlite_polymorphic_fetch_articles")

    var id: String = ULID().ulidString
    var title: String
    var body: String
}

@Persistable
struct SQLitePolymorphicReport: SQLitePolymorphicDocument {
    #Directory<SQLitePolymorphicReport>("sqlite_polymorphic_fetch_reports")

    var id: String = ULID().ulidString
    var title: String
    var pageCount: Int
}

protocol SQLiteSecurePolymorphicDocument: Polymorphable {
    var id: String { get }
    var title: String { get }
    var ownerID: String { get }
}

extension SQLiteSecurePolymorphicDocument {
    public static var polymorphableType: String { "SQLiteSecurePolymorphicDocument" }

    public static var polymorphicDirectoryPathComponents: [any DirectoryPathElement] {
        [Path("sqlite_secure_polymorphic_shared")]
    }

    public static var polymorphicIndexDescriptors: [IndexDescriptor] {
        [
            IndexDescriptor(
                name: "SQLiteSecurePolymorphicDocument_title",
                keyPaths: [\Self.title],
                kind: ScalarIndexKind<Self>(fields: [\Self.title])
            )
        ]
    }
}

@Persistable
struct SQLiteSecurePolymorphicArticle: SQLiteSecurePolymorphicDocument, SecurityPolicy {
    #Directory<SQLiteSecurePolymorphicArticle>("sqlite_secure_polymorphic_articles")

    var id: String = ULID().ulidString
    var title: String
    var ownerID: String
    var body: String

    static func allowGet(
        resource: SQLiteSecurePolymorphicArticle,
        auth: (any AuthContext)?
    ) -> Bool {
        resource.ownerID == auth?.userID
    }

    static func allowList(
        query: SecurityQuery<SQLiteSecurePolymorphicArticle>,
        auth: (any AuthContext)?
    ) -> Bool {
        auth != nil
    }

    static func allowCreate(
        newResource: SQLiteSecurePolymorphicArticle,
        auth: (any AuthContext)?
    ) -> Bool {
        newResource.ownerID == auth?.userID
    }

    static func allowUpdate(
        resource: SQLiteSecurePolymorphicArticle,
        newResource: SQLiteSecurePolymorphicArticle,
        auth: (any AuthContext)?
    ) -> Bool {
        resource.ownerID == auth?.userID
    }

    static func allowDelete(
        resource: SQLiteSecurePolymorphicArticle,
        auth: (any AuthContext)?
    ) -> Bool {
        resource.ownerID == auth?.userID
    }
}

private struct SQLitePolymorphicTestAuth: AuthContext {
    let userID: String
    var roles: Set<String> = []
}

protocol SQLitePolymorphicVectorDocument: Polymorphable {
    var id: String { get }
    var title: String { get }
    var embedding: [Float] { get }
}

extension SQLitePolymorphicVectorDocument {
    public static var polymorphableType: String { "SQLitePolymorphicVectorDocument" }

    public static var polymorphicDirectoryPathComponents: [any DirectoryPathElement] {
        [Path("sqlite_polymorphic_vector_shared")]
    }

    public static var polymorphicIndexDescriptors: [IndexDescriptor] {
        [
            IndexDescriptor(
                name: "SQLitePolymorphicVectorDocument_embedding",
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

@Persistable
struct SQLitePolymorphicVectorArticle: SQLitePolymorphicVectorDocument {
    #Directory<SQLitePolymorphicVectorArticle>("sqlite_polymorphic_vector_articles")

    var id: String = ULID().ulidString
    var title: String
    var embedding: [Float]
    var body: String
}

@Persistable
struct SQLitePolymorphicVectorReport: SQLitePolymorphicVectorDocument {
    #Directory<SQLitePolymorphicVectorReport>("sqlite_polymorphic_vector_reports")

    var id: String = ULID().ulidString
    var title: String
    var embedding: [Float]
    var pageCount: Int
}

protocol SQLitePolymorphicVectorNoIndexDocument: Polymorphable {
    var id: String { get }
    var title: String { get }
    var embedding: [Float] { get }
}

extension SQLitePolymorphicVectorNoIndexDocument {
    public static var polymorphableType: String { "SQLitePolymorphicVectorNoIndexDocument" }

    public static var polymorphicDirectoryPathComponents: [any DirectoryPathElement] {
        [Path("sqlite_polymorphic_vector_no_index_shared")]
    }
}

@Persistable
struct SQLitePolymorphicVectorNoIndexArticle: SQLitePolymorphicVectorNoIndexDocument {
    #Directory<SQLitePolymorphicVectorNoIndexArticle>("sqlite_polymorphic_vector_no_index_articles")

    var id: String = ULID().ulidString
    var title: String
    var embedding: [Float]
    var body: String
}

protocol SQLitePolymorphicOptionalVectorDocument: Polymorphable {
    var id: String { get }
    var title: String { get }
    var embedding: [Float]? { get }
}

extension SQLitePolymorphicOptionalVectorDocument {
    public static var polymorphableType: String { "SQLitePolymorphicOptionalVectorDocument" }

    public static var polymorphicDirectoryPathComponents: [any DirectoryPathElement] {
        [Path("sqlite_polymorphic_optional_vector_shared")]
    }

    public static var polymorphicIndexDescriptors: [IndexDescriptor] {
        [
            IndexDescriptor(
                name: "SQLitePolymorphicOptionalVectorDocument_embedding",
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

@Persistable
struct SQLitePolymorphicOptionalVectorArticle: SQLitePolymorphicOptionalVectorDocument {
    #Directory<SQLitePolymorphicOptionalVectorArticle>("sqlite_polymorphic_optional_vector_articles")

    var id: String = ULID().ulidString
    var title: String
    var embedding: [Float]?
    var body: String
}

@Persistable
struct SQLitePolymorphicOptionalVectorReport: SQLitePolymorphicOptionalVectorDocument {
    #Directory<SQLitePolymorphicOptionalVectorReport>("sqlite_polymorphic_optional_vector_reports")

    var id: String = ULID().ulidString
    var title: String
    var embedding: [Float]?
    var pageCount: Int
}

@Suite("Polymorphic Fetch SQLite Tests", .serialized, .heartbeat)
struct PolymorphicFetchSQLiteTests {

    private func setupContainer() async throws -> DBContainer {
        let schema = Schema(
            [SQLitePolymorphicArticle.self, SQLitePolymorphicReport.self],
            version: Schema.Version(1, 0, 0)
        )

        return try await DBContainer.inMemory(for: schema, security: .disabled)
    }

    private func setupVectorContainer() async throws -> DBContainer {
        let schema = Schema(
            [SQLitePolymorphicVectorArticle.self, SQLitePolymorphicVectorReport.self],
            version: Schema.Version(1, 0, 0)
        )

        return try await DBContainer.inMemory(for: schema, security: .disabled)
    }

    private func setupOptionalVectorContainer() async throws -> DBContainer {
        let schema = Schema(
            [SQLitePolymorphicOptionalVectorArticle.self, SQLitePolymorphicOptionalVectorReport.self],
            version: Schema.Version(1, 0, 0)
        )

        return try await DBContainer.inMemory(for: schema, security: .disabled)
    }

    private func setupNoIndexVectorContainer() async throws -> DBContainer {
        let schema = Schema(
            [SQLitePolymorphicVectorNoIndexArticle.self],
            version: Schema.Version(1, 0, 0)
        )

        return try await DBContainer.inMemory(for: schema, security: .disabled)
    }

    private func countPolymorphicIndexEntries(
        container: DBContainer,
        indexName: String,
        valuePrefix: String? = nil
    ) async throws -> Int {
        let group = try container.polymorphicGroup(identifier: SQLitePolymorphicArticle.polymorphableType)
        let groupSubspace = try await container.resolvePolymorphicDirectory(for: group.identifier)
        var indexSubspace = groupSubspace
            .subspace(SubspaceKey.indexes)
            .subspace(indexName)

        if let valuePrefix {
            indexSubspace = indexSubspace.subspace(valuePrefix)
        }

        return try await container.engine.withTransaction { transaction -> Int in
            let (begin, end) = indexSubspace.range()
            var count = 0
            for try await _ in transaction.getRange(begin: begin, end: end, snapshot: true) {
                count += 1
            }
            return count
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
            var count = 0
            for try await _ in transaction.getRange(begin: begin, end: end, snapshot: true) {
                count += 1
            }
            return count
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
        var indexSubspace = groupSubspace
            .subspace(SubspaceKey.indexes)
            .subspace("SQLiteSecurePolymorphicDocument_title")

        if let valuePrefix {
            indexSubspace = indexSubspace.subspace(valuePrefix)
        }

        return try await container.engine.withTransaction { transaction -> Int in
            let (begin, end) = indexSubspace.range()
            var count = 0
            for try await _ in transaction.getRange(begin: begin, end: end, snapshot: true) {
                count += 1
            }
            return count
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
            var count = 0
            for try await _ in transaction.getRange(begin: begin, end: end, snapshot: true) {
                count += 1
            }
            return count
        }
    }

    private func sqliteVectorResultID(_ result: PolymorphicQueryResult) -> String? {
        if let article = result.item(as: SQLitePolymorphicVectorArticle.self) {
            return article.id
        }
        if let report = result.item(as: SQLitePolymorphicVectorReport.self) {
            return report.id
        }
        return nil
    }

    private func sqliteOptionalVectorResultID(_ result: PolymorphicQueryResult) -> String? {
        if let article = result.item(as: SQLitePolymorphicOptionalVectorArticle.self) {
            return article.id
        }
        if let report = result.item(as: SQLitePolymorphicOptionalVectorReport.self) {
            return report.id
        }
        return nil
    }

    private func sqlitePolymorphicResultID(_ result: PolymorphicQueryResult) -> String? {
        if let article = result.item(as: SQLitePolymorphicArticle.self) {
            return article.id
        }
        if let report = result.item(as: SQLitePolymorphicReport.self) {
            return report.id
        }
        return nil
    }

    @Test("public SQLite container reopen keeps polymorphic data and shared indexes queryable")
    func publicSQLiteContainerReopenKeepsPolymorphicDataAndSharedIndexesQueryable() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        let schema = Schema(
            [SQLitePolymorphicArticle.self, SQLitePolymorphicReport.self],
            version: Schema.Version(1, 0, 0)
        )
        let initialContainer = try await DBContainer(
            for: schema,
            configuration: .init(backend: .custom(engine)),
            security: .disabled
        )
        let initialContext = initialContainer.newContext()

        var article = SQLitePolymorphicArticle(title: "Catalog Needle Article", body: "Body")
        article.id = "sqlite-polymorphic-reopen-article"
        var report = SQLitePolymorphicReport(title: "Catalog Needle Report", pageCount: 3)
        report.id = "sqlite-polymorphic-reopen-report"

        initialContext.insert(article)
        initialContext.insert(report)
        try await initialContext.save()

        let registry = SchemaRegistry(database: engine)
        let persistedEntities = try await registry.loadAll()
        let persistedEntityNames = persistedEntities.map(\.name)
        #expect(persistedEntityNames.contains(SQLitePolymorphicArticle.persistableType))
        #expect(persistedEntityNames.contains(SQLitePolymorphicReport.persistableType))

        let reopenedContainer = try await DBContainer(
            for: schema,
            configuration: .init(backend: .custom(engine)),
            security: .disabled
        )
        let reopenedContext = reopenedContainer.newContext()
        let fetched = try await reopenedContext.fetchPolymorphic(SQLitePolymorphicArticle.self)
        let fullTextResults = try await reopenedContext.findPolymorphic(SQLitePolymorphicArticle.self)
            .fullText(\.title)
            .term("needle")
            .execute()
        let fullTextIDs = Set(fullTextResults.compactMap(sqlitePolymorphicResultID))

        #expect(fetched.count == 2)
        #expect(fullTextIDs == Set([article.id, report.id]))
        #expect(try await countPolymorphicIndexEntries(
            container: reopenedContainer,
            indexName: "SQLitePolymorphicDocument_title"
        ) == 2)
    }

    @Test("fetchPolymorphic returns SQLite items written via dual-write")
    func fetchPolymorphicScanAfterDualWrite() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let article = SQLitePolymorphicArticle(title: "Hello", body: "World")
        let report = SQLitePolymorphicReport(title: "Quarterly", pageCount: 42)

        context.insert(article)
        context.insert(report)
        try await context.save()

        let items = try await context.fetchPolymorphic(SQLitePolymorphicArticle.self)

        #expect(items.count == 2)
        #expect(items.compactMap { $0 as? SQLitePolymorphicArticle }.count == 1)
        #expect(items.compactMap { $0 as? SQLitePolymorphicReport }.count == 1)
    }

    @Test("fetchPolymorphic(id:) retrieves SQLite items across concrete types")
    func fetchPolymorphicByIDAcrossTypes() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let article = SQLitePolymorphicArticle(title: "Headline", body: "Body text")
        let report = SQLitePolymorphicReport(title: "Audit", pageCount: 7)

        context.insert(article)
        context.insert(report)
        try await context.save()

        let fetchedArticle = try await context.fetchPolymorphic(SQLitePolymorphicArticle.self, id: article.id)
        let fetchedReport = try await context.fetchPolymorphic(SQLitePolymorphicArticle.self, id: report.id)
        let missing = try await context.fetchPolymorphic(SQLitePolymorphicArticle.self, id: "does-not-exist")

        #expect((fetchedArticle as? SQLitePolymorphicArticle)?.title == "Headline")
        #expect((fetchedReport as? SQLitePolymorphicReport)?.pageCount == 7)
        #expect(missing == nil)
    }

    @Test("findPolymorphic decodes mixed SQLite rows with ordering and continuation")
    func findPolymorphicDecodesMixedSQLiteRowsWithContinuation() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let gamma = SQLitePolymorphicArticle(title: "Gamma", body: "third")
        let alpha = SQLitePolymorphicReport(title: "Alpha", pageCount: 1)
        let beta = SQLitePolymorphicArticle(title: "Beta", body: "second")

        context.insert(gamma)
        context.insert(alpha)
        context.insert(beta)
        try await context.save()

        let firstPage = try await context.findPolymorphic(SQLitePolymorphicArticle.self)
            .orderBy(\.title)
            .pageSize(2)
            .executePage()

        #expect(firstPage.results.map { $0.fields["title"]?.stringValue } == ["Alpha", "Beta"])
        #expect(firstPage.results.first?.item(as: SQLitePolymorphicReport.self)?.id == alpha.id)
        #expect(firstPage.results.dropFirst().first?.item(as: SQLitePolymorphicArticle.self)?.id == beta.id)
        #expect(firstPage.continuation != nil)

        let secondPage = try await context.findPolymorphic(SQLitePolymorphicArticle.self)
            .orderBy(\.title)
            .pageSize(2)
            .continuing(from: firstPage.continuation)
            .executePage()

        #expect(secondPage.results.map { $0.fields["title"]?.stringValue } == ["Gamma"])
        #expect(secondPage.results.first?.item(as: SQLitePolymorphicArticle.self)?.id == gamma.id)
        #expect(secondPage.continuation == nil)
    }

    @Test("dual-write maintains SQLite shared polymorphic scalar indexes")
    func dualWriteMaintainsSharedPolymorphicScalarIndexes() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let article = SQLitePolymorphicArticle(title: "Indexed Article", body: "Body")
        let report = SQLitePolymorphicReport(title: "Indexed Report", pageCount: 4)

        context.insert(article)
        context.insert(report)
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

    @Test("savePolymorphic update and delete maintain SQLite shared scalar indexes")
    func savePolymorphicUpdateAndDeleteMaintainSharedScalarIndexes() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        var article = SQLitePolymorphicArticle(title: "Direct Indexed", body: "Saved directly")
        try await context.savePolymorphic(article, as: SQLitePolymorphicArticle.self)

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
        try await context.savePolymorphic(article, as: SQLitePolymorphicArticle.self)

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

        try await context.deletePolymorphic(
            SQLitePolymorphicArticle.self,
            id: article.id,
            as: SQLitePolymorphicArticle.self
        )

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
        seedContext.insert(original)
        try await seedContext.save()

        var current = original
        current.title = "Shared Stale Current"
        current.body = "current body"
        let updateContext = container.newContext()
        updateContext.replace(old: original, with: current)
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
        deleteContext.delete(original)
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

    @Test("clearAll removes only the target concrete type from SQLite shared polymorphic indexes")
    func clearAllRemovesOnlyTargetConcreteTypeFromSharedPolymorphicIndexes() async throws {
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

        context.insert(article)
        context.insert(report)
        try await context.save()

        #expect(try await countPolymorphicIndexEntries(
            container: container,
            indexName: "SQLitePolymorphicDocument_title"
        ) == 2)

        try await context.clearAll(SQLitePolymorphicArticle.self)

        let remaining = try await context.fetchPolymorphic(SQLitePolymorphicArticle.self)
        let remainingIDs = Set(remaining.compactMap { item -> String? in
            if let article = item as? SQLitePolymorphicArticle {
                return article.id
            }
            if let report = item as? SQLitePolymorphicReport {
                return report.id
            }
            return nil
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

    @Test("savePolymorphic and deletePolymorphic evaluate security against stored rows")
    func saveAndDeletePolymorphicEvaluateSecurityAgainstStoredRows() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        let schema = Schema(
            [SQLiteSecurePolymorphicArticle.self],
            version: Schema.Version(1, 0, 0)
        )
        let container = try await DBContainer(
            for: schema,
            configuration: .init(backend: .custom(engine)),
            security: .enabled()
        )

        var original = SQLiteSecurePolymorphicArticle(
            title: "Secure Original",
            ownerID: "alice",
            body: "Created by Alice"
        )
        original.id = "sqlite-secure-polymorphic-article"
        try await AuthContextKey.$current.withValue(SQLitePolymorphicTestAuth(userID: "alice")) {
            try await container.newContext().savePolymorphic(
                original,
                as: SQLiteSecurePolymorphicArticle.self
            )
        }

        var transferred = original
        transferred.title = "Secure Transferred"
        transferred.ownerID = "bob"
        transferred.body = "Transferred to Bob"
        try await AuthContextKey.$current.withValue(SQLitePolymorphicTestAuth(userID: "alice")) {
            try await container.newContext().savePolymorphic(
                transferred,
                as: SQLiteSecurePolymorphicArticle.self
            )
        }

        var deniedUpdate = transferred
        deniedUpdate.title = "Secure Unauthorized"
        deniedUpdate.body = "Alice should not be able to update after transfer"
        do {
            try await AuthContextKey.$current.withValue(SQLitePolymorphicTestAuth(userID: "alice")) {
                try await container.newContext().savePolymorphic(
                    deniedUpdate,
                    as: SQLiteSecurePolymorphicArticle.self
                )
            }
            Issue.record("Expected transferred polymorphic update to be denied")
        } catch let error as SecurityError {
            #expect(error.operation == .update)
            #expect(error.userID == "alice")
        }

        do {
            try await AuthContextKey.$current.withValue(SQLitePolymorphicTestAuth(userID: "alice")) {
                try await container.newContext().deletePolymorphic(
                    SQLiteSecurePolymorphicArticle.self,
                    id: original.id,
                    as: SQLiteSecurePolymorphicArticle.self
                )
            }
            Issue.record("Expected transferred polymorphic delete to be denied")
        } catch let error as SecurityError {
            #expect(error.operation == .delete)
            #expect(error.resourceID == original.id)
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

        let fetchedAsBob = try await AuthContextKey.$current.withValue(SQLitePolymorphicTestAuth(userID: "bob")) {
            try await container.newContext().fetchPolymorphic(
                SQLiteSecurePolymorphicArticle.self,
                id: original.id
            )
        }
        #expect((fetchedAsBob as? SQLiteSecurePolymorphicArticle)?.title == "Secure Transferred")
        #expect((fetchedAsBob as? SQLiteSecurePolymorphicArticle)?.ownerID == "bob")

        try await AuthContextKey.$current.withValue(SQLitePolymorphicTestAuth(userID: "bob")) {
            try await container.newContext().deletePolymorphic(
                SQLiteSecurePolymorphicArticle.self,
                id: original.id,
                as: SQLiteSecurePolymorphicArticle.self
            )
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

        context.insert(article)
        context.insert(report)
        context.insert(unrelated)
        try await context.save()

        let initial = try await context.findPolymorphic(SQLitePolymorphicArticle.self)
            .fullText(\.title)
            .term("needle")
            .execute()
        let initialIDs = Set(initial.compactMap(sqlitePolymorphicResultID))

        #expect(initialIDs == Set([article.id, report.id]))

        report.title = "Beacon Report"
        try await context.savePolymorphic(report, as: SQLitePolymorphicReport.self)

        let afterUpdateNeedle = try await context.findPolymorphic(SQLitePolymorphicArticle.self)
            .fullText(\.title)
            .term("needle")
            .execute()
        let afterUpdateBeacon = try await context.findPolymorphic(SQLitePolymorphicArticle.self)
            .fullText(\.title)
            .term("beacon")
            .execute()

        #expect(afterUpdateNeedle.count == 1)
        #expect(afterUpdateNeedle.first?.item(as: SQLitePolymorphicArticle.self)?.id == article.id)
        #expect(afterUpdateBeacon.count == 1)
        #expect(afterUpdateBeacon.first?.item(as: SQLitePolymorphicReport.self)?.id == report.id)

        try await context.deletePolymorphic(
            SQLitePolymorphicArticle.self,
            id: article.id,
            as: SQLitePolymorphicArticle.self
        )

        let afterDeleteNeedle = try await context.findPolymorphic(SQLitePolymorphicArticle.self)
            .fullText(\.title)
            .term("needle")
            .execute()

        #expect(afterDeleteNeedle.isEmpty)
    }

    @Test("savePolymorphic writes SQLite items visible to fetchPolymorphic")
    func savePolymorphicIsVisibleToFetchPolymorphic() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let article = SQLitePolymorphicArticle(title: "Direct", body: "Saved via savePolymorphic")
        try await context.savePolymorphic(article, as: SQLitePolymorphicArticle.self)

        let scanned = try await context.fetchPolymorphic(SQLitePolymorphicArticle.self)
        let fetchedByID = try await context.fetchPolymorphic(SQLitePolymorphicArticle.self, id: article.id)

        #expect(scanned.count == 1)
        #expect((scanned.first as? SQLitePolymorphicArticle)?.title == "Direct")
        #expect((fetchedByID as? SQLitePolymorphicArticle)?.id == article.id)
    }

    @Test("deletePolymorphic removes SQLite item from shared directory")
    func deletePolymorphicRemovesItem() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let article = SQLitePolymorphicArticle(title: "Doomed", body: "Delete me")
        try await context.savePolymorphic(article, as: SQLitePolymorphicArticle.self)

        let beforeDelete = try await context.fetchPolymorphic(SQLitePolymorphicArticle.self, id: article.id)
        #expect(beforeDelete != nil)

        try await context.deletePolymorphic(
            SQLitePolymorphicArticle.self,
            id: article.id,
            as: SQLitePolymorphicArticle.self
        )

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
                .vector(\.embedding, dimensions: 3)
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
                .vector(\.embedding, dimensions: 3)
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
                .vector(\.embedding, dimensions: 3)
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
        VectorReadBridge.registerReadExecutors()

        let context = container.newContext()
        let article = SQLitePolymorphicOptionalVectorArticle(
            title: "Optional Anchor",
            embedding: [1.0, 0.0, 0.0],
            body: "Article body"
        )
        let report = SQLitePolymorphicOptionalVectorReport(
            title: "Optional Near",
            embedding: [0.95, 0.05, 0.0],
            pageCount: 3
        )

        context.insert(article)
        context.insert(report)
        try await context.save()

        #expect(try await countPolymorphicOptionalVectorIndexEntries(container: container) == 2)

        let first = try await context.findPolymorphic(SQLitePolymorphicOptionalVectorArticle.self)
            .vector(\.embedding, dimensions: 3)
            .query([1.0, 0.0, 0.0], k: 1)
            .first()

        #expect(first?.item(as: SQLitePolymorphicOptionalVectorArticle.self)?.id == article.id)

        let results = try await context.findPolymorphic(SQLitePolymorphicOptionalVectorReport.self)
            .vector(\.embedding, dimensions: 3)
            .query([1.0, 0.0, 0.0], k: 2)
            .execute()
        let resultIDs = Set(results.compactMap(sqliteOptionalVectorResultID))

        #expect(resultIDs == Set([article.id, report.id]))
    }

    @Test("Polymorphic vector index is maintained and queried end-to-end on SQLite")
    func polymorphicVectorIndexIsMaintainedAndQueriedEndToEndOnSQLite() async throws {
        let container = try await setupVectorContainer()
        VectorReadBridge.registerReadExecutors()

        let context = container.newContext()

        let article = SQLitePolymorphicVectorArticle(
            title: "Anchor",
            embedding: [1.0, 0.0, 0.0],
            body: "Article body"
        )
        var report = SQLitePolymorphicVectorReport(
            title: "Near",
            embedding: [0.95, 0.05, 0.0],
            pageCount: 3
        )
        let farReport = SQLitePolymorphicVectorReport(
            title: "Far",
            embedding: [0.0, 1.0, 0.0],
            pageCount: 9
        )

        context.insert(article)
        context.insert(report)
        context.insert(farReport)
        try await context.save()

        #expect(try await countPolymorphicVectorIndexEntries(container: container) == 3)

        let firstPage = try await context.findPolymorphic(SQLitePolymorphicVectorArticle.self)
            .vector(\.embedding, dimensions: 3)
            .query([1.0, 0.0, 0.0], k: 2)
            .metric(.cosine)
            .executePage()

        #expect(firstPage.results.count == 2)
        #expect(firstPage.results.first?.item(as: SQLitePolymorphicVectorArticle.self)?.id == article.id)
        #expect(firstPage.results.dropFirst().first?.item(as: SQLitePolymorphicVectorReport.self)?.id == report.id)

        let reportStartedPage = try await context.findPolymorphic(SQLitePolymorphicVectorReport.self)
            .vector(\.embedding, dimensions: 3)
            .query([1.0, 0.0, 0.0], k: 2)
            .metric(.cosine)
            .executePage()
        let reportStartedIDs = Set(reportStartedPage.results.compactMap(sqliteVectorResultID))

        #expect(reportStartedIDs == Set([article.id, report.id]))

        report.embedding = [1.0, 0.0, 0.0]
        try await context.savePolymorphic(report, as: SQLitePolymorphicVectorReport.self)

        #expect(try await countPolymorphicVectorIndexEntries(container: container) == 3)

        let updatedPage = try await context.findPolymorphic(SQLitePolymorphicVectorArticle.self)
            .vector(\.embedding, dimensions: 3)
            .query([1.0, 0.0, 0.0], k: 2)
            .metric(.cosine)
            .executePage()
        let updatedIDs = Set(updatedPage.results.compactMap(sqliteVectorResultID))

        #expect(updatedIDs == Set([article.id, report.id]))

        try await context.deletePolymorphic(
            SQLitePolymorphicVectorArticle.self,
            id: article.id,
            as: SQLitePolymorphicVectorArticle.self
        )

        #expect(try await countPolymorphicVectorIndexEntries(container: container) == 2)

        let finalPage = try await context.findPolymorphic(SQLitePolymorphicVectorArticle.self)
            .vector(\.embedding, dimensions: 3)
            .query([1.0, 0.0, 0.0], k: 1)
            .metric(.cosine)
            .executePage()

        #expect(finalPage.results.count == 1)
        #expect(finalPage.results.first?.item(as: SQLitePolymorphicVectorReport.self)?.id == report.id)
    }
}
#endif
