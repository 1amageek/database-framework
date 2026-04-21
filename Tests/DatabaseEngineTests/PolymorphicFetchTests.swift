#if FOUNDATION_DB
import Testing
import TestHeartbeat
import Foundation
import StorageKit
import FDBStorage
import FoundationDB
import FullText
import TestSupport
@testable import DatabaseEngine
@testable import Core
@testable import FullTextIndex

// MARK: - Test Types
//
// Polymorphable conformance is declared manually (not via the @Polymorphable
// macro) because the Swift 6.3 frontend crashes when the #Directory
// freestanding macro is expanded inside a protocol body.

/// Polymorphic protocol with a shared directory distinct from either
/// conforming type's directory, forcing the dual-write path.
protocol PolymorphicFetchDocument: Polymorphable {
    var id: String { get }
    var title: String { get }
}

extension PolymorphicFetchDocument {
    public static var polymorphableType: String { "PolymorphicFetchDocument" }

    public static var polymorphicDirectoryPathComponents: [any DirectoryPathElement] {
        [Path("polymorphic_fetch_tests_shared")]
    }

    public static var polymorphicIndexDescriptors: [IndexDescriptor] {
        [
            IndexDescriptor(
                name: "PolymorphicFetchDocument_title",
                keyPaths: [\Self.title],
                kind: ScalarIndexKind<Self>(fields: [\Self.title])
            ),
            IndexDescriptor(
                name: "PolymorphicFetchDocument_id",
                keyPaths: [\Self.id],
                kind: ScalarIndexKind<Self>(fields: [\Self.id])
            ),
            IndexDescriptor(
                name: "PolymorphicFetchDocument_title_fulltext",
                keyPaths: [\Self.title],
                kind: FullTextIndexKind<Self>(fields: [\Self.title], tokenizer: .simple)
            ),
        ]
    }
}

@Persistable
struct PolymorphicFetchArticle: PolymorphicFetchDocument {
    #Directory<PolymorphicFetchArticle>("polymorphic_fetch_tests_articles")
    var id: String = ULID().ulidString
    var title: String
    var body: String
}

@Persistable
struct PolymorphicFetchReport: PolymorphicFetchDocument {
    #Directory<PolymorphicFetchReport>("polymorphic_fetch_tests_reports")
    var id: String = ULID().ulidString
    var title: String
    var pageCount: Int
}

/// Round-trip tests for polymorphic fetch/save/delete APIs.
///
/// **Why this file exists**
/// An earlier implementation of `fetchPolymorphic` / `savePolymorphic` /
/// `deletePolymorphic` read keys through a *nested* tuple subspace
/// (`itemSubspace.subspace(Tuple([typeCode]))`), while the dual-write path in
/// `processDualWrites` wrote keys using a *flat* tuple
/// (`itemSubspace.pack(Tuple([typeCode, id]))`). The two encodings disagree
/// because `Subspace.subspace(_ elements: any TupleElement...)` wraps each
/// `Tuple` argument with the `0x05` nested marker. Reads silently returned
/// zero results even though writes succeeded.
///
/// The regression was not caught by existing tests because no test exercised
/// the end-to-end round trip through the flat-tuple item subspace:
/// - `PermutedReadBridge` / `VectorReadBridge` use `fetchPolymorphicItems`
///   (a different code path that reconstructs keys from record annotations).
/// - `CanonicalQueryRPC` tests use `scanPolymorphicItems`, which scans the
///   whole item subspace without reconstructing a per-type subspace.
///
/// The tests below insert via the dual-write path
/// (`context.insert(_:)` + `context.save()`) and then read back through
/// every polymorphic API surface, so any future divergence between the
/// write and read key layouts will fail here first.
@Suite("Polymorphic Fetch Tests", .serialized, .heartbeat)
struct PolymorphicFetchTests {

    // MARK: - Helper Methods

    private func setupContainer() async throws -> DBContainer {
        try await FDBTestEnvironment.shared.ensureInitialized()
        let database = try await FDBTestSetup.shared.makeEngine()

        let schema = Schema(
            [PolymorphicFetchArticle.self, PolymorphicFetchReport.self],
            version: Schema.Version(1, 0, 0)
        )

        return try await DBContainer(
            testing: schema,
            configuration: .init(backend: .custom(database)),
            security: .disabled,
        )
    }

    private func cleanup(container: DBContainer) async throws {
        for path in [
            ["polymorphic_fetch_tests_articles"],
            ["polymorphic_fetch_tests_reports"],
            ["polymorphic_fetch_tests_shared"],
        ] {
            if try await container.engine.directoryService.exists(path: path) {
                try await container.engine.directoryService.remove(path: path)
            }
        }
        try await container.ensureIndexesReady()
    }

    private func countPolymorphicIndexEntries(
        container: DBContainer,
        indexName: String,
        valuePrefix: String? = nil
    ) async throws -> Int {
        let group = try container.polymorphicGroup(identifier: PolymorphicFetchArticle.polymorphableType)
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

    // MARK: - Dual-Write Round-Trip (regression: nested vs flat tuple encoding)

    @Test("fetchPolymorphic returns items written via dual-write (scan)")
    func fetchPolymorphicScanAfterDualWrite() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)

        let context = container.newContext()

        let article = PolymorphicFetchArticle(title: "Hello", body: "World")
        let report = PolymorphicFetchReport(title: "Quarterly", pageCount: 42)

        context.insert(article)
        context.insert(report)
        try await context.save()

        let items = try await context.fetchPolymorphic(PolymorphicFetchArticle.self)

        #expect(items.count == 2)

        let articles = items.compactMap { $0 as? PolymorphicFetchArticle }
        let reports = items.compactMap { $0 as? PolymorphicFetchReport }
        #expect(articles.count == 1)
        #expect(reports.count == 1)
        #expect(articles.first?.title == "Hello")
        #expect(reports.first?.pageCount == 42)
    }

    @Test("fetchPolymorphic(id:) retrieves by ID across conforming types")
    func fetchPolymorphicByIDAcrossTypes() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)

        let context = container.newContext()

        let article = PolymorphicFetchArticle(title: "Headline", body: "Body text")
        let report = PolymorphicFetchReport(title: "Audit", pageCount: 7)

        context.insert(article)
        context.insert(report)
        try await context.save()

        let fetchedArticle = try await context.fetchPolymorphic(PolymorphicFetchArticle.self, id: article.id)
        let fetchedReport = try await context.fetchPolymorphic(PolymorphicFetchArticle.self, id: report.id)
        let missing = try await context.fetchPolymorphic(PolymorphicFetchArticle.self, id: "does-not-exist")

        #expect((fetchedArticle as? PolymorphicFetchArticle)?.title == "Headline")
        #expect((fetchedReport as? PolymorphicFetchReport)?.pageCount == 7)
        #expect(missing == nil)
    }

    // MARK: - Multi-Item Scan

    @Test("fetchPolymorphic scans all items of each conforming type")
    func fetchPolymorphicScansAllItems() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)

        let context = container.newContext()

        for i in 1...3 {
            context.insert(PolymorphicFetchArticle(title: "A\(i)", body: "content \(i)"))
        }
        for i in 1...2 {
            context.insert(PolymorphicFetchReport(title: "R\(i)", pageCount: i * 10))
        }
        try await context.save()

        let items = try await context.fetchPolymorphic(PolymorphicFetchArticle.self)

        #expect(items.count == 5)
        #expect(items.compactMap { $0 as? PolymorphicFetchArticle }.count == 3)
        #expect(items.compactMap { $0 as? PolymorphicFetchReport }.count == 2)
    }

    @Test("findPolymorphic decodes mixed rows with ordering and continuation")
    func findPolymorphicDecodesMixedRowsWithContinuation() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)

        let context = container.newContext()

        let gamma = PolymorphicFetchArticle(title: "Gamma", body: "third")
        let alpha = PolymorphicFetchReport(title: "Alpha", pageCount: 1)
        let beta = PolymorphicFetchArticle(title: "Beta", body: "second")

        context.insert(gamma)
        context.insert(alpha)
        context.insert(beta)
        try await context.save()

        let firstPage = try await context.findPolymorphic(PolymorphicFetchArticle.self)
            .orderBy(\.title)
            .pageSize(2)
            .executePage()

        #expect(firstPage.results.map { $0.fields["title"]?.stringValue } == ["Alpha", "Beta"])
        #expect(firstPage.results.first?.item(as: PolymorphicFetchReport.self)?.id == alpha.id)
        #expect(firstPage.results.dropFirst().first?.item(as: PolymorphicFetchArticle.self)?.id == beta.id)
        #expect(firstPage.continuation != nil)

        let secondPage = try await context.findPolymorphic(PolymorphicFetchArticle.self)
            .orderBy(\.title)
            .pageSize(2)
            .continuing(from: firstPage.continuation)
            .executePage()

        #expect(secondPage.results.map { $0.fields["title"]?.stringValue } == ["Gamma"])
        #expect(secondPage.results.first?.item(as: PolymorphicFetchArticle.self)?.id == gamma.id)
        #expect(secondPage.continuation == nil)
    }

    // MARK: - Shared Index E2E

    @Test("dual-write maintains shared polymorphic scalar indexes")
    func dualWriteMaintainsSharedPolymorphicScalarIndexes() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)

        let context = container.newContext()

        let article = PolymorphicFetchArticle(title: "Indexed Article", body: "Body")
        let report = PolymorphicFetchReport(title: "Indexed Report", pageCount: 4)

        context.insert(article)
        context.insert(report)
        try await context.save()

        #expect(try await countPolymorphicIndexEntries(
            container: container,
            indexName: "PolymorphicFetchDocument_title"
        ) == 2)
        #expect(try await countPolymorphicIndexEntries(
            container: container,
            indexName: "PolymorphicFetchDocument_id"
        ) == 2)
        #expect(try await countPolymorphicIndexEntries(
            container: container,
            indexName: "PolymorphicFetchDocument_title",
            valuePrefix: "Indexed Article"
        ) == 1)
        #expect(try await countPolymorphicIndexEntries(
            container: container,
            indexName: "PolymorphicFetchDocument_title",
            valuePrefix: "Indexed Report"
        ) == 1)
    }

    @Test("savePolymorphic update and delete maintain shared scalar indexes")
    func savePolymorphicUpdateAndDeleteMaintainSharedScalarIndexes() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)

        let context = container.newContext()

        var article = PolymorphicFetchArticle(title: "Direct Indexed", body: "Saved directly")
        try await context.savePolymorphic(article, as: PolymorphicFetchArticle.self)

        #expect(try await countPolymorphicIndexEntries(
            container: container,
            indexName: "PolymorphicFetchDocument_title"
        ) == 1)
        #expect(try await countPolymorphicIndexEntries(
            container: container,
            indexName: "PolymorphicFetchDocument_title",
            valuePrefix: "Direct Indexed"
        ) == 1)

        article.title = "Direct Indexed Updated"
        try await context.savePolymorphic(article, as: PolymorphicFetchArticle.self)

        #expect(try await countPolymorphicIndexEntries(
            container: container,
            indexName: "PolymorphicFetchDocument_title"
        ) == 1)
        #expect(try await countPolymorphicIndexEntries(
            container: container,
            indexName: "PolymorphicFetchDocument_title",
            valuePrefix: "Direct Indexed"
        ) == 0)
        #expect(try await countPolymorphicIndexEntries(
            container: container,
            indexName: "PolymorphicFetchDocument_title",
            valuePrefix: "Direct Indexed Updated"
        ) == 1)

        try await context.deletePolymorphic(
            PolymorphicFetchArticle.self,
            id: article.id,
            as: PolymorphicFetchArticle.self
        )

        #expect(try await countPolymorphicIndexEntries(
            container: container,
            indexName: "PolymorphicFetchDocument_title"
        ) == 0)
        #expect(try await countPolymorphicIndexEntries(
            container: container,
            indexName: "PolymorphicFetchDocument_id"
        ) == 0)
    }

    @Test("polymorphic full-text query resolves shared descriptor and maintains indexes")
    func polymorphicFullTextQueryResolvesSharedDescriptorAndMaintainsIndexes() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)

        let context = container.newContext()

        let article = PolymorphicFetchArticle(title: "Needle Article", body: "Body")
        var report = PolymorphicFetchReport(title: "Needle Report", pageCount: 4)
        let unrelated = PolymorphicFetchReport(title: "Haystack", pageCount: 8)

        context.insert(article)
        context.insert(report)
        context.insert(unrelated)
        try await context.save()

        let initial = try await context.findPolymorphic(PolymorphicFetchArticle.self)
            .fullText(\.title)
            .term("needle")
            .execute()
        let initialIDs = Set(initial.compactMap { result -> String? in
            if let article = result.item(as: PolymorphicFetchArticle.self) {
                return article.id
            }
            if let report = result.item(as: PolymorphicFetchReport.self) {
                return report.id
            }
            return nil
        })

        #expect(initialIDs == Set([article.id, report.id]))

        report.title = "Beacon Report"
        try await context.savePolymorphic(report, as: PolymorphicFetchReport.self)

        let afterUpdateNeedle = try await context.findPolymorphic(PolymorphicFetchArticle.self)
            .fullText(\.title)
            .term("needle")
            .execute()
        let afterUpdateBeacon = try await context.findPolymorphic(PolymorphicFetchArticle.self)
            .fullText(\.title)
            .term("beacon")
            .execute()

        #expect(afterUpdateNeedle.count == 1)
        #expect(afterUpdateNeedle.first?.item(as: PolymorphicFetchArticle.self)?.id == article.id)
        #expect(afterUpdateBeacon.count == 1)
        #expect(afterUpdateBeacon.first?.item(as: PolymorphicFetchReport.self)?.id == report.id)

        try await context.deletePolymorphic(
            PolymorphicFetchArticle.self,
            id: article.id,
            as: PolymorphicFetchArticle.self
        )

        let afterDeleteNeedle = try await context.findPolymorphic(PolymorphicFetchArticle.self)
            .fullText(\.title)
            .term("needle")
            .execute()

        #expect(afterDeleteNeedle.isEmpty)
    }

    // MARK: - savePolymorphic + fetchPolymorphic Consistency

    @Test("savePolymorphic writes are visible to fetchPolymorphic")
    func savePolymorphicIsVisibleToFetchPolymorphic() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)

        let context = container.newContext()

        let article = PolymorphicFetchArticle(title: "Direct", body: "Saved via savePolymorphic")
        try await context.savePolymorphic(article, as: PolymorphicFetchArticle.self)

        let scanned = try await context.fetchPolymorphic(PolymorphicFetchArticle.self)
        let fetchedByID = try await context.fetchPolymorphic(PolymorphicFetchArticle.self, id: article.id)

        #expect(scanned.count == 1)
        #expect((scanned.first as? PolymorphicFetchArticle)?.title == "Direct")
        #expect((fetchedByID as? PolymorphicFetchArticle)?.id == article.id)
    }

    // MARK: - deletePolymorphic

    @Test("deletePolymorphic removes the item from the shared directory")
    func deletePolymorphicRemovesItem() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)

        let context = container.newContext()

        let article = PolymorphicFetchArticle(title: "Doomed", body: "Delete me")
        try await context.savePolymorphic(article, as: PolymorphicFetchArticle.self)

        let beforeDelete = try await context.fetchPolymorphic(PolymorphicFetchArticle.self, id: article.id)
        #expect(beforeDelete != nil)

        try await context.deletePolymorphic(PolymorphicFetchArticle.self, id: article.id, as: PolymorphicFetchArticle.self)

        let afterDelete = try await context.fetchPolymorphic(PolymorphicFetchArticle.self, id: article.id)
        let remaining = try await context.fetchPolymorphic(PolymorphicFetchArticle.self)

        #expect(afterDelete == nil)
        #expect(remaining.isEmpty)
    }

    @Test("clearAll removes only the matching concrete type from the shared directory")
    func clearAllRemovesConcreteTypeFromSharedDirectory() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)

        let context = container.newContext()

        let article = PolymorphicFetchArticle(title: "Keep reports", body: "Remove article")
        let report = PolymorphicFetchReport(title: "Survivor", pageCount: 9)

        context.insert(article)
        context.insert(report)
        try await context.save()

        try await context.clearAll(PolymorphicFetchArticle.self)

        let remaining = try await context.fetchPolymorphic(PolymorphicFetchArticle.self)
        let clearedArticle = try await context.fetchPolymorphic(PolymorphicFetchArticle.self, id: article.id)
        let survivingReport = try await context.fetchPolymorphic(PolymorphicFetchArticle.self, id: report.id)

        #expect(remaining.count == 1)
        #expect(remaining.first is PolymorphicFetchReport)
        #expect(clearedArticle == nil)
        #expect((survivingReport as? PolymorphicFetchReport)?.pageCount == 9)
    }
}
#endif
