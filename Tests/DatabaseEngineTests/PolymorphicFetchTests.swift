#if FOUNDATION_DB
import Testing
import TestHeartbeat
import Foundation
import StorageKit
import FDBStorage
import FoundationDB
import TestSupport
@testable import DatabaseEngine
@testable import Core

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
