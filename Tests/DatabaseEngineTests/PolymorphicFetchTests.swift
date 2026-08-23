#if !os(WASI)
#if FOUNDATION_DB
import Testing
import TestHeartbeat
import Foundation
import StorageKit
import FDBStorage
import DatabaseKit
import TestSupport
@testable import DatabaseEngine
import DatabaseRuntime
@testable import DatabaseKit
@testable import FullTextIndex

/// Polymorphic protocol with a shared projection directory distinct from each
/// concrete type's primary directory.
@Polymorphable
@PolymorphicDirectory("polymorphic_fetch_tests_shared")
@PolymorphicIndex(
    .ordered(
        name: "PolymorphicFetchDocument_title",
        keys: [.ascending("title")]
    ))
@PolymorphicIndex(
    .ordered(
        name: "PolymorphicFetchDocument_id",
        keys: [.ascending("id")]
    ))
@PolymorphicIndex(
    .text(
        name: "PolymorphicFetchDocument_title_fulltext",
        fields: ["title"],
        mode: .fullText(tokenizer: .simple)
    ))
protocol PolymorphicFetchDocument:
    Polymorphable<PolymorphicFetchDocumentPolymorphicGroup>
{
    var id: String { get }
    var title: String { get }
}

@Persistable
struct PolymorphicFetchArticle: PolymorphicFetchDocument {
    #Directory<PolymorphicFetchArticle>("polymorphic_fetch_tests_articles")
    var id: String = UUID().uuidString
    var title: String
    var body: String
}

@Persistable
struct PolymorphicFetchReport: PolymorphicFetchDocument {
    #Directory<PolymorphicFetchReport>("polymorphic_fetch_tests_reports")
    var id: String = UUID().uuidString
    var title: String
    var pageCount: Int64
}

/// Round-trip tests for polymorphic reads and canonical staged mutations.
///
/// **Why this file exists**
/// An earlier polymorphic read path constructed keys through a *nested* tuple subspace
/// (`itemSubspace.subspace(Tuple([typeCode]))`), while projection maintenance
/// wrote keys using a *flat* tuple
/// (`itemSubspace.pack(Tuple([typeCode, id]))`). The two encodings disagree
/// because `Subspace.subspace(_ elements: any TupleElement...)` wraps each
/// `Tuple` argument with the `0x05` nested marker. Reads silently returned
/// zero results even though writes succeeded.
///
/// The regression was not caught by existing tests because no test exercised
/// the end-to-end round trip through the flat-tuple item subspace:
/// - Index-specific readers use `fetchPolymorphicItems` (a different code path
///   that reconstructs keys from entity annotations).
/// - `CanonicalQueryRPC` tests use `scanPolymorphicItems`, which scans the
///   whole item subspace without reconstructing a per-type subspace.
///
/// The tests below insert through canonical transaction-owned projection maintenance
/// (`try context.insert(_:)` + `context.save()`) and then read back through
/// every polymorphic API surface, so any future divergence between the
/// write and read key layouts will fail here first.
@Suite("Polymorphic Fetch Tests", .foundationDBScenario, .serialized, .heartbeat)
struct PolymorphicFetchTests {

    // MARK: - Helper Methods

    private func setupContainer() async throws -> DBContainer {
        try await FoundationDBScenarioEnvironment.shared.ensureInitialized()
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()

        let schema = try Schema(
            entities: [
                try PolymorphicFetchArticle.schemaEntity,
                try PolymorphicFetchReport.schemaEntity,
            ],
            version: Schema.Version(1, 0, 0)
        )

        return try await DBContainer.open(
            testing: schema,
            configuration: .testing(storageEngine: database),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(PolymorphicFetchArticle.self), try DatabaseFrameworkRuntime.entity(PolymorphicFetchReport.self),
                ]),
            security: .testingDisabled,
        )
    }

    private func cleanup(container: DBContainer) async throws {
        try await container.resetTestBaseData()
    }

    private func countPolymorphicIndexEntries(
        container: DBContainer,
        indexName: String,
        valuePrefix: String? = nil
    ) async throws -> Int {
        let group = try container.polymorphicGroup(identifier: PolymorphicFetchArticle.polymorphableType)
        let groupSubspace = try await container.testBasePolymorphicDirectory(for: group.identifier)
        var indexSubspace = try IndexLifecycleStore(
            container: container,
            subspace: groupSubspace
        ).indexSubspace(for: indexName)

        if let valuePrefix {
            let value = try FieldValue.string(valuePrefix).toTupleElement()
            indexSubspace = Subspace(
                prefix: indexSubspace.prefix.appending(
                    contentsOf: Tuple(value).pack()
                )
            )
        }
        let resolvedIndexSubspace = indexSubspace

        return try await container.engine.withTransaction { transaction -> Int in
            let (begin, end) = resolvedIndexSubspace.range()
            return try await transaction.collectRange(
                begin: begin,
                end: end,
                snapshot: true
            ).count
        }
    }

    // MARK: - Projection Round-Trip

    @Test("Empty polymorphic reads do not create a projection namespace")
    func emptyReadsDoNotCreateProjectionNamespace() async throws {
        let container = try await setupContainer()
        let path = ["polymorphic_fetch_tests_shared"]
        if try await container.engine.namespaceExists(path: path) {
            try await container.engine.removeNamespace(path: path)
        }
        #expect(try await container.engine.namespaceExists(path: path) == false)

        let context = container.testBaseContext()
        let scanned = try await context.fetchPolymorphic(
            PolymorphicFetchArticle.self
        )
        let fetched = try await context.fetchPolymorphic(
            PolymorphicFetchArticle.self,
            id: "missing"
        )

        #expect(scanned.isEmpty)
        #expect(fetched == nil)
        #expect(try await container.engine.namespaceExists(path: path) == false)
    }

    @Test("Caller-owned polymorphic fetch observes projection writes in the same transaction")
    func callerOwnedFetchObservesProjectionWrite() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        let context = container.testBaseContext()
        let article = PolymorphicFetchArticle(
            title: "Read your writes",
            body: "Same transaction"
        )
        let canonicalModel = try PersistedModel(article)
        let identifier = try article.persistableIdentifierTuple()
        let entity = try #require(
            container.schema.entity(
                named: PolymorphicFetchArticle.persistableType
            )
        )
        let compositeIdentifier = try PolymorphicIdentifierKey.tuple(
            for: entity,
            identifier: identifier
        )
        let group = try container.polymorphicGroup(
            identifier: PolymorphicFetchArticle.polymorphableType
        )

        try await container.withTestBaseTransaction { transaction in
            try await PolymorphicProjectionMaintainer(container: container)
                .update(
                    PersistableWriteResult(
                        canonicalModel: canonicalModel,
                        previousCanonicalModel: nil,
                        encodedValue: try DataAccess.serialize(canonicalModel),
                        identifier: identifier
                    ),
                    transaction: transaction
                )

            let missingIdentifier = try PolymorphicIdentifierKey.tuple(
                for: entity,
                identifier: Tuple("missing")
            )
            let fetched = try await context.fetchPolymorphicItemsPreservingOrder(
                group: group,
                ids: [compositeIdentifier, missingIdentifier],
                transaction: transaction,
                workMeter: ReadExecutionContext(
                    monotonicClock: container.monotonicClock
                ).workMeter
            )
            #expect(fetched.count == 2)
            #expect(
                try fetched[0]?.item
                    .decode(as: PolymorphicFetchArticle.self).title
                    == "Read your writes"
            )
            #expect(fetched[1] == nil)
        }
    }

    @Test("fetchPolymorphic returns transaction-maintained projections")
    func fetchPolymorphicScansMaintainedProjections() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)

        let context = container.testBaseContext()

        let article = PolymorphicFetchArticle(title: "Hello", body: "World")
        let report = PolymorphicFetchReport(title: "Quarterly", pageCount: 42)

        try context.insert(article)
        try context.insert(report)
        try await context.save()

        let items = try await context.fetchPolymorphic(PolymorphicFetchArticle.self)

        #expect(items.count == 2)

        let articles = try items
            .filter { $0.entity == PolymorphicFetchArticle.persistableType }
            .map { try $0.decode(as: PolymorphicFetchArticle.self) }
        let reports = try items
            .filter { $0.entity == PolymorphicFetchReport.persistableType }
            .map { try $0.decode(as: PolymorphicFetchReport.self) }
        #expect(articles.count == 1)
        #expect(reports.count == 1)
        #expect(articles.first?.title == "Hello")
        #expect(reports.first?.pageCount == 42)
    }

    @Test("fetchPolymorphic(id:) retrieves by ID across conforming types")
    func fetchPolymorphicByIDAcrossTypes() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)

        let context = container.testBaseContext()

        let article = PolymorphicFetchArticle(title: "Headline", body: "Body text")
        let report = PolymorphicFetchReport(title: "Audit", pageCount: 7)

        try context.insert(article)
        try context.insert(report)
        try await context.save()

        let fetchedArticle = try await context.fetchPolymorphic(PolymorphicFetchArticle.self, id: article.id)
        let fetchedReport = try await context.fetchPolymorphic(PolymorphicFetchArticle.self, id: report.id)
        let missing = try await context.fetchPolymorphic(PolymorphicFetchArticle.self, id: "does-not-exist")

        #expect(
            try fetchedArticle?.decode(as: PolymorphicFetchArticle.self).title
                == "Headline"
        )
        #expect(
            try fetchedReport?.decode(as: PolymorphicFetchReport.self).pageCount
                == 7
        )
        #expect(missing == nil)
    }

    // MARK: - Multi-Item Scan

    @Test("fetchPolymorphic scans all items of each conforming type")
    func fetchPolymorphicScansAllItems() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)

        let context = container.testBaseContext()

        for i in 1...3 {
            try context.insert(PolymorphicFetchArticle(title: "A\(i)", body: "content \(i)"))
        }
        for i in 1...2 {
            try context.insert(
                PolymorphicFetchReport(
                    title: "R\(i)",
                    pageCount: Int64(i * 10)
                )
            )
        }
        try await context.save()

        let items = try await context.fetchPolymorphic(PolymorphicFetchArticle.self)

        #expect(items.count == 5)
        #expect(
            items.filter {
                $0.entity == PolymorphicFetchArticle.persistableType
            }.count == 3
        )
        #expect(
            items.filter {
                $0.entity == PolymorphicFetchReport.persistableType
            }.count == 2
        )
    }

    @Test("findPolymorphic decodes mixed rows with ordering and continuation")
    func findPolymorphicDecodesMixedRowsWithContinuation() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)

        let context = container.testBaseContext()

        let gamma = PolymorphicFetchArticle(title: "Gamma", body: "third")
        let alpha = PolymorphicFetchReport(title: "Alpha", pageCount: 1)
        let beta = PolymorphicFetchArticle(title: "Beta", body: "second")

        try context.insert(gamma)
        try context.insert(alpha)
        try context.insert(beta)
        try await context.save()

        let firstPage = try await context.findPolymorphic(PolymorphicFetchArticle.self)
            .orderBy(PolymorphicFetchArticle.fields.title)
            .pageSize(2)
            .executePage()

        #expect(firstPage.results.map { $0.fields["title"]?.stringValue } == ["Alpha", "Beta"])
        #expect(try firstPage.results.first?.decodedModel(as: PolymorphicFetchReport.self)?.id == alpha.id)
        #expect(try firstPage.results.dropFirst().first?.decodedModel(as: PolymorphicFetchArticle.self)?
                .id == beta.id)
        #expect(firstPage.continuation != nil)

        let secondPage = try await context.findPolymorphic(PolymorphicFetchArticle.self)
            .orderBy(PolymorphicFetchArticle.fields.title)
            .pageSize(2)
            .continuing(from: firstPage.continuation)
            .executePage()

        #expect(secondPage.results.map { $0.fields["title"]?.stringValue } == ["Gamma"])
        #expect(try secondPage.results.first?.decodedModel(as: PolymorphicFetchArticle.self)?.id == gamma.id)
        #expect(secondPage.continuation == nil)
    }

    // MARK: - Shared Index E2E

    @Test("Projection maintenance updates shared polymorphic scalar indexes")
    func projectionMaintenanceUpdatesSharedScalarIndexes() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)

        let context = container.testBaseContext()

        let article = PolymorphicFetchArticle(title: "Indexed Article", body: "Body")
        let report = PolymorphicFetchReport(title: "Indexed Report", pageCount: 4)

        try context.insert(article)
        try context.insert(report)
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

    @Test("staged update and delete maintain shared scalar indexes")
    func stagedUpdateAndDeleteMaintainSharedScalarIndexes() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)

        let context = container.testBaseContext()

        var article = PolymorphicFetchArticle(title: "Direct Indexed", body: "Saved directly")
        try context.upsert(article)
        try await context.save()

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
        try context.upsert(article)
        try await context.save()

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

        try context.delete(article)
        try await context.save()

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

        let context = container.testBaseContext()

        let article = PolymorphicFetchArticle(title: "Needle Article", body: "Body")
        var report = PolymorphicFetchReport(title: "Needle Report", pageCount: 4)
        let unrelated = PolymorphicFetchReport(title: "Haystack", pageCount: 8)

        try context.insert(article)
        try context.insert(report)
        try context.insert(unrelated)
        try await context.save()

        let initial = try await context.findPolymorphic(PolymorphicFetchArticle.self)
            .fullText(PolymorphicFetchArticle.fields.title)
            .term("needle")
            .execute()
        let initialIDs = try Set(initial.compactMap { result -> String? in
            if let article = try result.decodedModel(as: PolymorphicFetchArticle.self) {
                return article.id
            }
            if let report = try result.decodedModel(as: PolymorphicFetchReport.self) {
                return report.id
            }
            return nil
        })

        #expect(initialIDs == Set([article.id, report.id]))

        report.title = "Beacon Report"
        try context.upsert(report)
        try await context.save()

        let afterUpdateNeedle = try await context.findPolymorphic(PolymorphicFetchArticle.self)
            .fullText(PolymorphicFetchArticle.fields.title)
            .term("needle")
            .execute()
        let afterUpdateBeacon = try await context.findPolymorphic(PolymorphicFetchArticle.self)
            .fullText(PolymorphicFetchArticle.fields.title)
            .term("beacon")
            .execute()

        #expect(afterUpdateNeedle.count == 1)
        #expect(try afterUpdateNeedle.first?.decodedModel(as: PolymorphicFetchArticle.self)?.id == article.id)
        #expect(afterUpdateBeacon.count == 1)
        #expect(try afterUpdateBeacon.first?.decodedModel(as: PolymorphicFetchReport.self)?.id == report.id)

        try context.delete(article)
        try await context.save()

        let afterDeleteNeedle = try await context.findPolymorphic(PolymorphicFetchArticle.self)
            .fullText(PolymorphicFetchArticle.fields.title)
            .term("needle")
            .execute()

        #expect(afterDeleteNeedle.isEmpty)
    }

    // MARK: - Staged Mutation and Polymorphic Read Consistency

    @Test("staged writes are visible to polymorphic fetches")
    func stagedWriteIsVisibleToPolymorphicFetches() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)

        let context = container.testBaseContext()

        let article = PolymorphicFetchArticle(title: "Direct", body: "Saved via staged upsert")
        try context.upsert(article)
        try await context.save()

        let scanned = try await context.fetchPolymorphic(PolymorphicFetchArticle.self)
        let fetchedByID = try await context.fetchPolymorphic(PolymorphicFetchArticle.self, id: article.id)

        #expect(scanned.count == 1)
        #expect(
            try scanned.first?.decode(as: PolymorphicFetchArticle.self).title
                == "Direct"
        )
        #expect(
            try fetchedByID?.decode(as: PolymorphicFetchArticle.self).id
                == article.id
        )
    }

    // MARK: - Staged Delete

    @Test("staged delete removes the item from the shared directory")
    func stagedDeleteRemovesItem() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)

        let context = container.testBaseContext()

        let article = PolymorphicFetchArticle(title: "Doomed", body: "Delete me")
        try context.upsert(article)
        try await context.save()

        let beforeDelete = try await context.fetchPolymorphic(PolymorphicFetchArticle.self, id: article.id)
        #expect(beforeDelete != nil)

        try context.delete(article)
        try await context.save()

        let afterDelete = try await context.fetchPolymorphic(PolymorphicFetchArticle.self, id: article.id)
        let remaining = try await context.fetchPolymorphic(PolymorphicFetchArticle.self)

        #expect(afterDelete == nil)
        #expect(remaining.isEmpty)
    }

    @Test("deleteAll removes only the matching concrete type from the shared directory")
    func deleteAllRemovesConcreteTypeFromSharedDirectory() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)

        let context = container.testBaseContext()

        let article = PolymorphicFetchArticle(title: "Keep reports", body: "Remove article")
        let report = PolymorphicFetchReport(title: "Survivor", pageCount: 9)

        try context.insert(article)
        try context.insert(report)
        try await context.save()

        try await context.deleteAll(PolymorphicFetchArticle.self)
        try await context.save()

        let remaining = try await context.fetchPolymorphic(PolymorphicFetchArticle.self)
        let clearedArticle = try await context.fetchPolymorphic(PolymorphicFetchArticle.self, id: article.id)
        let survivingReport = try await context.fetchPolymorphic(PolymorphicFetchArticle.self, id: report.id)

        #expect(remaining.count == 1)
        #expect(remaining.first?.entity == PolymorphicFetchReport.persistableType)
        #expect(clearedArticle == nil)
        #expect(
            try survivingReport?.decode(as: PolymorphicFetchReport.self).pageCount
                == 9
        )
    }
}
#endif

#endif
