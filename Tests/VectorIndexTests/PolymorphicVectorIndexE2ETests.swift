#if FOUNDATION_DB
import Testing
import Foundation
import StorageKit
import FDBStorage
import DatabaseKit
import DatabaseTypes
import TestSupport
@testable import DatabaseEngine
@testable import VectorIndex

// MARK: - Test Types

@Polymorphable(identifier: "PolymorphicVectorE2EDocument")
@PolymorphicDirectory("polymorphic_vector_e2e_shared")
@PolymorphicIndex(
    .vector(dimensions: 3, metric: .cosine),
    embedding: "embedding",
    name: "PolymorphicVectorE2EDocument_embedding"
)
protocol PolymorphicVectorE2EDocument:
    Polymorphable<PolymorphicVectorE2EDocumentPolymorphicGroup>
{
    var id: String { get }
    var title: String { get }
    var embedding: Vector { get }
}

@Persistable
struct PolymorphicVectorArticle: PolymorphicVectorE2EDocument {
    #Directory<PolymorphicVectorArticle>("polymorphic_vector_e2e_articles")

    var id: String = UUID().uuidString
    var title: String
    var embedding: Vector
    var body: String
}

@Persistable
struct PolymorphicVectorReport: PolymorphicVectorE2EDocument {
    #Directory<PolymorphicVectorReport>("polymorphic_vector_e2e_reports")

    var id: String = UUID().uuidString
    var title: String
    var embedding: Vector
    var pageCount: Int64
}

@Polymorphable(identifier: "PolymorphicVectorNoIndexDocument")
@PolymorphicDirectory("polymorphic_vector_no_index_shared")
protocol PolymorphicVectorNoIndexDocument:
    Polymorphable<PolymorphicVectorNoIndexDocumentPolymorphicGroup>
{
    var id: String { get }
    var title: String { get }
    var embedding: Vector { get }
}

@Persistable
struct PolymorphicVectorNoIndexArticle: PolymorphicVectorNoIndexDocument {
    #Directory<PolymorphicVectorNoIndexArticle>("polymorphic_vector_no_index_articles")

    var id: String = UUID().uuidString
    var title: String
    var embedding: Vector
    var body: String
}

@Polymorphable(identifier: "PolymorphicOptionalVectorE2EDocument")
@PolymorphicDirectory("polymorphic_optional_vector_e2e_shared")
@PolymorphicIndex(
    .vector(dimensions: 3, metric: .cosine),
    embedding: "embedding",
    name: "PolymorphicOptionalVectorE2EDocument_embedding"
)
protocol PolymorphicOptionalVectorE2EDocument:
    Polymorphable<PolymorphicOptionalVectorE2EDocumentPolymorphicGroup>
{
    var id: String { get }
    var title: String { get }
    var embedding: Vector? { get }
}

@Persistable
struct PolymorphicOptionalVectorArticle: PolymorphicOptionalVectorE2EDocument {
    #Directory<PolymorphicOptionalVectorArticle>("polymorphic_optional_vector_e2e_articles")

    var id: String = UUID().uuidString
    var title: String
    var embedding: Vector?
    var body: String
}

@Persistable
struct PolymorphicOptionalVectorReport: PolymorphicOptionalVectorE2EDocument {
    #Directory<PolymorphicOptionalVectorReport>("polymorphic_optional_vector_e2e_reports")

    var id: String = UUID().uuidString
    var title: String
    var embedding: Vector?
    var pageCount: Int64
}

@Suite("Polymorphic Vector Index E2E Tests", .tags(.fdb), .serialized, .heartbeat)
struct PolymorphicVectorIndexE2ETests {
    private let indexName = "PolymorphicVectorE2EDocument_embedding"
    private let optionalIndexName = "PolymorphicOptionalVectorE2EDocument_embedding"

    private func setupContainer() async throws -> DBContainer {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let schema = try Schema(
            entities: [
                try PolymorphicVectorArticle.schemaEntity,
                try PolymorphicVectorReport.schemaEntity,
            ],
            version: Schema.Version(1, 0, 0)
        )

        return try await DBContainer.open(
            testing: schema,
            configuration: .init(backend: .custom(database)),
            runtimeConfiguration: try vectorRuntimeConfiguration(),
            security: .disabled
        )
    }

    private func setupOptionalContainer() async throws -> DBContainer {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let schema = try Schema(
            entities: [
                try PolymorphicOptionalVectorArticle.schemaEntity,
                try PolymorphicOptionalVectorReport.schemaEntity,
            ],
            version: Schema.Version(1, 0, 0)
        )

        return try await DBContainer.open(
            testing: schema,
            configuration: .init(backend: .custom(database)),
            runtimeConfiguration: try vectorRuntimeConfiguration(),
            security: .disabled
        )
    }

    private func setupNoIndexContainer() async throws -> DBContainer {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let schema = try Schema(
            entities: [
                try PolymorphicVectorNoIndexArticle.schemaEntity
            ],
            version: Schema.Version(1, 0, 0)
        )

        return try await DBContainer.open(
            testing: schema,
            configuration: .init(backend: .custom(database)),
            runtimeConfiguration: try vectorRuntimeConfiguration(),
            security: .disabled
        )
    }

    private func vectorRuntimeConfiguration() throws -> DatabaseRuntimeConfiguration {
        try DatabaseRuntimeConfiguration(
            indexMaintainerProviders: [
                VectorIndexMaintainerProvider()
            ],
            indexReadExecutors: [VectorReadExecutors.indexExecutor],
            polymorphicIndexReadExecutors: [VectorReadExecutors.polymorphicIndexExecutor]
        )
    }

    private func cleanup(container: DBContainer) async throws {
        for path in [
            ["polymorphic_vector_e2e_articles"],
            ["polymorphic_vector_e2e_reports"],
            ["polymorphic_vector_e2e_shared"],
            ["polymorphic_vector_no_index_articles"],
            ["polymorphic_vector_no_index_shared"],
            ["polymorphic_optional_vector_e2e_articles"],
            ["polymorphic_optional_vector_e2e_reports"],
            ["polymorphic_optional_vector_e2e_shared"],
        ] {
            if try await container.engine.directoryExists(path: path) {
                try await container.engine.removeDirectory(path: path)
            }
        }
        try await container.ensureIndexesReady()
    }

    private func countVectorIndexEntries(container: DBContainer) async throws -> Int {
        let group = try container.polymorphicGroup(identifier: PolymorphicVectorArticle.polymorphableType)
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

    private func countOptionalVectorIndexEntries(container: DBContainer) async throws -> Int {
        let group = try container.polymorphicGroup(identifier: PolymorphicOptionalVectorArticle.polymorphableType)
        let groupSubspace = try await container.resolvePolymorphicDirectory(for: group.identifier)
        let indexSubspace = groupSubspace
            .subspace(SubspaceKey.indexes)
            .subspace(optionalIndexName)

        return try await container.engine.withTransaction { transaction -> Int in
            let (begin, end) = indexSubspace.range()
            var count = 0
            for try await _ in transaction.getRange(begin: begin, end: end, snapshot: true) {
                count += 1
            }
            return count
        }
    }

    private func resultID(_ result: PolymorphicQueryResult) -> String? {
        if let article = result.item(as: PolymorphicVectorArticle.self) {
            return article.id
        }
        if let report = result.item(as: PolymorphicVectorReport.self) {
            return report.id
        }
        return nil
    }

    private func optionalResultID(_ result: PolymorphicQueryResult) -> String? {
        if let article = result.item(as: PolymorphicOptionalVectorArticle.self) {
            return article.id
        }
        if let report = result.item(as: PolymorphicOptionalVectorReport.self) {
            return report.id
        }
        return nil
    }

    @Test("Schema keeps member-specific polymorphic vector descriptors")
    func schemaKeepsMemberSpecificVectorDescriptors() throws {
        let schema = try Schema(
            entities: [
                try PolymorphicVectorArticle.schemaEntity,
                try PolymorphicVectorReport.schemaEntity,
            ],
            version: Schema.Version(1, 0, 0)
        )

        let articleDescriptor = try #require(
            schema.polymorphicIndexDescriptors(
                identifier: PolymorphicVectorArticle.polymorphableType,
                memberType: PolymorphicVectorArticle.self
            ).first { $0.name == indexName }
        )
        let reportDescriptor = try #require(
            schema.polymorphicIndexDescriptors(
                identifier: PolymorphicVectorReport.polymorphableType,
                memberType: PolymorphicVectorReport.self
            ).first { $0.name == indexName }
        )

        let articleSpecification = try VectorIndexSpecification(
            articleDescriptor.kind
        )
        let reportSpecification = try VectorIndexSpecification(
            reportDescriptor.kind
        )
        #expect(articleSpecification.metadata.fieldNames == ["embedding"])
        #expect(reportSpecification.metadata.fieldNames == ["embedding"])
        #expect(articleSpecification.dimensions == 3)
        #expect(reportSpecification.dimensions == 3)
    }

    @Test("Polymorphic vector query requires a query vector")
    func polymorphicVectorQueryRequiresQueryVector() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        do {
            _ = try await context.findPolymorphic(PolymorphicVectorArticle.self)
                .vector(PolymorphicVectorArticle.fields.embedding, dimensions: 3)
                .executePage()
            Issue.record("Expected VectorQueryError.noQueryVector")
        } catch VectorQueryError.noQueryVector {
        } catch {
            Issue.record("Expected VectorQueryError.noQueryVector, got \(error)")
        }
    }

    @Test("Polymorphic vector query rejects mismatched query dimensions")
    func polymorphicVectorQueryRejectsMismatchedDimensions() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        do {
            _ = try await context.findPolymorphic(PolymorphicVectorArticle.self)
                .vector(PolymorphicVectorArticle.fields.embedding, dimensions: 3)
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

    @Test("Polymorphic vector query reports missing shared descriptor")
    func polymorphicVectorQueryReportsMissingSharedDescriptor() async throws {
        let container = try await setupNoIndexContainer()
        try await cleanup(container: container)
        let context = container.newContext()

        do {
            _ = try await context.findPolymorphic(PolymorphicVectorNoIndexArticle.self)
                .vector(
                    PolymorphicVectorNoIndexArticle.fields.embedding,
                    dimensions: 3
                )
                .query([1.0, 0.0, 0.0], k: 1)
                .executePage()
            Issue.record("Expected PolymorphicVectorQueryError.indexNotFound")
        } catch PolymorphicVectorQueryError.indexNotFound(let groupIdentifier, let fieldName) {
            #expect(groupIdentifier == PolymorphicVectorNoIndexArticle.polymorphableType)
            #expect(fieldName == "embedding")
        } catch {
            Issue.record("Expected PolymorphicVectorQueryError.indexNotFound, got \(error)")
        }
    }

    @Test("Polymorphic optional vector KeyPath overload queries shared index end-to-end")
    func polymorphicOptionalVectorKeyPathOverloadQueriesSharedIndexEndToEnd() async throws {
        let container = try await setupOptionalContainer()
        try await cleanup(container: container)
        let context = container.newContext()
        let article = PolymorphicOptionalVectorArticle(
            title: "Optional Anchor",
            embedding: try Vector(float32: [1.0, 0.0, 0.0]),
            body: "Article body"
        )
        let report = PolymorphicOptionalVectorReport(
            title: "Optional Near",
            embedding: try Vector(float32: [0.95, 0.05, 0.0]),
            pageCount: 3
        )

        try context.insert(article)
        try context.insert(report)
        try await context.save()

        #expect(try await countOptionalVectorIndexEntries(container: container) == 2)

        let first = try await context.findPolymorphic(PolymorphicOptionalVectorArticle.self)
            .vector(
                PolymorphicOptionalVectorArticle.fields.embedding,
                dimensions: 3
            )
            .query([1.0, 0.0, 0.0], k: 1)
            .first()

        #expect(first?.item(as: PolymorphicOptionalVectorArticle.self)?.id == article.id)

        let results = try await context.findPolymorphic(PolymorphicOptionalVectorReport.self)
            .vector(
                PolymorphicOptionalVectorReport.fields.embedding,
                dimensions: 3
            )
            .query([1.0, 0.0, 0.0], k: 2)
            .execute()
        let resultIDs = Set(results.compactMap(optionalResultID))

        #expect(resultIDs == Set([article.id, report.id]))
    }

    @Test("Polymorphic vector index is maintained and queried end-to-end")
    func polymorphicVectorIndexIsMaintainedAndQueriedEndToEnd() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        let context = container.newContext()

        let article = PolymorphicVectorArticle(
            title: "Anchor",
            embedding: try Vector(float32: [1.0, 0.0, 0.0]),
            body: "Article body"
        )
        var report = PolymorphicVectorReport(
            title: "Near",
            embedding: try Vector(float32: [0.95, 0.05, 0.0]),
            pageCount: 3
        )
        let farReport = PolymorphicVectorReport(
            title: "Far",
            embedding: try Vector(float32: [0.0, 1.0, 0.0]),
            pageCount: 9
        )

        try context.insert(article)
        try context.insert(report)
        try context.insert(farReport)
        try await context.save()

        #expect(try await countVectorIndexEntries(container: container) == 3)

        let firstPage = try await context.findPolymorphic(PolymorphicVectorArticle.self)
            .vector(PolymorphicVectorArticle.fields.embedding, dimensions: 3)
            .query([1.0, 0.0, 0.0], k: 2)
            .metric(.cosine)
            .executePage()

        #expect(firstPage.results.count == 2)
        #expect(firstPage.results.first?.item(as: PolymorphicVectorArticle.self)?.id == article.id)
        #expect(firstPage.results.dropFirst().first?.item(as: PolymorphicVectorReport.self)?.id == report.id)

        let reportStartedPage = try await context.findPolymorphic(PolymorphicVectorReport.self)
            .vector(PolymorphicVectorReport.fields.embedding, dimensions: 3)
            .query([1.0, 0.0, 0.0], k: 2)
            .metric(.cosine)
            .executePage()
        let reportStartedIDs = Set(reportStartedPage.results.compactMap(resultID))

        #expect(reportStartedIDs == Set([article.id, report.id]))

        report.embedding = try Vector(float32: [1.0, 0.0, 0.0])
        try context.upsert(report)
        try await context.save()

        #expect(try await countVectorIndexEntries(container: container) == 3)

        let updatedPage = try await context.findPolymorphic(PolymorphicVectorArticle.self)
            .vector(PolymorphicVectorArticle.fields.embedding, dimensions: 3)
            .query([1.0, 0.0, 0.0], k: 2)
            .metric(.cosine)
            .executePage()
        let updatedIDs = Set(updatedPage.results.compactMap(resultID))

        #expect(updatedIDs == Set([article.id, report.id]))

        try context.delete(article)
        try await context.save()

        #expect(try await countVectorIndexEntries(container: container) == 2)

        let finalPage = try await context.findPolymorphic(PolymorphicVectorArticle.self)
            .vector(PolymorphicVectorArticle.fields.embedding, dimensions: 3)
            .query([1.0, 0.0, 0.0], k: 1)
            .metric(.cosine)
            .executePage()

        #expect(finalPage.results.count == 1)
        #expect(finalPage.results.first?.item(as: PolymorphicVectorReport.self)?.id == report.id)
    }
}
#endif
