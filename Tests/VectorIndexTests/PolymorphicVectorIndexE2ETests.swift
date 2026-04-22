#if FOUNDATION_DB
import Testing
import Foundation
import StorageKit
import FDBStorage
import Core
import Vector
import TestSupport
@testable import DatabaseEngine
@testable import VectorIndex

// MARK: - Test Types
//
// Polymorphable conformance is declared manually rather than with the
// @Polymorphable macro because the Swift 6.3 frontend currently crashes when a
// protocol body expands freestanding macros such as #Directory.

protocol PolymorphicVectorE2EDocument: Polymorphable {
    var id: String { get }
    var title: String { get }
    var embedding: [Float] { get }
}

extension PolymorphicVectorE2EDocument {
    public static var polymorphableType: String { "PolymorphicVectorE2EDocument" }

    public static var polymorphicDirectoryPathComponents: [any DirectoryPathElement] {
        [Path("polymorphic_vector_e2e_shared")]
    }

    public static var polymorphicIndexDescriptors: [IndexDescriptor] {
        [
            IndexDescriptor(
                name: "PolymorphicVectorE2EDocument_embedding",
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
struct PolymorphicVectorArticle: PolymorphicVectorE2EDocument {
    #Directory<PolymorphicVectorArticle>("polymorphic_vector_e2e_articles")

    var id: String = ULID().ulidString
    var title: String
    var embedding: [Float]
    var body: String
}

@Persistable
struct PolymorphicVectorReport: PolymorphicVectorE2EDocument {
    #Directory<PolymorphicVectorReport>("polymorphic_vector_e2e_reports")

    var id: String = ULID().ulidString
    var title: String
    var embedding: [Float]
    var pageCount: Int
}

protocol PolymorphicVectorNoIndexDocument: Polymorphable {
    var id: String { get }
    var title: String { get }
    var embedding: [Float] { get }
}

extension PolymorphicVectorNoIndexDocument {
    public static var polymorphableType: String { "PolymorphicVectorNoIndexDocument" }

    public static var polymorphicDirectoryPathComponents: [any DirectoryPathElement] {
        [Path("polymorphic_vector_no_index_shared")]
    }
}

@Persistable
struct PolymorphicVectorNoIndexArticle: PolymorphicVectorNoIndexDocument {
    #Directory<PolymorphicVectorNoIndexArticle>("polymorphic_vector_no_index_articles")

    var id: String = ULID().ulidString
    var title: String
    var embedding: [Float]
    var body: String
}

protocol PolymorphicOptionalVectorE2EDocument: Polymorphable {
    var id: String { get }
    var title: String { get }
    var embedding: [Float]? { get }
}

extension PolymorphicOptionalVectorE2EDocument {
    public static var polymorphableType: String { "PolymorphicOptionalVectorE2EDocument" }

    public static var polymorphicDirectoryPathComponents: [any DirectoryPathElement] {
        [Path("polymorphic_optional_vector_e2e_shared")]
    }

    public static var polymorphicIndexDescriptors: [IndexDescriptor] {
        [
            IndexDescriptor(
                name: "PolymorphicOptionalVectorE2EDocument_embedding",
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
struct PolymorphicOptionalVectorArticle: PolymorphicOptionalVectorE2EDocument {
    #Directory<PolymorphicOptionalVectorArticle>("polymorphic_optional_vector_e2e_articles")

    var id: String = ULID().ulidString
    var title: String
    var embedding: [Float]?
    var body: String
}

@Persistable
struct PolymorphicOptionalVectorReport: PolymorphicOptionalVectorE2EDocument {
    #Directory<PolymorphicOptionalVectorReport>("polymorphic_optional_vector_e2e_reports")

    var id: String = ULID().ulidString
    var title: String
    var embedding: [Float]?
    var pageCount: Int
}

@Suite("Polymorphic Vector Index E2E Tests", .tags(.fdb), .serialized, .heartbeat)
struct PolymorphicVectorIndexE2ETests {
    private let indexName = "PolymorphicVectorE2EDocument_embedding"
    private let optionalIndexName = "PolymorphicOptionalVectorE2EDocument_embedding"

    private func setupContainer() async throws -> DBContainer {
        try await FDBTestSetup.shared.initialize()
        let database = try await FDBTestSetup.shared.makeEngine()
        let schema = Schema(
            [PolymorphicVectorArticle.self, PolymorphicVectorReport.self],
            version: Schema.Version(1, 0, 0)
        )

        return try await DBContainer(
            testing: schema,
            configuration: .init(backend: .custom(database)),
            security: .disabled
        )
    }

    private func setupOptionalContainer() async throws -> DBContainer {
        try await FDBTestSetup.shared.initialize()
        let database = try await FDBTestSetup.shared.makeEngine()
        let schema = Schema(
            [PolymorphicOptionalVectorArticle.self, PolymorphicOptionalVectorReport.self],
            version: Schema.Version(1, 0, 0)
        )

        return try await DBContainer(
            testing: schema,
            configuration: .init(backend: .custom(database)),
            security: .disabled
        )
    }

    private func setupNoIndexContainer() async throws -> DBContainer {
        try await FDBTestSetup.shared.initialize()
        let database = try await FDBTestSetup.shared.makeEngine()
        let schema = Schema(
            [PolymorphicVectorNoIndexArticle.self],
            version: Schema.Version(1, 0, 0)
        )

        return try await DBContainer(
            testing: schema,
            configuration: .init(backend: .custom(database)),
            security: .disabled
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
            if try await container.engine.directoryService.exists(path: path) {
                try await container.engine.directoryService.remove(path: path)
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
        let schema = Schema(
            [PolymorphicVectorArticle.self, PolymorphicVectorReport.self],
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

        #expect(articleDescriptor.kind is VectorIndexKind<PolymorphicVectorArticle>)
        #expect(reportDescriptor.kind is VectorIndexKind<PolymorphicVectorReport>)
        #expect(articleDescriptor.keyPaths.first is PartialKeyPath<PolymorphicVectorArticle>)
        #expect(reportDescriptor.keyPaths.first is PartialKeyPath<PolymorphicVectorReport>)
    }

    @Test("Polymorphic vector query requires a query vector")
    func polymorphicVectorQueryRequiresQueryVector() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        do {
            _ = try await context.findPolymorphic(PolymorphicVectorArticle.self)
                .vector(\.embedding, dimensions: 3)
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

    @Test("Polymorphic vector query reports missing shared descriptor")
    func polymorphicVectorQueryReportsMissingSharedDescriptor() async throws {
        let container = try await setupNoIndexContainer()
        try await cleanup(container: container)
        let context = container.newContext()

        do {
            _ = try await context.findPolymorphic(PolymorphicVectorNoIndexArticle.self)
                .vector(\.embedding, dimensions: 3)
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
        VectorReadBridge.registerReadExecutors()

        let context = container.newContext()
        let article = PolymorphicOptionalVectorArticle(
            title: "Optional Anchor",
            embedding: [1.0, 0.0, 0.0],
            body: "Article body"
        )
        let report = PolymorphicOptionalVectorReport(
            title: "Optional Near",
            embedding: [0.95, 0.05, 0.0],
            pageCount: 3
        )

        context.insert(article)
        context.insert(report)
        try await context.save()

        #expect(try await countOptionalVectorIndexEntries(container: container) == 2)

        let first = try await context.findPolymorphic(PolymorphicOptionalVectorArticle.self)
            .vector(\.embedding, dimensions: 3)
            .query([1.0, 0.0, 0.0], k: 1)
            .first()

        #expect(first?.item(as: PolymorphicOptionalVectorArticle.self)?.id == article.id)

        let results = try await context.findPolymorphic(PolymorphicOptionalVectorReport.self)
            .vector(\.embedding, dimensions: 3)
            .query([1.0, 0.0, 0.0], k: 2)
            .execute()
        let resultIDs = Set(results.compactMap(optionalResultID))

        #expect(resultIDs == Set([article.id, report.id]))
    }

    @Test("Polymorphic vector index is maintained and queried end-to-end")
    func polymorphicVectorIndexIsMaintainedAndQueriedEndToEnd() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        VectorReadBridge.registerReadExecutors()

        let context = container.newContext()

        let article = PolymorphicVectorArticle(
            title: "Anchor",
            embedding: [1.0, 0.0, 0.0],
            body: "Article body"
        )
        var report = PolymorphicVectorReport(
            title: "Near",
            embedding: [0.95, 0.05, 0.0],
            pageCount: 3
        )
        let farReport = PolymorphicVectorReport(
            title: "Far",
            embedding: [0.0, 1.0, 0.0],
            pageCount: 9
        )

        context.insert(article)
        context.insert(report)
        context.insert(farReport)
        try await context.save()

        #expect(try await countVectorIndexEntries(container: container) == 3)

        let firstPage = try await context.findPolymorphic(PolymorphicVectorArticle.self)
            .vector(\.embedding, dimensions: 3)
            .query([1.0, 0.0, 0.0], k: 2)
            .metric(.cosine)
            .executePage()

        #expect(firstPage.results.count == 2)
        #expect(firstPage.results.first?.item(as: PolymorphicVectorArticle.self)?.id == article.id)
        #expect(firstPage.results.dropFirst().first?.item(as: PolymorphicVectorReport.self)?.id == report.id)

        let reportStartedPage = try await context.findPolymorphic(PolymorphicVectorReport.self)
            .vector(\.embedding, dimensions: 3)
            .query([1.0, 0.0, 0.0], k: 2)
            .metric(.cosine)
            .executePage()
        let reportStartedIDs = Set(reportStartedPage.results.compactMap(resultID))

        #expect(reportStartedIDs == Set([article.id, report.id]))

        report.embedding = [1.0, 0.0, 0.0]
        try await context.savePolymorphic(report, as: PolymorphicVectorReport.self)

        #expect(try await countVectorIndexEntries(container: container) == 3)

        let updatedPage = try await context.findPolymorphic(PolymorphicVectorArticle.self)
            .vector(\.embedding, dimensions: 3)
            .query([1.0, 0.0, 0.0], k: 2)
            .metric(.cosine)
            .executePage()
        let updatedIDs = Set(updatedPage.results.compactMap(resultID))

        #expect(updatedIDs == Set([article.id, report.id]))

        try await context.deletePolymorphic(
            PolymorphicVectorArticle.self,
            id: article.id,
            as: PolymorphicVectorArticle.self
        )

        #expect(try await countVectorIndexEntries(container: container) == 2)

        let finalPage = try await context.findPolymorphic(PolymorphicVectorArticle.self)
            .vector(\.embedding, dimensions: 3)
            .query([1.0, 0.0, 0.0], k: 1)
            .metric(.cosine)
            .executePage()

        #expect(finalPage.results.count == 1)
        #expect(finalPage.results.first?.item(as: PolymorphicVectorReport.self)?.id == report.id)
    }
}
#endif
