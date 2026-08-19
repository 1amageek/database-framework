#if FOUNDATION_DB
import Testing
import DatabaseRuntime
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
    .vector(
        name: "PolymorphicVectorE2EDocument_embedding",
        embedding: "embedding",
        dimensions: 3, metric: .cosine
    ))
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
    .vector(
        name: "PolymorphicOptionalVectorE2EDocument_embedding",
        embedding: "embedding",
        dimensions: 3, metric: .cosine
    ))
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
            configuration: .testing(storageEngine: database),
            runtimeConfiguration: try vectorRuntimeConfiguration(
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(PolymorphicVectorArticle.self), try DatabaseFrameworkRuntime.entity(PolymorphicVectorReport.self),
                ]
            ),
            security: .testingDisabled
        )
    }

    private func setupOptionalContainer() async throws -> DBContainer {
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
            configuration: .testing(storageEngine: database),
            runtimeConfiguration: try vectorRuntimeConfiguration(
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(PolymorphicOptionalVectorArticle.self), try DatabaseFrameworkRuntime.entity(PolymorphicOptionalVectorReport.self),
                ]
            ),
            security: .testingDisabled
        )
    }

    private func setupNoIndexContainer() async throws -> DBContainer {
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let schema = try Schema(
            entities: [
                try PolymorphicVectorNoIndexArticle.schemaEntity
            ],
            version: Schema.Version(1, 0, 0)
        )

        return try await DBContainer.open(
            testing: schema,
            configuration: .testing(storageEngine: database),
            runtimeConfiguration: try vectorRuntimeConfiguration(
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(PolymorphicVectorNoIndexArticle.self)]
            ),
            security: .testingDisabled
        )
    }

    private func vectorRuntimeConfiguration(
        entityRuntimes: [EntityRuntimeRegistration],
        indexConfigurations: [any IndexRuntimeConfiguration] = []
    ) throws -> DatabaseRuntimeConfiguration {
        try DatabaseRuntimeConfiguration(
            executionIdentity: DatabaseExecutionRuntimeIdentity(
                identifier: "database-tests",
                revision: 1
            ),
            indexMaintainerProviderDescriptors: [
                .init(describing: VectorIndexMaintainerProvider())
            ],
            polymorphicIndexReadExecutors: [VectorReadExecutors.polymorphicIndexExecutor()],
            entityRuntimes: entityRuntimes,
            indexConfigurations: indexConfigurations
        )
    }

    @Test("Polymorphic vector runtime policy targets the declaration name")
    func polymorphicRuntimePolicyUsesDeclarationName() throws {
        let schema = try Schema(
            entities: [
                try PolymorphicVectorArticle.schemaEntity,
                try PolymorphicVectorReport.schemaEntity,
            ],
            version: Schema.Version(1, 0, 0)
        )
        let runtime = try vectorRuntimeConfiguration(
            entityRuntimes: [
                try DatabaseFrameworkRuntime.entity(
                    PolymorphicVectorArticle.self
                ),
                try DatabaseFrameworkRuntime.entity(
                    PolymorphicVectorReport.self
                ),
            ],
            indexConfigurations: [
                VectorIndexConfiguration(
                    indexName: indexName,
                    algorithm: .hnsw(.default)
                )
            ]
        )

        let layouts = try IndexRuntimeConfigurationValidator.validate(
            schema: schema,
            runtimeConfiguration: runtime
        )
        #expect(layouts[indexName]?.name == "vector.hnsw")
    }

    private func countVectorIndexEntries(container: DBContainer) async throws -> Int {
        let group = try container.polymorphicGroup(identifier: PolymorphicVectorArticle.polymorphableType)
        let groupSubspace = try await container.testBasePolymorphicDirectory(for: group.identifier)
        let indexSubspace = try IndexLifecycleStore(
            container: container,
            subspace: groupSubspace
        ).indexSubspace(for: indexName)

        return try await container.engine.withTransaction { transaction -> Int in
            let (begin, end) = indexSubspace.range()
            return try await transaction.collectRange(
                begin: begin,
                end: end,
                snapshot: true
            ).count
        }
    }

    private func countOptionalVectorIndexEntries(container: DBContainer) async throws -> Int {
        let group = try container.polymorphicGroup(identifier: PolymorphicOptionalVectorArticle.polymorphableType)
        let groupSubspace = try await container.testBasePolymorphicDirectory(for: group.identifier)
        let indexSubspace = try IndexLifecycleStore(
            container: container,
            subspace: groupSubspace
        ).indexSubspace(for: optionalIndexName)

        return try await container.engine.withTransaction { transaction -> Int in
            let (begin, end) = indexSubspace.range()
            return try await transaction.collectRange(
                begin: begin,
                end: end,
                snapshot: true
            ).count
        }
    }

    private func resultID(_ result: PolymorphicQueryResult) throws -> String? {
        if let article = try result.decodedModel(as: PolymorphicVectorArticle.self) {
            return article.id
        }
        if let report = try result.decodedModel(as: PolymorphicVectorReport.self) {
            return report.id
        }
        return nil
    }

    private func optionalResultID(_ result: PolymorphicQueryResult) throws -> String? {
        if let article = try result.decodedModel(as: PolymorphicOptionalVectorArticle.self) {
            return article.id
        }
        if let report = try result.decodedModel(as: PolymorphicOptionalVectorReport.self) {
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
            articleDescriptor.declaration.definition
        )
        let reportSpecification = try VectorIndexSpecification(
            reportDescriptor.declaration.definition
        )
        #expect(articleDescriptor.fieldNames == ["embedding"])
        #expect(reportDescriptor.fieldNames == ["embedding"])
        #expect(articleSpecification.dimensions == 3)
        #expect(reportSpecification.dimensions == 3)
    }

    @Test("Polymorphic vector query requires a query vector")
    func polymorphicVectorQueryRequiresQueryVector() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            let context = container.testBaseContext()

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
    }

    @Test("Polymorphic vector query rejects mismatched query dimensions")
    func polymorphicVectorQueryRejectsMismatchedDimensions() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            let context = container.testBaseContext()

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
    }

    @Test("Polymorphic vector query reports missing shared descriptor")
    func polymorphicVectorQueryReportsMissingSharedDescriptor() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupNoIndexContainer()
            let context = container.testBaseContext()

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
    }

    @Test("Polymorphic optional vector KeyPath overload queries shared index end-to-end")
    func polymorphicOptionalVectorKeyPathOverloadQueriesSharedIndexEndToEnd() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupOptionalContainer()
            let context = container.testBaseContext()
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

            #expect(try first?.decodedModel(as: PolymorphicOptionalVectorArticle.self)?.id == article.id)

            let results = try await context.findPolymorphic(PolymorphicOptionalVectorReport.self)
                .vector(
                    PolymorphicOptionalVectorReport.fields.embedding,
                    dimensions: 3
                )
                .query([1.0, 0.0, 0.0], k: 2)
                .execute()
            let resultIDs = try Set(results.compactMap(optionalResultID))

            #expect(resultIDs == Set([article.id, report.id]))
        }
    }

    @Test("Polymorphic vector index is maintained and queried end-to-end")
    func polymorphicVectorIndexIsMaintainedAndQueriedEndToEnd() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            let context = container.testBaseContext()

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
            #expect(try firstPage.results.first?.decodedModel(as: PolymorphicVectorArticle.self)?.id == article.id)
            #expect(try firstPage.results.dropFirst().first?.decodedModel(as: PolymorphicVectorReport.self)?
                    .id == report.id)

            let reportStartedPage = try await context.findPolymorphic(
                PolymorphicVectorReport.self
            )
            .vector(PolymorphicVectorReport.fields.embedding, dimensions: 3)
            .query([1.0, 0.0, 0.0], k: 2)
            .metric(.cosine)
            .executePage()
            let reportStartedIDs = try Set(reportStartedPage.results.compactMap(resultID))

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
            let updatedIDs = try Set(updatedPage.results.compactMap(resultID))

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
            #expect(
                try finalPage.results.first?.decodedModel(as: PolymorphicVectorReport.self)?.id == report.id)
        }
    }
}
#endif
