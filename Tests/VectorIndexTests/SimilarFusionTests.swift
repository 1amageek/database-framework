import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import Testing
@testable import DatabaseEngine
@testable import VectorIndex

@Persistable
private struct SimilarFusionDocument {
    #Directory<SimilarFusionDocument>("vector_similar_fusion_documents")

    var id: String
    var title: String
    var embedding: Vector

    #Index(
        .vector(dimensions: 2, metric: .dotProduct),
        embedding: \SimilarFusionDocument.embedding,
        name: "SimilarFusionDocument_embedding"
    )
}

@Suite("Similar fusion query", .serialized)
struct SimilarFusionTests {
    @Test("Similar uses the configured vector backend and canonical identifiers")
    func selectedBackendAndIdentifiers() async throws {
        for algorithm in [
            VectorAlgorithm.flat,
            .hnsw(.init(m: 4, efConstruction: 20, efSearch: 20)),
        ] {
            let container = try await makeContainer(algorithm: algorithm)
            do {
                let context = container.newContext()
                let strongest = SimilarFusionDocument(
                    id: "strongest",
                    title: "Strongest",
                    embedding: try Vector(float32: [2, 0])
                )
                let weaker = SimilarFusionDocument(
                    id: "weaker",
                    title: "Weaker",
                    embedding: try Vector(float32: [1, 0])
                )
                try context.insert(strongest)
                try context.insert(weaker)
                try await context.save()

                let query = Similar(
                    SimilarFusionDocument.fields.embedding,
                    dimensions: 2,
                    context: context.indexQueryContext
                )
                .nearest(to: try Vector(float32: [1, 0]), k: 2)
                .metric(.dotProduct)

                let results = try await query.execute(candidates: nil)

                #expect(results.map(\.item.id) == ["strongest", "weaker"])
                #expect(results.map(\.score) == [1, 0])
            } catch {
                await container.shutdown()
                throw error
            }
            await container.shutdown()
        }
    }

    @Test("Similar treats an explicit empty candidate set as empty")
    func emptyCandidatesAreRestrictive() async throws {
        let container = try await makeContainer(algorithm: .flat)
        do {
            let context = container.newContext()
            let query = Similar(
                SimilarFusionDocument.fields.embedding,
                dimensions: 2,
                context: context.indexQueryContext
            )
            .nearest(to: [1, 0], k: 1)
            .metric(.dotProduct)

            #expect(try await query.execute(candidates: []).isEmpty)
        } catch {
            await container.shutdown()
            throw error
        }
        await container.shutdown()
    }

    @Test("Large candidate sets remain semantically restrictive")
    func largeCandidateSetsDoNotDependOnGlobalNearestNeighbors() async throws {
        let container = try await makeContainer(algorithm: .flat)
        do {
            let context = container.newContext()
            var candidateIDs: Set<String> = []

            for index in 0...1_000 {
                let identifier = "candidate-\(index)"
                candidateIDs.insert(identifier)
                try context.insert(
                    SimilarFusionDocument(
                        id: identifier,
                        title: "Candidate",
                        embedding: try Vector(float32: [0, 1])
                    )
                )
            }
            for index in 0...1_000 {
                try context.insert(
                    SimilarFusionDocument(
                        id: "closer-outsider-\(index)",
                        title: "Outsider",
                        embedding: try Vector(float32: [2, 0])
                    )
                )
            }
            try await context.save()

            let results = try await Similar(
                SimilarFusionDocument.fields.embedding,
                dimensions: 2,
                context: context.indexQueryContext
            )
            .nearest(to: [1, 0], k: 1)
            .metric(.dotProduct)
            .execute(candidates: candidateIDs)

            #expect(results.count == 1)
            #expect(results.first.map { candidateIDs.contains($0.item.id) } == true)
        } catch {
            await container.shutdown()
            throw error
        }
        await container.shutdown()
    }

    @Test("Similar reports missing query configuration")
    func missingQueryIsTypedFailure() async throws {
        let container = try await makeContainer(algorithm: .flat)
        let context = container.newContext()
        let query = Similar(
            SimilarFusionDocument.fields.embedding,
            dimensions: 2,
            context: context.indexQueryContext
        ).metric(.dotProduct)

        do {
            _ = try await query.execute(candidates: nil)
            Issue.record("Expected missing vector query failure")
        } catch FusionQueryError.invalidConfiguration(let reason) {
            #expect(reason.contains("required"))
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
        await container.shutdown()
    }

    @Test("Vector queries report orphaned index entries as corruption")
    func orphanedIndexEntryIsTypedFailure() async throws {
        let engine = InMemoryEngine()
        let container = try await makeContainer(
            algorithm: .flat,
            engine: engine
        )
        let context = container.newContext()
        let indexName = "SimilarFusionDocument_embedding"
        let indexSubspace = try await context.indexQueryContext
            .withReadableIndex(
                named: indexName,
                kindIdentifier: VectorIndexSpecification.identifier,
                for: SimilarFusionDocument.self
            ) { readableIndex, _ in
                guard let readableIndex else {
                    throw VectorQueryError.indexNotFound(indexName)
                }
                return readableIndex.subspace
            }
        let orphanedIdentifier = Tuple("orphaned")
        try await engine.withTransaction { transaction in
            try transaction.setValue(
                VectorConversion.floatArrayToBytes([1, 0]),
                for: indexSubspace.pack(orphanedIdentifier)
            )
        }

        do {
            _ = try await Similar(
                SimilarFusionDocument.fields.embedding,
                dimensions: 2,
                context: context.indexQueryContext
            )
            .nearest(to: [1, 0], k: 1)
            .metric(.dotProduct)
            .execute(candidates: nil)
            Issue.record("Expected an orphaned vector index entry failure")
        } catch VectorQueryError.indexedItemMissing(
            let reportedIndex,
            let reportedPrimaryKey
        ) {
            #expect(reportedIndex == indexName)
            #expect(reportedPrimaryKey == orphanedIdentifier.pack())
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
        await container.shutdown()
    }

    private func makeContainer(
        algorithm: VectorAlgorithm,
        engine: InMemoryEngine = InMemoryEngine()
    ) async throws -> DBContainer {
        let schema = try Schema(
            entities: [try SimilarFusionDocument.schemaEntity]
        )
        return try await DBContainer.open(
            for: schema,
            configuration: .testing(
                storageEngine: engine,
                indexConfigurations: [
                    VectorIndexConfiguration<SimilarFusionDocument>(
                        field: SimilarFusionDocument.fields.embedding,
                        algorithm: algorithm
                    )
                ]
            ),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        SimilarFusionDocument.self
                    )
                ]
            ),
            security: .disabled
        )
    }
}
