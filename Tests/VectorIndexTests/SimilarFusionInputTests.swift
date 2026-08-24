import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import TestSupport
import Testing

@testable import DatabaseEngine
@testable import VectorIndex

@Persistable
private struct VectorFusionInputItem {
    var id: String
    var embedding: Vector
}

@Persistable
private struct VectorFusionExecutionItem {
    #Index(
        .vector(
            name: "vector_fusion_embedding",
            embedding: \VectorFusionExecutionItem.embedding,
            dimensions: 4,
            metric: .euclidean
        )
    )

    var id: String
    var name: String
    var embedding: Vector
    var eligible: Bool
}

@Suite("Vector Fusion input")
struct SimilarFusionInputTests {
    @Test("Similar lowers to an immutable canonical index input")
    func lowersToCanonicalInput() throws {
        let query = try Similar(
            VectorFusionInputItem.fields.embedding,
            dimensions: 3
        )
        .index(named: "vector_embedding")
        .nearest(to: [1, 2, 3], k: 7)
        .metric(.euclidean)

        let input = query.fusionInput
        #expect(input.scoring == .annotation(
            name: "distance",
            order: .lowerIsBetter
        ))
        #expect(input.requirement == .unrestricted)
        #expect(input.limit == 7)
        guard case .index(let source) = input.operation else {
            Issue.record("Similar must lower to an index operation")
            return
        }
        #expect(source.selection == .named(
            name: "vector_embedding",
            type: .vector
        ))
        #expect(source.referencedFields == [
            VectorFusionInputItem.fields.embedding.identity,
        ])
        #expect(source.parameters[VectorReadParameter.fieldName] == .string(
            "embedding"
        ))
        #expect(source.parameters[VectorReadParameter.dimensions] == .int64(3))
        #expect(source.parameters[VectorReadParameter.metric] == .string(
            VectorDistanceMetric.euclidean.rawValue
        ))
        #expect(source.parameters[VectorReadParameter.queryVector] != nil)
    }

    @Test("Similar rejects a negative result count")
    func rejectsNegativeResultCount() throws {
        let query = Similar(
            VectorFusionInputItem.fields.embedding,
            dimensions: 3
        )
        #expect {
            _ = try query.nearest(to: [1, 2, 3], k: -1)
        } throws: { error in
            error as? VectorFusionInputError == .invalidResultCount(-1)
        }
    }

    @Test("Similar executes exact ordering for every physical layout")
    func executesEveryPhysicalLayout() async throws {
        let algorithms: [(name: String, algorithm: VectorAlgorithm)] = [
            ("flat", .flat),
            ("hnsw", .hnsw(.default)),
            (
                "ivf",
                .ivf(try VectorIVFParameters(
                    nlist: 2,
                    nprobe: 1,
                    kmeansIterations: 2
                ))
            ),
            (
                "pq",
                .pq(try VectorPQParameters(m: 2, niter: 2))
            ),
        ]

        for configuration in algorithms {
            let container = try await makeContainer(
                algorithm: configuration.algorithm,
                runtimeIdentifier: "vector-fusion-\(configuration.name)"
            )
            do {
                let context = container.testBaseContext()
                try await insertFixtures(into: context)
                let similar = try Similar(
                    VectorFusionExecutionItem.fields.embedding,
                    dimensions: 4
                )
                    .index(named: "vector_fusion_embedding")
                    .nearest(to: [0, 0, 0, 0], k: 3)
                    .metric(.euclidean)
                let result = try await context.execute(
                    FusionQuery<VectorFusionExecutionItem> {
                        similar
                    }
                )
                #expect(
                    result.results.map(\.item.id)
                        == ["exact", "near", "farther"],
                    "Unexpected ordering for \(configuration.name)"
                )
            } catch {
                await container.shutdown()
                throw error
            }
            await container.shutdown()
        }
    }

    @Test("Similar restricts candidates before selecting top K")
    func restrictsCandidatesBeforeTopK() async throws {
        let container = try await makeContainer(
            algorithm: .hnsw(.default),
            runtimeIdentifier: "vector-fusion-restricted"
        )
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        try await insertFixtures(into: context)
        let eligible = try Filter(
            VectorFusionExecutionItem.fields.eligible,
            equals: true
        )
        let similar = try Similar(
            VectorFusionExecutionItem.fields.embedding,
            dimensions: 4
        )
            .index(named: "vector_fusion_embedding")
            .nearest(to: [0, 0, 0, 0], k: 2)
            .metric(.euclidean)

        let result = try await context.execute(
            FusionQuery<VectorFusionExecutionItem> {
                eligible
                similar
            }
        )
        #expect(result.results.map(\.item.id) == ["near", "farther"])
    }

    @Test("Malformed vector payloads fail as index corruption")
    func malformedPayloadFailsAsCorruption() async throws {
        let container = try await makeContainer(
            algorithm: .flat,
            runtimeIdentifier: "vector-fusion-corruption"
        )
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let item = try VectorFusionExecutionItem(
            id: "corrupted",
            name: "Corrupted",
            embedding: Vector(float32: [0, 0, 0, 0]),
            eligible: true
        )
        try context.insert(item)
        try await context.save()

        try await container.withTestBaseTransaction { transaction in
            let readable = try #require(
                try await context.indexQueryContext.readableIndex(
                    named: "vector_fusion_embedding",
                    indexType: .vector,
                    for: VectorFusionExecutionItem.self,
                    transaction: transaction
                )
            )
            try transaction.setValue(
                ByteString([0x01]),
                for: readable.subspace.pack(Tuple(item.id))
            )
        }
        let similar = try Similar(
            VectorFusionExecutionItem.fields.embedding,
            dimensions: 4
        )
            .index(named: "vector_fusion_embedding")
            .nearest(to: [0, 0, 0, 0], k: 1)
            .metric(.euclidean)

        await #expect {
            _ = try await context.execute(
                FusionQuery<VectorFusionExecutionItem> {
                    similar
                }
            )
        } throws: { error in
            error as? FusionExecutionError == .corruptedIndex(.vector)
        }
    }

    private func makeContainer(
        algorithm: VectorAlgorithm,
        runtimeIdentifier: String
    ) async throws -> DBContainer {
        let entity = try VectorFusionExecutionItem.schemaEntity
        return try await DBContainer.open(
            testing: try Schema(entities: [entity]),
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: runtimeIdentifier,
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        VectorFusionExecutionItem.self
                    ),
                ],
                indexConfigurations: [
                    VectorIndexConfiguration(
                        indexName: "vector_fusion_embedding",
                        algorithm: algorithm
                    ),
                ]
            ),
            security: .testingDisabled
        )
    }

    private func insertFixtures(
        into context: DatabaseContext
    ) async throws {
        for item in [
            try VectorFusionExecutionItem(
                id: "exact",
                name: "Exact",
                embedding: Vector(float32: [0, 0, 0, 0]),
                eligible: false
            ),
            try VectorFusionExecutionItem(
                id: "near",
                name: "Near",
                embedding: Vector(float32: [1, 0, 0, 0]),
                eligible: true
            ),
            try VectorFusionExecutionItem(
                id: "farther",
                name: "Farther",
                embedding: Vector(float32: [2, 0, 0, 0]),
                eligible: true
            ),
            try VectorFusionExecutionItem(
                id: "outside",
                name: "Outside",
                embedding: Vector(float32: [10, 0, 0, 0]),
                eligible: false
            ),
        ] {
            try context.insert(item)
        }
        try await context.save()
    }
}
