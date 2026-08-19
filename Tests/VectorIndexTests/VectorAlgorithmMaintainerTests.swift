// VectorAlgorithmMaintainerTests.swift
// Tests for non-default VectorIndex algorithm maintainers

import DatabaseKit
import DatabaseTypes
import Foundation
import StorageKit
import TestHeartbeat
import Testing

@testable import DatabaseEngine
@testable import VectorIndex

@Suite("Vector Algorithm Maintainer Tests", .serialized, .heartbeat)
struct VectorAlgorithmMaintainerTests {

    @Test("Heap reverse sorting preserves strict ordering for equivalent values")
    func heapReverseSortPreservesEquivalentInsertionOrder() {
        var heap = MinHeap<(ordinal: Int, distance: Double)>(
            maxSize: 8,
            heapType: .max,
            comparator: { $0.distance > $1.distance }
        )
        for ordinal in 0..<8 {
            heap.insert((ordinal: ordinal, distance: 1))
        }

        #expect(heap.sorted().map(\.ordinal) == Array(0..<8))
    }

    @Test("PQ distance tables preserve each vector metric")
    func pqDistanceTablesPreserveVectorMetrics() throws {
        let quantizer = try ProductQuantizer(
            dimensions: 4,
            codebooks: [
                [[1, 0], [-1, 0], [0, 0]],
                [[0, 0], [0, 1], [0, 0]],
            ]
        )
        let query: [Float] = [1, 0, 0, 0]
        let sameDirectionCodes: [UInt8] = [0, 0]
        let oppositeDirectionCodes: [UInt8] = [1, 0]

        let euclideanTable = try quantizer.distanceTable(for: query, metric: .euclidean)
        #expect(try quantizer.distance(for: sameDirectionCodes, using: euclideanTable) == 0)
        #expect(try quantizer.distance(for: oppositeDirectionCodes, using: euclideanTable) == 2)

        let cosineTable = try quantizer.distanceTable(for: query, metric: .cosine)
        #expect(try quantizer.distance(for: sameDirectionCodes, using: cosineTable) == 0)
        #expect(try quantizer.distance(for: oppositeDirectionCodes, using: cosineTable) == 2)

        let dotProductTable = try quantizer.distanceTable(for: query, metric: .dotProduct)
        #expect(try quantizer.distance(for: sameDirectionCodes, using: dotProductTable) == -1)
        #expect(try quantizer.distance(for: oppositeDirectionCodes, using: dotProductTable) == 1)
    }

    @Test("PQ rejects invalid layouts and compressed codes as typed failures")
    func pqRejectsInvalidLayoutsAndCodes() throws {
        #expect(throws: ProductQuantizationError.self) {
            _ = try ProductQuantizer(dimensions: 3, parameters: PQParameters(m: 2))
        }
        #expect(throws: ProductQuantizationError.self) {
            _ = try ProductQuantizer(dimensions: 4, codebooks: [])
        }

        let quantizer = try ProductQuantizer(
            dimensions: 2,
            codebooks: [[[1, 0], [0, 1]]]
        )
        let table = try quantizer.distanceTable(for: [1, 0], metric: .cosine)
        #expect(throws: ProductQuantizationError.self) {
            _ = try quantizer.distance(for: [UInt8](), using: table)
        }
        #expect(throws: ProductQuantizationError.self) {
            _ = try quantizer.distance(for: [UInt8(2)], using: table)
        }
        #expect(throws: ProductQuantizationError.nonFiniteCentroidElement(
            subspace: 0,
            centroid: 0,
            element: 1
        )) {
            _ = try ProductQuantizer(
                dimensions: 2,
                codebooks: [[[1, .nan]]]
            )
        }
        #expect(throws: ProductQuantizationError.nonFiniteInputElement(1)) {
            _ = try quantizer.encode(vector: [1, .infinity])
        }

        let untrained = try ProductQuantizer(
            dimensions: 2,
            parameters: PQParameters(m: 1)
        )
        #expect(throws: ProductQuantizationError.nonFiniteTrainingElement(
            vector: 0,
            element: 1
        )) {
            _ = try untrained.train(vectors: [[1, .nan]])
        }
    }

    @Test("Vector training is deterministic across repeated executions")
    func vectorTrainingIsDeterministic() throws {
        let vectors: [[Float]] = [
            [1, 0, 0, 0],
            [0.9, 0.1, 0, 0],
            [0, 1, 0, 0],
            [0.1, 0.9, 0, 0],
        ]
        let clustering = KMeansClustering(
            k: 2,
            dimensions: 4,
            maxIterations: 4
        )
        #expect(clustering.train(vectors: vectors) == clustering.train(vectors: vectors))

        let quantizer = try ProductQuantizer(
            dimensions: 4,
            parameters: PQParameters(m: 2, niter: 4)
        )
        #expect(
            try quantizer.train(vectors: vectors).trainedCodebooks
                == quantizer.train(vectors: vectors).trainedCodebooks
        )
    }

    @Test("IVF serves exact results while untrained and preserves state transitions")
    func ivfUntrainedLifecycle() async throws {
        let database = InMemoryEngine()
        let context = try makeContext(name: "ivf-untrained-lifecycle")
        let maintainer = IVFIndexMaintainer<HNSWDocument>(
            index: context.index,
            dimensions: 4,
            metric: .euclidean,
            subspace: context.indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            parameters: IVFParameters(nlist: 2, nprobe: 2, kmeansIterations: 3)
        )
        let documents = try algorithmDocuments()

        try await database.withTransaction { transaction in
            try await maintainer.finalizeBuild(transaction: transaction)
            for document in documents {
                try await maintainer.scanItem(
                    document,
                    id: Tuple(document.id),
                    transaction: transaction
                )
            }
        }
        #expect(try await database.withTransaction { transaction in
            try await maintainer.isTrained(transaction: transaction)
        } == false)
        let initial = try await database.withTransaction { transaction in
            try await maintainer.search(
                queryVector: [1, 0, 0, 0],
                k: documents.count,
                transaction: transaction
            )
        }
        #expect(initial.count == documents.count)
        #expect(initial.first?.primaryKey.first as? String == "exact")

        try await database.withTransaction { transaction in
            for document in documents {
                try await maintainer.updateIndex(
                    oldItem: document,
                    newItem: nil,
                    transaction: transaction
                )
            }
        }
        let empty = try await database.withTransaction { transaction in
            try await maintainer.search(
                queryVector: [1, 0, 0, 0],
                k: 1,
                transaction: transaction
            )
        }
        #expect(empty.isEmpty)
        #expect(try await database.withTransaction { transaction in
            try await maintainer.isTrained(transaction: transaction)
        } == false)
    }

    @Test("PQ serves exact results while untrained and preserves state transitions")
    func pqUntrainedLifecycle() async throws {
        let database = InMemoryEngine()
        let context = try makeContext(name: "pq-untrained-lifecycle")
        let maintainer = try PQIndexMaintainer<HNSWDocument>(
            index: context.index,
            dimensions: 4,
            metric: .euclidean,
            subspace: context.indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            parameters: PQParameters(m: 2, niter: 3)
        )
        let documents = try algorithmDocuments()

        try await database.withTransaction { transaction in
            try await maintainer.finalizeBuild(transaction: transaction)
            for document in documents {
                try await maintainer.scanItem(
                    document,
                    id: Tuple(document.id),
                    transaction: transaction
                )
            }
        }
        #expect(try await database.withTransaction { transaction in
            try await maintainer.isTrained(transaction: transaction)
        } == false)
        let initial = try await database.withTransaction { transaction in
            try await maintainer.search(
                queryVector: [1, 0, 0, 0],
                k: documents.count,
                transaction: transaction
            )
        }
        #expect(initial.count == documents.count)
        #expect(initial.first?.primaryKey.first as? String == "exact")

        try await database.withTransaction { transaction in
            for document in documents {
                try await maintainer.updateIndex(
                    oldItem: document,
                    newItem: nil,
                    transaction: transaction
                )
            }
        }
        let empty = try await database.withTransaction { transaction in
            try await maintainer.search(
                queryVector: [1, 0, 0, 0],
                k: 1,
                transaction: transaction
            )
        }
        #expect(empty.isEmpty)
        #expect(try await database.withTransaction { transaction in
            try await maintainer.isTrained(transaction: transaction)
        } == false)
    }

    @Test("IVF training rejects invalid input without marking the index trained")
    func ivfTrainingRejectsInvalidInput() async throws {
        let database = InMemoryEngine()
        let context = try makeContext(name: "ivf-invalid-training")
        let maintainer = IVFIndexMaintainer<HNSWDocument>(
            index: context.index,
            dimensions: 4,
            metric: .euclidean,
            subspace: context.indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            parameters: IVFParameters(
                nlist: 2,
                nprobe: 2,
                kmeansIterations: 3
            )
        )

        await #expect(throws: VectorIndexError.self) {
            try await database.withTransaction { transaction in
                try await maintainer.train(
                    vectors: [],
                    transaction: transaction
                )
            }
        }
        await #expect(throws: VectorIndexError.self) {
            try await database.withTransaction { transaction in
                try await maintainer.train(
                    vectors: [[1, 0]],
                    transaction: transaction
                )
            }
        }
        await #expect(throws: VectorIndexError.self) {
            try await database.withTransaction { transaction in
                try await maintainer.train(
                    vectors: [[1, 0, .nan, 0]],
                    transaction: transaction
                )
            }
        }

        let isTrained = try await database.withTransaction { transaction in
            try await maintainer.isTrained(transaction: transaction)
        }
        #expect(!isTrained)

        let invalidDimensionMaintainer = IVFIndexMaintainer<HNSWDocument>(
            index: context.index,
            dimensions: 0,
            metric: .euclidean,
            subspace: context.indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            parameters: IVFParameters(
                nlist: 2,
                nprobe: 2,
                kmeansIterations: 3
            )
        )
        await #expect(throws: VectorIndexError.self) {
            try await database.withTransaction { transaction in
                try await invalidDimensionMaintainer.train(
                    vectors: [[1]],
                    transaction: transaction
                )
            }
        }
    }

    @Test("IVF training and retraining atomically redistribute stored vectors")
    func ivfTrainingRedistributesStoredVectors() async throws {
        let database = InMemoryEngine()
        let context = try makeContext(name: "ivf-retraining")
        let maintainer = IVFIndexMaintainer<HNSWDocument>(
            index: context.index,
            dimensions: 4,
            metric: .euclidean,
            subspace: context.indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            parameters: IVFParameters(
                nlist: 2,
                nprobe: 2,
                kmeansIterations: 3
            )
        )
        let docs = try algorithmDocuments()

        try await database.withTransaction { transaction in
            try await maintainer.scanItems(
                docs.map { (item: $0, id: Tuple($0.id)) },
                transaction: transaction
            )
            try await maintainer.train(
                vectors: [[1, 0, 0, 0], [0, 1, 0, 0]],
                transaction: transaction
            )
        }
        #expect(try await collectValues(
            database: database,
            subspace: context.indexSubspace.subspace(0)
        ).count == 2)

        try await database.withTransaction { transaction in
            try await maintainer.train(
                vectors: [[1, 0, 0, 0]],
                transaction: transaction
            )
        }
        #expect(try await collectValues(
            database: database,
            subspace: context.indexSubspace.subspace(0)
        ).count == 1)
        #expect(try await collectValues(
            database: database,
            subspace: context.indexSubspace.subspace(2)
        ).count == docs.count)
        #expect(try await collectValues(
            database: database,
            subspace: context.indexSubspace.subspace(3)
        ).count == docs.count)

        let results = try await database.withTransaction { transaction in
            try await maintainer.search(
                queryVector: [1, 0, 0, 0],
                k: docs.count,
                transaction: transaction
            )
        }
        #expect(results.count == docs.count)
        #expect(results.first?.primaryKey.first as? String == "exact")
    }

    @Test("IVF deletion rejects malformed assignment identifiers")
    func ivfDeletionRejectsMalformedAssignmentIdentifiers() async throws {
        let database = InMemoryEngine()
        let context = try makeContext(name: "ivf-malformed-assignment")
        let maintainer = IVFIndexMaintainer<HNSWDocument>(
            index: context.index,
            dimensions: 4,
            metric: .euclidean,
            subspace: context.indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            parameters: IVFParameters(
                nlist: 2,
                nprobe: 2,
                kmeansIterations: 1
            )
        )
        let document = HNSWDocument(
            id: "malformed",
            title: "Malformed",
            embedding: try Vector(float32: [1, 0, 0, 0])
        )
        let assignmentKey = context.indexSubspace
            .subspace(IVFIndexStorageKey.assignments.rawValue)
            .pack(Tuple(document.id))

        for payload in [
            Tuple().pack(),
            Tuple(Int64.max).pack(),
            Tuple(Int64(0), Int64(1)).pack(),
        ] {
            try await database.withTransaction { transaction in
                try transaction.setValue(payload, for: assignmentKey)
            }
            await #expect(throws: VectorIndexError.self) {
                try await database.withTransaction { transaction in
                    try await maintainer.updateIndex(
                        oldItem: document,
                        newItem: nil,
                        transaction: transaction
                    )
                }
            }
        }
    }

    @Test("IVF training rejects out-of-range persisted cluster identifiers")
    func ivfTrainingRejectsOutOfRangeClusterIdentifiers() async throws {
        for corruptListKey in [false, true] {
            let database = InMemoryEngine()
            let context = try makeContext(
                name: corruptListKey
                    ? "ivf-list-cluster-out-of-range"
                    : "ivf-assignment-cluster-out-of-range"
            )
            let parameters = IVFParameters(
                nlist: 2,
                nprobe: 2,
                kmeansIterations: 1
            )
            let maintainer = IVFIndexMaintainer<HNSWDocument>(
                index: context.index,
                dimensions: 4,
                metric: .euclidean,
                subspace: context.indexSubspace,
                idExpression: FieldKeyExpression(fieldName: "id"),
                parameters: parameters
            )
            let document = try #require(algorithmDocuments().first)
            let id = Tuple(document.id)
            let assignments = context.indexSubspace.subspace(
                IVFIndexStorageKey.assignments.rawValue
            )
            let lists = context.indexSubspace.subspace(
                IVFIndexStorageKey.lists.rawValue
            )

            try await database.withTransaction { transaction in
                try await maintainer.scanItem(
                    document,
                    id: id,
                    transaction: transaction
                )
                if corruptListKey {
                    let originalKey = lists.subspace(0).pack(id)
                    let payload = try #require(
                        try await transaction.getValue(for: originalKey)
                    )
                    try transaction.clear(key: originalKey)
                    try transaction.setValue(
                        payload,
                        for: lists.subspace(parameters.nlist).pack(id)
                    )
                } else {
                    try transaction.setValue(
                        Tuple(Int64(parameters.nlist)).pack(),
                        for: assignments.pack(id)
                    )
                }
            }

            await #expect(throws: VectorIndexError.self) {
                try await database.withTransaction { transaction in
                    try await maintainer.finalizeBuild(transaction: transaction)
                }
            }
            #expect(try await database.withTransaction { transaction in
                try await maintainer.isTrained(transaction: transaction)
            } == false)
        }
    }

    @Test("IVF rejects non-contiguous persisted centroid keys")
    func ivfRejectsNonContiguousCentroidKeys() async throws {
        let database = InMemoryEngine()
        let context = try makeContext(name: "ivf-centroid-key-corrupt")
        let maintainer = IVFIndexMaintainer<HNSWDocument>(
            index: context.index,
            dimensions: 4,
            metric: .euclidean,
            subspace: context.indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            parameters: IVFParameters(nlist: 2, nprobe: 1, kmeansIterations: 3)
        )
        let document = try #require(algorithmDocuments().first)

        try await database.withTransaction { transaction in
            try await maintainer.scanItem(
                document,
                id: Tuple(document.id),
                transaction: transaction
            )
            try await maintainer.train(
                vectors: [[1, 0, 0, 0], [0, 1, 0, 0]],
                transaction: transaction
            )
            let centroidSubspace = context.indexSubspace.subspace(0)
            let originalKey = centroidSubspace.pack(Tuple(0))
            let payload = try #require(
                try await transaction.getValue(for: originalKey)
            )
            try transaction.clear(key: originalKey)
            try transaction.setValue(
                payload,
                for: centroidSubspace.pack(Tuple(2))
            )
        }

        await #expect(throws: VectorIndexError.self) {
            _ = try await database.withTransaction { transaction in
                try await maintainer.search(
                    queryVector: [1, 0, 0, 0],
                    k: 1,
                    transaction: transaction
                )
            }
        }
    }

    @Test("IVF stores contiguous Float32 vector payloads and returns nearest neighbors after training")
    func ivfStoresContiguousFloat32VectorPayloadsAndSearchesAfterTraining() async throws {
        let database = InMemoryEngine()
        let context = try makeContext(name: "ivf")
        let maintainer = IVFIndexMaintainer<HNSWDocument>(
            index: context.index,
            dimensions: 4,
            metric: .euclidean,
            subspace: context.indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            parameters: IVFParameters(nlist: 2, nprobe: 2, kmeansIterations: 3)
        )
        let docs = try algorithmDocuments()
        let trainingVectors = try docs.map { try float32Elements(of: $0.embedding) }

        try await database.withTransaction { transaction in
            try await maintainer.train(vectors: trainingVectors, transaction: transaction)
            try await maintainer.scanItems(
                docs.map { (item: $0, id: Tuple($0.id)) },
                transaction: transaction
            )
        }

        let results = try await database.withTransaction { transaction in
            try await maintainer.search(queryVector: [1, 0, 0, 0], k: 2, transaction: transaction)
        }
        let firstId = results.first?.primaryKey.first as? String
        #expect(results.count == 2)
        #expect(firstId == "exact")

        let payloadLengths = try await collectValues(
            database: database,
            subspace: context.indexSubspace.subspace(2)
        ).map(\.count)
        #expect(payloadLengths.contains(16))
        #expect(payloadLengths.allSatisfy { $0 == 16 })
    }

    @Test("IVF rejects malformed stored vector payloads")
    func ivfRejectsMalformedStoredVectorPayloads() async throws {
        let database = InMemoryEngine()
        let context = try makeContext(name: "ivf-corrupt")
        let maintainer = IVFIndexMaintainer<HNSWDocument>(
            index: context.index,
            dimensions: 4,
            metric: .euclidean,
            subspace: context.indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            parameters: IVFParameters(nlist: 2, nprobe: 2, kmeansIterations: 3)
        )
        let docs = try algorithmDocuments()
        let trainingVectors = try docs.map { try float32Elements(of: $0.embedding) }

        try await database.withTransaction { transaction in
            try await maintainer.train(vectors: trainingVectors, transaction: transaction)
            try await maintainer.scanItems(
                docs.map { (item: $0, id: Tuple($0.id)) },
                transaction: transaction
            )

            let listSubspace = context.indexSubspace.subspace(2)
            let (begin, end) = listSubspace.range()
            let entries = try await transaction.collectRange(
                from: .firstGreaterOrEqual(begin),
                to: .firstGreaterOrEqual(end),
                snapshot: true
            )
            guard let firstKey = entries.first?.0 else {
                throw VectorIndexError.invalidStructure("Expected IVF list entry")
            }
            try transaction.setValue([0x00], for: firstKey)
        }

        await #expect(throws: VectorIndexError.self) {
            _ = try await database.withTransaction { transaction in
                try await maintainer.search(queryVector: [1, 0, 0, 0], k: 2, transaction: transaction)
            }
        }
    }

    @Test("PQ stores vector payloads, compressed codes, and searches after training")
    func pqStoresVectorPayloadsAndCompressedCodesAndSearchesAfterTraining() async throws {
        let database = InMemoryEngine()
        let context = try makeContext(name: "pq")
        let maintainer = try PQIndexMaintainer<HNSWDocument>(
            index: context.index,
            dimensions: 4,
            metric: .euclidean,
            subspace: context.indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            parameters: PQParameters(m: 2, niter: 2)
        )
        let docs = try algorithmDocuments()

        try await database.withTransaction { transaction in
            try await maintainer.scanItems(
                docs.map { (item: $0, id: Tuple($0.id)) },
                transaction: transaction
            )
            try await maintainer.train(transaction: transaction)
        }

        let results = try await database.withTransaction { transaction in
            try await maintainer.search(queryVector: [1, 0, 0, 0], k: 2, transaction: transaction)
        }
        let firstId = results.first?.primaryKey.first as? String
        #expect(results.count == 2)
        #expect(firstId == "exact")

        let vectorPayloadLengths = try await collectValues(
            database: database,
            subspace: context.indexSubspace.subspace(3)
        ).map(\.count)
        let codeLengths = try await collectValues(
            database: database,
            subspace: context.indexSubspace.subspace(2)
        ).map(\.count)
        #expect(vectorPayloadLengths.allSatisfy { $0 == 16 })
        #expect(codeLengths.allSatisfy { $0 == 2 })
    }

    @Test("PQ rejects malformed compressed codes")
    func pqRejectsMalformedCompressedCodes() async throws {
        let database = InMemoryEngine()
        let context = try makeContext(name: "pq-corrupt")
        let maintainer = try PQIndexMaintainer<HNSWDocument>(
            index: context.index,
            dimensions: 4,
            metric: .euclidean,
            subspace: context.indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            parameters: PQParameters(m: 2, niter: 2)
        )
        let docs = try algorithmDocuments()

        try await database.withTransaction { transaction in
            try await maintainer.scanItems(
                docs.map { (item: $0, id: Tuple($0.id)) },
                transaction: transaction
            )
            try await maintainer.train(transaction: transaction)

            let codesSubspace = context.indexSubspace.subspace(2)
            let (begin, end) = codesSubspace.range()
            let entries = try await transaction.collectRange(
                from: .firstGreaterOrEqual(begin),
                to: .firstGreaterOrEqual(end),
                snapshot: true
            )
            guard let firstKey = entries.first?.0 else {
                throw VectorIndexError.invalidStructure("Expected PQ code entry")
            }
            try transaction.setValue([0x00], for: firstKey)
        }

        await #expect(throws: VectorIndexError.self) {
            _ = try await database.withTransaction { transaction in
                try await maintainer.search(queryVector: [1, 0, 0, 0], k: 2, transaction: transaction)
            }
        }
    }

    @Test("PQ retraining replaces every persisted compressed code")
    func pqRetrainingRebuildsPersistedCodes() async throws {
        let database = InMemoryEngine()
        let context = try makeContext(name: "pq-retraining")
        let maintainer = try PQIndexMaintainer<HNSWDocument>(
            index: context.index,
            dimensions: 4,
            metric: .euclidean,
            subspace: context.indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            parameters: PQParameters(m: 2, niter: 2)
        )
        let docs = try algorithmDocuments()
        let codesSubspace = context.indexSubspace.subspace(2)

        try await database.withTransaction { transaction in
            try await maintainer.scanItems(
                docs.map { (item: $0, id: Tuple($0.id)) },
                transaction: transaction
            )
            try await maintainer.train(transaction: transaction)
            try transaction.setValue(
                ByteString([0, 0]),
                for: codesSubspace.pack(Tuple("orphan"))
            )
            try await maintainer.train(transaction: transaction)
        }

        let codeCount = try await collectValues(
            database: database,
            subspace: codesSubspace
        ).count
        #expect(codeCount == docs.count)

        let results = try await database.withTransaction { transaction in
            try await maintainer.search(
                queryVector: [1, 0, 0, 0],
                k: docs.count,
                transaction: transaction
            )
        }
        #expect(results.count == docs.count)
    }

    @Test("PQ rejects non-contiguous persisted codebook keys")
    func pqRejectsNonContiguousCodebookKeys() async throws {
        let database = InMemoryEngine()
        let context = try makeContext(name: "pq-codebook-key-corrupt")
        let maintainer = try PQIndexMaintainer<HNSWDocument>(
            index: context.index,
            dimensions: 4,
            metric: .euclidean,
            subspace: context.indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            parameters: PQParameters(m: 2, niter: 2)
        )
        let docs = try algorithmDocuments()

        try await database.withTransaction { transaction in
            try await maintainer.scanItems(
                docs.map { (item: $0, id: Tuple($0.id)) },
                transaction: transaction
            )
            try await maintainer.train(transaction: transaction)
            let codebookSubspace = context.indexSubspace.subspace(0)
            let originalKey = codebookSubspace.pack(Tuple(0))
            let payload = try #require(
                try await transaction.getValue(for: originalKey)
            )
            try transaction.clear(key: originalKey)
            try transaction.setValue(
                payload,
                for: codebookSubspace.pack(Tuple(2))
            )
        }

        await #expect(throws: VectorIndexError.self) {
            _ = try await database.withTransaction { transaction in
                try await maintainer.search(
                    queryVector: [1, 0, 0, 0],
                    k: 1,
                    transaction: transaction
                )
            }
        }
    }

    private func makeContext(name: String
    ) throws -> (index: ResolvedIndex, indexSubspace: Subspace) {
        let testId = UUID().uuidString.prefix(8)
        let subspace = Subspace(prefix: Tuple("test", "vector-algorithm", name, String(testId)).pack())
        let indexSubspace = subspace.subspace("I").subspace("HNSWDocument_embedding")
        let index = try ResolvedIndex(
            for: HNSWDocument.self,
            name: "HNSWDocument_embedding",
            definition: vectorIndexDefinition(
                dimensions: 4, metric: .euclidean),
            rootExpression: FieldKeyExpression(fieldName: "embedding"),
            itemTypes: Set(["HNSWDocument"])
        )
        return (index, indexSubspace)
    }

    private func algorithmDocuments() throws -> [HNSWDocument] {
        [
            HNSWDocument(id: "exact", title: "Exact", embedding: try Vector(float32: [1, 0, 0, 0])),
            HNSWDocument(id: "near", title: "Near", embedding: try Vector(float32: [0.9, 0.1, 0, 0])),
            HNSWDocument(id: "orthogonal", title: "Orthogonal", embedding: try Vector(float32: [0, 1, 0, 0])),
            HNSWDocument(id: "opposite", title: "Opposite", embedding: try Vector(float32: [-1, 0, 0, 0])),
        ]
    }

    private func collectValues(
        database: InMemoryEngine,
        subspace: Subspace
    ) async throws -> [ByteString] {
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            let entries = try await transaction.collectRange(
                from: .firstGreaterOrEqual(begin),
                to: .firstGreaterOrEqual(end),
                snapshot: true
            )
            return entries.map(\.1)
        }
    }

    private func float32Elements(of vector: Vector) throws -> [Float] {
        guard let elements = vector.withFloat32Elements({ Array($0) }) else {
            throw VectorIndexError.invalidStructure(
                "IVF training requires Float32 vectors"
            )
        }
        return elements
    }
}
