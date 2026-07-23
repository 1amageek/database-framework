// VectorAlgorithmMaintainerTests.swift
// Tests for non-default VectorIndex algorithm maintainers

import Testing
import TestHeartbeat
import Foundation
import StorageKit
import Core
import Vector
@testable import DatabaseEngine
@testable import VectorIndex

@Suite("Vector Algorithm Maintainer Tests", .serialized, .heartbeat)
struct VectorAlgorithmMaintainerTests {

    @Test("IVF stores contiguous Float32 vector payloads and returns nearest neighbors after training")
    func ivfStoresContiguousFloat32VectorPayloadsAndSearchesAfterTraining() async throws {
        let database = InMemoryEngine()
        let context = makeContext(name: "ivf")
        let maintainer = IVFIndexMaintainer<HNSWDocument>(
            index: context.index,
            dimensions: 4,
            metric: .euclidean,
            subspace: context.indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            parameters: IVFParameters(nlist: 2, nprobe: 2, kmeansIterations: 3)
        )
        let docs = algorithmDocuments()

        try await database.withTransaction { transaction in
            try await maintainer.train(vectors: docs.map(\.embedding), transaction: transaction)
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
        let context = makeContext(name: "ivf-corrupt")
        let maintainer = IVFIndexMaintainer<HNSWDocument>(
            index: context.index,
            dimensions: 4,
            metric: .euclidean,
            subspace: context.indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            parameters: IVFParameters(nlist: 2, nprobe: 2, kmeansIterations: 3)
        )
        let docs = algorithmDocuments()

        try await database.withTransaction { transaction in
            try await maintainer.train(vectors: docs.map(\.embedding), transaction: transaction)
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
        let context = makeContext(name: "pq")
        let maintainer = PQIndexMaintainer<HNSWDocument>(
            index: context.index,
            dimensions: 4,
            metric: .euclidean,
            subspace: context.indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            parameters: PQParameters(m: 2, niter: 2)
        )
        let docs = algorithmDocuments()

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
        let context = makeContext(name: "pq-corrupt")
        let maintainer = PQIndexMaintainer<HNSWDocument>(
            index: context.index,
            dimensions: 4,
            metric: .euclidean,
            subspace: context.indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            parameters: PQParameters(m: 2, niter: 2)
        )
        let docs = algorithmDocuments()

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

    private func makeContext(name: String) -> (index: Index, indexSubspace: Subspace) {
        let testId = UUID().uuidString.prefix(8)
        let subspace = Subspace(prefix: Tuple("test", "vector-algorithm", name, String(testId)).pack())
        let indexSubspace = subspace.subspace("I").subspace("HNSWDocument_embedding")
        let kind = VectorIndexKind<HNSWDocument>(embedding: \.embedding, dimensions: 4, metric: .euclidean)
        let index = Index(
            name: "HNSWDocument_embedding",
            kind: kind,
            rootExpression: FieldKeyExpression(fieldName: "embedding"),
            subspaceKey: "HNSWDocument_embedding",
            itemTypes: Set(["HNSWDocument"])
        )
        return (index, indexSubspace)
    }

    private func algorithmDocuments() -> [HNSWDocument] {
        [
            HNSWDocument(id: "exact", title: "Exact", embedding: [1, 0, 0, 0]),
            HNSWDocument(id: "near", title: "Near", embedding: [0.9, 0.1, 0, 0]),
            HNSWDocument(id: "orthogonal", title: "Orthogonal", embedding: [0, 1, 0, 0]),
            HNSWDocument(id: "opposite", title: "Opposite", embedding: [-1, 0, 0, 0])
        ]
    }

    private func collectValues(
        database: InMemoryEngine,
        subspace: Subspace
    ) async throws -> [Bytes] {
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
}
