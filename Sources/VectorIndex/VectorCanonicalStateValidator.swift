import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit

/// Rejects approximate-index results whose persisted scoring state no longer
/// agrees with the canonical entity returned by the same transaction snapshot.
struct VectorCanonicalStateValidator: Sendable {
    let indexSubspace: Subspace
    let fieldName: String
    let dimensions: Int
    let algorithm: VectorAlgorithm

    func validate(
        primaryKey: Tuple,
        model: PersistedModel,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter
    ) async throws {
        let persistedBytes: ByteString
        switch algorithm {
        case .hnsw:
            let labelsSubspace = indexSubspace.subspace("l")
            try workMeter.consume(at: .indexScan)
            guard let packedLabel = try await transaction.getValue(
                for: labelsSubspace.pack(primaryKey),
                snapshot: true
            ) else {
                throw VectorIndexError.invalidStructure(
                    "HNSW result has no primary-key label mapping"
                )
            }
            let label = try HNSWLabelCodec.decodePacked(packedLabel)
            let vectorsSubspace = indexSubspace.subspace("v")
            try workMeter.consume(at: .indexScan)
            guard let value = try await transaction.getValue(
                for: vectorsSubspace.pack(HNSWLabelCodec.tuple(label)),
                snapshot: true
            ) else {
                throw VectorIndexError.invalidStructure(
                    "HNSW result has no persisted canonical vector"
                )
            }
            persistedBytes = value

        case .pq:
            let vectorsSubspace = indexSubspace.subspace(
                PQIndexStorageKey.vectors.rawValue
            )
            try workMeter.consume(at: .indexScan)
            guard let value = try await transaction.getValue(
                for: vectorsSubspace.pack(primaryKey),
                snapshot: true
            ) else {
                throw VectorIndexError.invalidStructure(
                    "PQ result has no persisted canonical vector"
                )
            }
            persistedBytes = value

        case .flat, .ivf:
            return
        }

        let persisted = try VectorConversion.persistedVector(
            persistedBytes,
            expectedCount: dimensions
        )
        try workMeter.consume(UInt64(dimensions), at: .indexScan)
        let fieldValue = try DataAccess.extractFieldValue(
            from: model,
            keyPath: fieldName
        )
        guard try VectorConversion.matchesPersistedVector(
            persisted,
            fieldValue: fieldValue
        ) else {
            throw VectorIndexError.invalidStructure(
                "Vector index scoring state disagrees with the canonical entity"
            )
        }
    }
}
