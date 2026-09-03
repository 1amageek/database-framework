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
        entities: DatabaseRetainedPersistedModels,
        position: Int,
        transaction: any TransactionReadAccess,
        snapshot: Bool,
        workMeter: DatabaseWorkMeter
    ) async throws -> Bool {
        let persisted = try await persistedVector(
            primaryKey: primaryKey,
            transaction: transaction,
            snapshot: snapshot,
            workMeter: workMeter
        )
        guard let persisted else { return true }
        try workMeter.consume(UInt64(dimensions), at: .indexScan)
        let matches = try entities.withVectorField(
            at: position,
            keyPath: fieldName,
            workMeter: workMeter
        ) { field in
            try VectorConversion.matchesPersistedVector(
                persisted,
                field: field
            )
        }
        guard let matches else { return false }
        guard matches else {
            throw VectorIndexError.invalidStructure(
                "Vector index scoring state disagrees with the canonical entity"
            )
        }
        return true
    }

    func validate(
        primaryKey: Tuple,
        entities: borrowing DatabaseRetainedPolymorphicEntities,
        position: Int,
        transaction: any TransactionReadAccess,
        snapshot: Bool,
        workMeter: DatabaseWorkMeter
    ) async throws -> Bool {
        guard let persisted = try await persistedVector(
            primaryKey: primaryKey,
            transaction: transaction,
            snapshot: snapshot,
            workMeter: workMeter
        ) else {
            return true
        }
        try workMeter.consume(UInt64(dimensions), at: .indexScan)
        let matches = try entities.withVectorField(
            at: position,
            keyPath: fieldName,
            workMeter: workMeter
        ) { field in
            try VectorConversion.matchesPersistedVector(
                persisted,
                field: field
            )
        }
        guard let matches else {
            return false
        }
        guard matches else {
            throw VectorIndexError.invalidStructure(
                "Vector index scoring state disagrees with the canonical entity"
            )
        }
        return true
    }

    private func persistedVector(
        primaryKey: Tuple,
        transaction: any TransactionReadAccess,
        snapshot: Bool,
        workMeter: DatabaseWorkMeter
    ) async throws -> PersistedVectorView? {
        let persistedBytes: ByteString
        switch algorithm {
        case .hnsw:
            let labelsSubspace = indexSubspace.subspace("l")
            try workMeter.consume(at: .indexScan)
            guard let packedLabel = try await readPointValue(
                using: transaction,
                for: labelsSubspace.pack(primaryKey),
                snapshot: snapshot,
                workMeter: workMeter,
                at: .indexScan
            ) else {
                throw VectorIndexError.invalidStructure(
                    "HNSW result has no primary-key label mapping"
                )
            }
            let label = try HNSWLabelCodec.decodePacked(packedLabel)
            let vectorsSubspace = indexSubspace.subspace("v")
            try workMeter.consume(at: .indexScan)
            guard let value = try await readPointValue(
                using: transaction,
                for: vectorsSubspace.pack(HNSWLabelCodec.tuple(label)),
                snapshot: snapshot,
                workMeter: workMeter,
                at: .indexScan
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
            guard let value = try await readPointValue(
                using: transaction,
                for: vectorsSubspace.pack(primaryKey),
                snapshot: snapshot,
                workMeter: workMeter,
                at: .indexScan
            ) else {
                throw VectorIndexError.invalidStructure(
                    "PQ result has no persisted canonical vector"
                )
            }
            persistedBytes = value

        case .flat, .ivf:
            return nil
        }

        return try VectorConversion.persistedVector(
            persistedBytes,
            expectedCount: dimensions
        )
    }
}
