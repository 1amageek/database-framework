import DatabaseEngine
import DatabaseTypes
import StorageKit
import SwiftHNSW

struct HNSWIndexReader: Sendable {
    private let storage: HNSWIndexStorage

    init(storage: HNSWIndexStorage) {
        self.storage = storage
    }

    func search(
        queryVector: Vector,
        k: Int,
        parameters: HNSWSearchParameters,
        transaction: any TransactionReadAccess,
        snapshot: Bool = true,
        workMeter: DatabaseWorkMeter
    ) async throws -> VectorRetainedMatches {
        guard queryVector.count == storage.dimensions else {
            throw VectorIndexError.dimensionMismatch(
                expected: storage.dimensions,
                actual: queryVector.count
            )
        }
        guard k > 0 else {
            throw VectorIndexError.invalidArgument("k must be positive")
        }
        guard parameters.ef > 0 else {
            throw VectorIndexError.invalidArgument("ef must be positive")
        }

        let graphQueryVector = try storage.retainedGraphVector(
            from: queryVector,
            workMeter: workMeter
        )
        let graphSnapshot = try await storage.loadSearchSnapshot(
            transaction: transaction,
            snapshot: snapshot,
            workMeter: workMeter
        )
        let libraryResultReservation = try workMeter.reserveIntermediate(
            rows: UInt64(k),
            bytes: try DatabaseIntermediateFootprint(bytes: 32)
                .multiplied(by: UInt64(k)).bytes,
            at: .indexScan
        )
        defer { libraryResultReservation.release() }
        let results = try graphQueryVector.search(
            in: graphSnapshot,
            k: k,
            efSearch: parameters.ef,
            workMeter: workMeter
        )

        var output = try VectorSearchAccumulator(k: k, workMeter: workMeter)
        for result in results {
            let distance = try await storage.validateSearchDistance(
                result.distance,
                label: result.label,
                queryVector: queryVector,
                transaction: transaction,
                snapshot: snapshot,
                workMeter: workMeter
            )
            guard try graphSnapshot.insertMatch(
                label: result.label,
                distance: distance,
                into: &output
            ) else {
                throw VectorIndexError.invalidStructure(
                    "HNSW search result references an unknown primary-key label"
                )
            }
        }
        return try output.finish()
    }
    /// Preserves the public maintainer result while keeping canonical readers
    /// on the retained-owner path.
    func search(
        queryVector: Vector,
        k: Int,
        parameters: HNSWSearchParameters,
        transaction: any TransactionAccess
    ) async throws -> [(primaryKey: [any TupleElement], distance: Double)] {
        let workMeter = VectorRetainedMatches.makeUnboundedWorkMeter()
        let retained = try await search(
            queryVector: queryVector,
            k: k,
            parameters: parameters,
            transaction: transaction,
            snapshot: true,
            workMeter: workMeter
        )
        return try retained.promotedOutput()
    }

}
