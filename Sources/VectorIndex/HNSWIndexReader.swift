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
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter? = nil
    ) async throws -> [(primaryKey: [any TupleElement], distance: Double)] {
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

        let graphQueryVector = try storage.graphVector(from: queryVector)
        let snapshot = try await storage.loadSearchSnapshot(
            transaction: transaction,
            workMeter: workMeter
        )
        let results = try snapshot.search(
            queryVector: graphQueryVector,
            k: k,
            efSearch: parameters.ef,
            workMeter: workMeter
        )

        var output: [(primaryKey: [any TupleElement], distance: Double)] = []
        output.reserveCapacity(results.count)
        for result in results {
            guard let primaryKey = snapshot.primaryKeysByLabel[result.label] else {
                throw VectorIndexError.invalidStructure(
                    "HNSW search result references an unknown primary-key label"
                )
            }
            output.append(
                (
                    primaryKey: try Tuple.unpack(from: primaryKey),
                    distance: try await storage.validateSearchDistance(
                        result.distance,
                        label: result.label,
                        queryVector: queryVector,
                        transaction: transaction,
                        workMeter: workMeter
                    )
                )
            )
        }
        return output
    }
}
