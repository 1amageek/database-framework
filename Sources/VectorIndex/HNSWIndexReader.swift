import StorageKit

struct HNSWIndexReader: Sendable {
    private let storage: HNSWIndexStorage

    init(storage: HNSWIndexStorage) {
        self.storage = storage
    }

    func search(
        queryVector: [Float],
        k: Int,
        parameters: HNSWSearchParameters,
        transaction: any TransactionAccess
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

        let snapshot = try await storage.loadSearchSnapshot(
            transaction: transaction
        )
        let results = try snapshot.search(
            queryVector: queryVector,
            k: k,
            efSearch: parameters.ef
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
                    primaryKey: try primaryKey.elements(),
                    distance: Double(result.distance)
                )
            )
        }
        return output
    }
}
