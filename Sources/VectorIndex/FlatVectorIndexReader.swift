import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit

struct FlatVectorIndexReader: Sendable {
    private let subspace: Subspace
    private let dimensions: Int
    private let metric: VectorMetric

    init(
        subspace: Subspace,
        dimensions: Int,
        metric: VectorMetric
    ) {
        self.subspace = subspace
        self.dimensions = dimensions
        self.metric = metric
    }

    func search(
        queryVector: Vector,
        k: Int,
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter? = nil
    ) async throws -> [(primaryKey: [any TupleElement], distance: Double)] {
        guard queryVector.count == dimensions else {
            throw VectorIndexError.dimensionMismatch(
                expected: dimensions,
                actual: queryVector.count
            )
        }
        guard k > 0 else {
            throw VectorIndexError.invalidArgument("k must be positive")
        }

        let (begin, end) = subspace.range()
        var cursor = transaction.rangeCursor(
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            limit: 0,
            reverse: false,
            snapshot: true,
            streamingMode: .iterator
        )

        var nearest = MinHeap<(primaryKey: [any TupleElement], distance: Double)>(
            maxSize: k,
            heapType: .max,
            comparator: VectorSearchResultOrdering.isWorse
        )

        try await cursor.consume { key, value in
            try workMeter?.consume(at: .indexScan)
            let primaryKey: Tuple
            do {
                primaryKey = try subspace.unpack(key)
            } catch {
                throw VectorIndexError.invalidStructure("Invalid Flat vector primary key")
            }
            let vector = try VectorConversion.persistedVector(
                value,
                expectedCount: dimensions
            )
            nearest.insert(
                (
                    primaryKey: try primaryKey.elements(),
                    distance: try VectorConversion.distance(
                        metric: metric,
                        from: queryVector,
                        to: vector
                    )
                )
            )
        }

        return nearest.sorted()
    }
}
