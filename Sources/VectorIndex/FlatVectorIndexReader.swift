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
        queryVector: [Float],
        k: Int,
        transaction: any TransactionAccess
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
        let entries = try await TransactionRangeCollection.collect(
            using: transaction,
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            limit: 0,
            reverse: false,
            snapshot: true,
            streamingMode: .wantAll
        )

        var nearest = MinHeap<(primaryKey: [any TupleElement], distance: Double)>(
            maxSize: k,
            heapType: .max,
            comparator: { $0.distance > $1.distance }
        )

        for (key, value) in entries {
            let primaryKey: Tuple
            do {
                primaryKey = try subspace.unpack(key)
            } catch {
                throw VectorIndexError.invalidStructure("Invalid Flat vector primary key")
            }
            let vector = try VectorConversion.decodeFloatArray(
                value,
                expectedCount: dimensions
            )
            nearest.insert(
                (
                    primaryKey: try primaryKey.elements(),
                    distance: distance(from: queryVector, to: vector)
                )
            )
        }

        return nearest.sorted()
    }

    private func distance(from query: [Float], to candidate: [Float]) -> Double {
        switch metric {
        case .cosine:
            return VectorConversion.cosineDistance(query, candidate)
        case .euclidean:
            return VectorConversion.euclideanDistance(query, candidate)
        case .dotProduct:
            return VectorConversion.dotProductDistance(query, candidate)
        }
    }
}
