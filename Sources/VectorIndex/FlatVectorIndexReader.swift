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
        snapshot: Bool = true,
        workMeter: DatabaseWorkMeter
    ) async throws -> VectorRetainedMatches {
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
            snapshot: snapshot,
            streamingMode: .iterator
        )

        var nearest = try VectorSearchAccumulator(k: k, workMeter: workMeter)

        try await cursor.consume { key, value in
            try workMeter.consume(at: .indexScan)
            let vector = try VectorConversion.persistedVector(
                value,
                expectedCount: dimensions
            )
            let distance = try VectorConversion.distance(
                metric: metric,
                from: queryVector,
                to: vector
            )
            guard subspace.contains(key) else {
                throw VectorIndexError.invalidStructure(
                    "Invalid Flat vector primary key"
                )
            }
            try nearest.insert(
                packedPrimaryKey: key[
                    subspace.prefix.count..<key.count
                ],
                distance: distance
            )
        }

        return try nearest.finish()
    }
    /// Preserves the public maintainer result while keeping canonical readers
    /// on the retained-owner path.
    func search(
        queryVector: Vector,
        k: Int,
        transaction: any TransactionAccess
    ) async throws -> [(primaryKey: [any TupleElement], distance: Double)] {
        let workMeter = VectorRetainedMatches.makeUnboundedWorkMeter()
        let retained = try await search(
            queryVector: queryVector,
            k: k,
            transaction: transaction,
            snapshot: true,
            workMeter: workMeter
        )
        return try retained.promotedOutput()
    }

}
