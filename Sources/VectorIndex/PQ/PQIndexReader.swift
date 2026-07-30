import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit

enum PQIndexStorageKey: Int {
    case codebooks = 0
    case metadata = 1
    case codes = 2
    case vectors = 3
}

struct PQIndexReader: Sendable {
    private let subspace: Subspace
    private let dimensions: Int
    private let metric: VectorMetric
    private let parameters: PQParameters

    init(
        subspace: Subspace,
        dimensions: Int,
        metric: VectorMetric,
        parameters: PQParameters
    ) throws(VectorIndexError) {
        guard dimensions > 0 else {
            throw .invalidArgument("PQ vector dimensions must be positive")
        }
        guard dimensions % parameters.m == 0 else {
            throw .invalidArgument(
                "PQ vector dimensions \(dimensions) are not divisible by \(parameters.m) subquantizers"
            )
        }
        self.subspace = subspace
        self.dimensions = dimensions
        self.metric = metric
        self.parameters = parameters
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

        let codebooks = try await loadCodebooks(transaction: transaction)
        guard !codebooks.isEmpty else {
            throw VectorIndexError.invalidStructure("PQ index not trained")
        }

        let quantizer = try ProductQuantizer(
            dimensions: dimensions,
            codebooks: codebooks
        )
        let distanceTable = try quantizer.distanceTable(
            for: queryVector,
            metric: metric
        )

        var nearest = MinHeap<(primaryKey: [any TupleElement], distance: Double)>(
            maxSize: k,
            heapType: .max,
            comparator: { $0.distance > $1.distance }
        )
        let codesSubspace = subspace.subspace(PQIndexStorageKey.codes.rawValue)
        let (begin, end) = codesSubspace.range()
        let entries = try await TransactionRangeCollection.collect(
            using: transaction,
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            limit: 0,
            reverse: false,
            snapshot: true,
            streamingMode: .wantAll
        )

        for (key, code) in entries {
            let primaryKey: Tuple
            do {
                primaryKey = try codesSubspace.unpack(key)
            } catch {
                throw VectorIndexError.invalidStructure("Invalid PQ code primary key")
            }
            guard code.count == parameters.m else {
                throw VectorIndexError.invalidStructure("Invalid PQ code length")
            }
            nearest.insert(
                (
                    primaryKey: try primaryKey.elements(),
                    distance: try quantizer.distance(
                        for: code,
                        using: distanceTable
                    )
                )
            )
        }

        return nearest.sorted()
    }

    func loadCodebooks(
        transaction: any TransactionAccess
    ) async throws -> [[[Float]]] {
        let codebooksSubspace = subspace.subspace(
            PQIndexStorageKey.codebooks.rawValue
        )
        let (begin, end) = codebooksSubspace.range()
        let entries = try await TransactionRangeCollection.collect(
            using: transaction,
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            limit: 0,
            reverse: false,
            snapshot: true,
            streamingMode: .wantAll
        )

        let dimensionsPerSubquantizer = dimensions / parameters.m
        var codebooks: [[[Float]]] = []
        codebooks.reserveCapacity(entries.count)
        for (_, value) in entries {
            let flattened = try VectorConversion.decodeFloatArray(
                value,
                expectedCount: parameters.ksub * dimensionsPerSubquantizer
            )
            var centroids: [[Float]] = []
            centroids.reserveCapacity(parameters.ksub)
            for index in 0..<parameters.ksub {
                let start = index * dimensionsPerSubquantizer
                centroids.append(
                    Array(flattened[start..<(start + dimensionsPerSubquantizer)])
                )
            }
            codebooks.append(centroids)
        }

        guard codebooks.isEmpty || codebooks.count == parameters.m else {
            throw VectorIndexError.invalidStructure("Invalid PQ codebook count")
        }
        return codebooks
    }
}
