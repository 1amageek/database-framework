import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit

enum IVFIndexStorageKey: Int {
    case centroids = 0
    case metadata = 1
    case lists = 2
    case assignments = 3
}

struct IVFIndexReader: Sendable {
    private let subspace: Subspace
    private let dimensions: Int
    private let metric: VectorMetric
    private let parameters: IVFParameters

    init(
        subspace: Subspace,
        dimensions: Int,
        metric: VectorMetric,
        parameters: IVFParameters
    ) {
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

        let centroids = try await loadCentroids(transaction: transaction)
        guard !centroids.isEmpty else {
            throw VectorIndexError.invalidStructure("IVF index not trained")
        }

        let clustering = KMeansClustering(
            k: parameters.nlist,
            dimensions: dimensions
        )
        let nearestClusters = clustering.findNearestCentroids(
            query: queryVector,
            centroids: centroids,
            nprobe: parameters.nprobe
        )

        var nearest = MinHeap<(primaryKey: [any TupleElement], distance: Double)>(
            maxSize: k,
            heapType: .max,
            comparator: { $0.distance > $1.distance }
        )

        for clusterID in nearestClusters {
            let listSubspace = subspace
                .subspace(IVFIndexStorageKey.lists.rawValue)
                .subspace(clusterID)
            let (begin, end) = listSubspace.range()
            let entries = try await TransactionRangeCollection.collect(
                using: transaction,
                from: .firstGreaterOrEqual(begin),
                to: .firstGreaterOrEqual(end),
                limit: 0,
                reverse: false,
                snapshot: true,
                streamingMode: .wantAll
            )

            for (key, value) in entries {
                let primaryKey: Tuple
                do {
                    primaryKey = try listSubspace.unpack(key)
                } catch {
                    throw VectorIndexError.invalidStructure(
                        "Invalid IVF list primary key"
                    )
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
        }

        return nearest.sorted()
    }

    func loadCentroids(
        transaction: any TransactionAccess
    ) async throws -> [[Float]] {
        let centroidSubspace = subspace.subspace(
            IVFIndexStorageKey.centroids.rawValue
        )
        let (begin, end) = centroidSubspace.range()
        let entries = try await TransactionRangeCollection.collect(
            using: transaction,
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            limit: 0,
            reverse: false,
            snapshot: true,
            streamingMode: .wantAll
        )

        var centroids: [[Float]] = []
        centroids.reserveCapacity(entries.count)
        for (_, value) in entries {
            centroids.append(
                try VectorConversion.decodeFloatArray(
                    value,
                    expectedCount: dimensions
                )
            )
        }
        return centroids
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
