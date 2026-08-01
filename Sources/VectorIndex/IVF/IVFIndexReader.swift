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
        queryVector: Vector,
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

        var centroidDistances: [(index: Int, distance: Double)] = []
        centroidDistances.reserveCapacity(centroids.count)
        for (index, centroid) in centroids.enumerated() {
            centroidDistances.append(
                (
                    index: index,
                    distance: try VectorConversion.distance(
                        metric: .euclidean,
                        from: queryVector,
                        to: centroid
                    )
                )
            )
        }
        centroidDistances.sort { $0.distance < $1.distance }
        let nearestClusters = centroidDistances
            .prefix(parameters.nprobe)
            .map { $0.index }

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
        }

        return nearest.sorted()
    }

    func loadCentroids(
        transaction: any TransactionAccess
    ) async throws -> [PersistedVectorView] {
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

        var centroids: [PersistedVectorView] = []
        centroids.reserveCapacity(entries.count)
        for (expectedIndex, entry) in entries.enumerated() {
            let (key, value) = entry
            do {
                let keyTuple = try centroidSubspace.unpack(key)
                guard keyTuple.count == 1,
                      case .signedInteger(let encodedIndex) = try keyTuple.value(at: 0),
                      Int(exactly: encodedIndex) == expectedIndex else {
                    throw VectorIndexError.invalidStructure(
                        "Invalid IVF centroid key sequence"
                    )
                }
            } catch let error as VectorIndexError {
                throw error
            } catch {
                throw VectorIndexError.invalidStructure(
                    "Invalid IVF centroid key sequence"
                )
            }
            centroids.append(
                try VectorConversion.persistedVector(
                    value,
                    expectedCount: dimensions
                )
            )
        }
        guard centroids.count <= parameters.nlist else {
            throw VectorIndexError.invalidStructure(
                "IVF centroid count exceeds the configured cluster count"
            )
        }
        return centroids
    }
}
