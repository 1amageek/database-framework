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

        let metadata = try await loadMetadata(transaction: transaction)
        if metadata.vectorCount == 0 {
            guard try await !hasStoredVector(transaction: transaction) else {
                throw VectorIndexError.invalidStructure(
                    "IVF metadata reports an empty index with persisted vectors"
                )
            }
            return []
        }

        let centroids = try await loadCentroids(transaction: transaction)
        if !metadata.trained {
            guard centroids.isEmpty else {
                throw VectorIndexError.invalidStructure(
                    "IVF untrained metadata has persisted centroids"
                )
            }
            return try await exactSearch(
                queryVector: queryVector,
                k: k,
                transaction: transaction,
                workMeter: workMeter
            )
        }
        guard !centroids.isEmpty else {
            throw VectorIndexError.invalidStructure("IVF index not trained")
        }

        var centroidDistances: [(index: Int, distance: Double)] = []
        centroidDistances.reserveCapacity(centroids.count)
        for (index, centroid) in centroids.enumerated() {
            try workMeter?.consume(at: .indexScan)
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
        centroidDistances.sort {
            if $0.distance == $1.distance { return $0.index < $1.index }
            return $0.distance < $1.distance
        }
        let nearestClusters = centroidDistances
            .prefix(parameters.nprobe)
            .map { $0.index }

        var nearest = MinHeap<(primaryKey: [any TupleElement], distance: Double)>(
            maxSize: k,
            heapType: .max,
            comparator: VectorSearchResultOrdering.isWorse
        )

        for clusterID in nearestClusters {
            let listSubspace = subspace
                .subspace(IVFIndexStorageKey.lists.rawValue)
                .subspace(clusterID)
            let (begin, end) = listSubspace.range()
            var cursor = transaction.rangeCursor(
                from: .firstGreaterOrEqual(begin),
                to: .firstGreaterOrEqual(end),
                limit: 0,
                reverse: false,
                snapshot: true,
                streamingMode: .iterator
            )

            try await cursor.consume { key, value in
                try workMeter?.consume(at: .indexScan)
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

    private func loadMetadata(
        transaction: any TransactionReadAccess
    ) async throws -> IVFMetadata {
        let key = subspace.pack(Tuple([IVFIndexStorageKey.metadata.rawValue]))
        guard let value = try await transaction.getValue(
            for: key,
            snapshot: true
        ) else {
            throw VectorIndexError.invalidStructure("IVF metadata is missing")
        }
        do {
            let metadata = try IVFMetadata(packed: value)
            guard metadata.nlist == parameters.nlist,
                  metadata.dimensions == dimensions else {
                throw VectorIndexError.invalidStructure(
                    "IVF metadata does not match the configured index"
                )
            }
            return metadata
        } catch let error as VectorIndexError {
            throw error
        } catch {
            throw VectorIndexError.invalidStructure("Invalid IVF metadata")
        }
    }

    private func exactSearch(
        queryVector: Vector,
        k: Int,
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter? = nil
    ) async throws -> [(primaryKey: [any TupleElement], distance: Double)] {
        let listsSubspace = subspace.subspace(IVFIndexStorageKey.lists.rawValue)
        let (begin, end) = listsSubspace.range()
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
            let tuple: Tuple
            do {
                tuple = try listsSubspace.unpack(key)
            } catch {
                throw VectorIndexError.invalidStructure(
                    "Invalid IVF list primary key"
                )
            }
            guard tuple.count >= 2,
                  case .signedInteger(let encodedCluster) = try tuple.value(at: 0),
                  let cluster = Int(exactly: encodedCluster),
                  cluster >= 0,
                  cluster < parameters.nlist else {
                throw VectorIndexError.invalidStructure(
                    "Invalid IVF list cluster key"
                )
            }
            let elements = try tuple.elements()
            let vector = try VectorConversion.persistedVector(
                value,
                expectedCount: dimensions
            )
            nearest.insert(
                (
                    primaryKey: Array(elements.dropFirst()),
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

    private func hasStoredVector(
        transaction: any TransactionReadAccess
    ) async throws -> Bool {
        let range = subspace.subspace(IVFIndexStorageKey.lists.rawValue).range()
        var cursor = transaction.rangeCursor(
            from: .firstGreaterOrEqual(range.begin),
            to: .firstGreaterOrEqual(range.end),
            limit: 1,
            reverse: false,
            snapshot: true,
            streamingMode: .iterator
        )
        let row = try await cursor.next()
        try await cursor.finish()
        return row != nil
    }

    func loadCentroids(
        transaction: any TransactionReadAccess
    ) async throws -> [PersistedVectorView] {
        let centroidSubspace = subspace.subspace(
            IVFIndexStorageKey.centroids.rawValue
        )
        let (begin, end) = centroidSubspace.range()
        var cursor = transaction.rangeCursor(
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            limit: 0,
            reverse: false,
            snapshot: true,
            streamingMode: .iterator
        )

        var centroids: [PersistedVectorView] = []
        var expectedIndex = 0
        try await cursor.consume { key, value in
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
            expectedIndex += 1
        }
        guard centroids.count <= parameters.nlist else {
            throw VectorIndexError.invalidStructure(
                "IVF centroid count exceeds the configured cluster count"
            )
        }
        return centroids
    }
}
