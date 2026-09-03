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

        let metadata = try await loadMetadata(
            transaction: transaction,
            snapshot: snapshot,
            workMeter: workMeter
        )
        if metadata.vectorCount == 0 {
            guard try await !hasStoredVector(
                transaction: transaction,
                snapshot: snapshot,
                workMeter: workMeter
            ) else {
                throw VectorIndexError.invalidStructure(
                    "IVF metadata reports an empty index with persisted vectors"
                )
            }
            return try VectorSearchAccumulator(
                k: k,
                workMeter: workMeter
            ).finish()
        }

        let centroids = try await loadRetainedCentroids(
            transaction: transaction,
            snapshot: snapshot,
            workMeter: workMeter
        )
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
                snapshot: snapshot,
                workMeter: workMeter
            )
        }
        guard !centroids.isEmpty else {
            throw VectorIndexError.invalidStructure("IVF index not trained")
        }

        let centroidDistanceReservation = try workMeter.reserveIntermediate(
            rows: UInt64(centroids.count),
            bytes: DatabaseIntermediateCollectionMeter.arrayFootprint(
                count: centroids.count,
                element: (index: Int, distance: Double).self
            ).bytes,
            at: .indexScan
        )
        defer { centroidDistanceReservation.release() }
        var centroidDistances: [(index: Int, distance: Double)] = []
        centroidDistances.reserveCapacity(centroids.count)
        for (index, centroid) in centroids.enumerated() {
            try workMeter.consume(at: .indexScan)
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
        var nearest = try VectorSearchAccumulator(k: k, workMeter: workMeter)

        for centroid in centroidDistances.prefix(parameters.nprobe) {
            let clusterID = centroid.index
            let listSubspace = subspace
                .subspace(IVFIndexStorageKey.lists.rawValue)
                .subspace(clusterID)
            let (begin, end) = listSubspace.range()
            var cursor = transaction.rangeCursor(
                from: .firstGreaterOrEqual(begin),
                to: .firstGreaterOrEqual(end),
                limit: 0,
                reverse: false,
                snapshot: snapshot,
                streamingMode: .iterator
            )

            try await cursor.consume { key, value in
                try workMeter.consume(at: .indexScan)
                guard listSubspace.contains(key) else {
                    throw VectorIndexError.invalidStructure(
                        "Invalid IVF list primary key"
                    )
                }
                let vector = try VectorConversion.persistedVector(
                    value,
                    expectedCount: dimensions
                )
                try nearest.insert(
                    packedPrimaryKey: key[
                        listSubspace.prefix.count..<key.count
                    ],
                    distance: try VectorConversion.distance(
                        metric: metric,
                        from: queryVector,
                        to: vector
                    )
                )
            }
        }

        return try nearest.finish()
    }

    private func loadMetadata(
        transaction: any TransactionReadAccess,
        snapshot: Bool,
        workMeter: DatabaseWorkMeter
    ) async throws -> IVFMetadata {
        let key = subspace.pack(Tuple([IVFIndexStorageKey.metadata.rawValue]))
        try workMeter.consume(at: .indexScan)
        guard let value = try await readPointValue(
            using: transaction,
            for: key,
            snapshot: snapshot,
            workMeter: workMeter,
            at: .indexScan
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
        snapshot: Bool,
        workMeter: DatabaseWorkMeter
    ) async throws -> VectorRetainedMatches {
        let listsSubspace = subspace.subspace(IVFIndexStorageKey.lists.rawValue)
        let (begin, end) = listsSubspace.range()
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
            let vector = try VectorConversion.persistedVector(
                value,
                expectedCount: dimensions
            )
            try nearest.insert(
                packedPrimaryKey: key[
                    listsSubspace.prefix.count..<key.count
                ],
                droppingFirstElement: true,
                distance: try VectorConversion.distance(
                    metric: metric,
                    from: queryVector,
                    to: vector
                )
            )
        }
        return try nearest.finish()
    }

    private func hasStoredVector(
        transaction: any TransactionReadAccess,
        snapshot: Bool,
        workMeter: DatabaseWorkMeter
    ) async throws -> Bool {
        let range = subspace.subspace(IVFIndexStorageKey.lists.rawValue).range()
        var cursor = transaction.rangeCursor(
            from: .firstGreaterOrEqual(range.begin),
            to: .firstGreaterOrEqual(range.end),
            limit: 1,
            reverse: false,
            snapshot: snapshot,
            streamingMode: .iterator
        )
        var found = false
        try await cursor.consume { _, _ in
            try workMeter.consume(at: .indexScan)
            found = true
        }
        return found
    }

    private func loadRetainedCentroids(
        transaction: any TransactionReadAccess,
        snapshot: Bool,
        workMeter: DatabaseWorkMeter
    ) async throws -> DatabaseSharedRetainedArray<PersistedVectorView> {
        let centroidSubspace = subspace.subspace(
            IVFIndexStorageKey.centroids.rawValue
        )
        let range = centroidSubspace.range()
        var cursor = transaction.rangeCursor(
            from: .firstGreaterOrEqual(range.begin),
            to: .firstGreaterOrEqual(range.end),
            limit: 0,
            reverse: false,
            snapshot: snapshot,
            streamingMode: .iterator
        )
        var builder = try DatabaseRetainedArrayBuilder<PersistedVectorView>(
            workMeter: workMeter,
            stage: .indexScan,
            layout: try DatabaseRetainedArrayLayout.forElement(
                PersistedVectorView.self
            ),
            expectedCount: parameters.nlist
        )
        var expectedIndex = 0
        try await cursor.consume { key, value in
            try workMeter.consume(at: .indexScan)
            let keyTuple: Tuple
            do {
                keyTuple = try centroidSubspace.unpack(key)
            } catch {
                throw VectorIndexError.invalidStructure(
                    "Invalid IVF centroid key sequence"
                )
            }
            guard keyTuple.count == 1,
                  case .signedInteger(let encodedIndex) =
                    try keyTuple.value(at: 0),
                  Int(exactly: encodedIndex) == expectedIndex else {
                throw VectorIndexError.invalidStructure(
                    "Invalid IVF centroid key sequence"
                )
            }
            let admission = try builder.prepareAppend(
                footprint: DatabaseIntermediateFootprint(rows: 1),
                at: .indexScan
            )
            let payloadReservation = try workMeter.reserveIntermediate(
                bytes: UInt64(value.count),
                at: .indexScan
            )
            do {
                let retained = try DatabaseRetainedByteString.make(
                    value,
                    reservation: payloadReservation,
                    at: .indexScan
                )
                let centroid = try VectorConversion.persistedVector(
                    retained,
                    expectedCount: dimensions
                )
                builder.append(centroid, using: admission)
            } catch {
                payloadReservation.release()
                throw error
            }
            expectedIndex += 1
        }
        guard builder.count <= parameters.nlist else {
            throw VectorIndexError.invalidStructure(
                "IVF centroid count exceeds the configured cluster count"
            )
        }
        return try builder.finish().moveToSharedOwnership(at: .indexScan)
    }

    func loadCentroids(
        transaction: any TransactionReadAccess,
        snapshot: Bool = true,
        workMeter: DatabaseWorkMeter? = nil
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
            snapshot: snapshot,
            streamingMode: .iterator
        )

        let reservation = try workMeter.map { meter in
            try meter.reserveIntermediate(
                rows: UInt64(parameters.nlist),
                bytes: DatabaseIntermediateCollectionMeter.arrayFootprint(
                    count: parameters.nlist,
                    element: PersistedVectorView.self
                ).bytes,
                at: .indexScan
            )
        }
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
            let retainedValue: ByteString
            if let reservation {
                try reservation.reserveAdditional(
                    bytes: UInt64(value.count) + 32,
                    at: .indexScan
                )
                retainedValue = try DatabaseRetainedByteString.make(
                    value,
                    reservation: reservation,
                    at: .indexScan
                )
            } else {
                retainedValue = value
            }
            centroids.append(
                try VectorConversion.persistedVector(
                    retainedValue,
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
    /// Preserves the public maintainer result at its explicit output boundary.
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
