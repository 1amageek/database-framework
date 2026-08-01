// IVFIndexMaintainer.swift
// VectorIndex - IVF (Inverted File Index) maintainer for vector indexes
//
// Reference: Jégou et al., "Product Quantization for Nearest Neighbor Search",
// IEEE Transactions on Pattern Analysis and Machine Intelligence, 2011

import DatabaseTypes
import DatabaseKit
import DatabaseEngine
import StorageKit

/// Maintainer for IVF (Inverted File Index) vector indexes
///
/// **Algorithm**:
/// IVF partitions the vector space into nlist clusters using K-means.
/// Each vector is assigned to its nearest cluster centroid.
/// At query time, only nprobe nearest clusters are searched.
///
/// **Storage Layout**:
/// ```
/// [subspace]/centroids/[clusterId] = Float32 binary payload, little-endian
/// [subspace]/metadata = JSON { nlist, dimensions, trained, vectorCount }
/// [subspace]/lists/[clusterId]/[primaryKey] = Float32 binary payload, little-endian
/// [subspace]/assignments/[primaryKey] = Int64(clusterId)
/// ```
///
/// **Performance**:
/// - Training: O(n × k × d × iterations)
/// - Insert: O(k) for centroid lookup + O(1) storage
/// - Query: O(k × d + nprobe × n/k × d)
///
/// **Usage**:
/// ```swift
/// let maintainer = IVFIndexMaintainer<Product>(
///     index: vectorIndex,
///     dimensions: 384,
///     metric: .cosine,
///     subspace: vectorSubspace,
///     idExpression: FieldKeyExpression(fieldName: "id"),
///     parameters: IVFParameters(nlist: 100, nprobe: 10)
/// )
/// ```
public struct IVFIndexMaintainer<Item: Persistable>: IndexMaintainer {
    private struct StoredListEntry: Sendable {
        let primaryKey: Tuple
        let payload: ByteString
        let vector: PersistedVectorView
    }

    private struct ReassignedListEntry: Sendable {
        let primaryKey: Tuple
        let payload: ByteString
        let clusterID: Int
    }

    // MARK: - Properties

    public let index: Index
    public let subspace: Subspace
    public let idExpression: KeyExpression

    private let dimensions: Int
    private let metric: VectorMetric
    private let parameters: IVFParameters

    // MARK: - Initialization

    /// Create IVF index maintainer
    ///
    /// - Parameters:
    ///   - index: Index definition
    ///   - dimensions: Vector dimensions
    ///   - metric: Distance metric
    ///   - subspace: FDB subspace for this index
    ///   - idExpression: Expression for extracting item's unique identifier
    ///   - parameters: IVF algorithm parameters
    public init(
        index: Index,
        dimensions: Int,
        metric: VectorMetric,
        subspace: Subspace,
        idExpression: KeyExpression,
        parameters: IVFParameters
    ) {
        self.index = index
        self.dimensions = dimensions
        self.metric = metric
        self.subspace = subspace
        self.idExpression = idExpression
        self.parameters = parameters
    }

    // MARK: - IndexMaintainer

    public func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionAccess
    ) async throws {
        // Remove old vector from inverted list
        if let oldItem = oldItem {
            do {
                let oldId = try DataAccess.extractId(from: oldItem, using: idExpression)
                try await removeFromInvertedList(id: oldId, transaction: transaction)
            } catch DataAccessError.nilValueCannotBeIndexed {
                // Sparse index: nil vector was not indexed
            }
        }

        // Add new vector to inverted list
        if let newItem = newItem {
            do {
                let newId = try DataAccess.extractId(from: newItem, using: idExpression)
                let vector = try extractVector(from: newItem)
                try await addToInvertedList(id: newId, vector: vector, item: newItem, transaction: transaction)
            } catch DataAccessError.nilValueCannotBeIndexed {
                // Sparse index: nil vector is not indexed
            }
        }
    }

    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionAccess
    ) async throws {
        do {
            let vector = try extractVector(from: item)
            try await addToInvertedList(id: id, vector: vector, item: item, transaction: transaction)
        } catch DataAccessError.nilValueCannotBeIndexed {
            // Sparse index: nil vector is not indexed
        }
    }

    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [ByteString] {
        // IVF stores data in inverted lists, not directly by primary key
        // Return the assignment key for verification
        let assignmentSubspace = subspace.subspace(IVFIndexStorageKey.assignments.rawValue)
        return [assignmentSubspace.pack(id)]
    }

    // MARK: - Training

    /// Train centroids using K-means clustering
    ///
    /// Should be called before the index is used for queries.
    /// Typically done during batch index building.
    ///
    /// - Parameters:
    ///   - vectors: Training vectors
    ///   - transaction: FDB transaction
    public func train(
        vectors: [[Float]],
        transaction: any TransactionAccess
    ) async throws {
        guard dimensions > 0 else {
            throw VectorIndexError.invalidArgument(
                "IVF vector dimensions must be positive"
            )
        }
        guard !vectors.isEmpty else {
            throw VectorIndexError.invalidArgument(
                "IVF training requires at least one vector"
            )
        }
        for (vectorIndex, vector) in vectors.enumerated() {
            guard vector.count == dimensions else {
                throw VectorIndexError.dimensionMismatch(
                    expected: dimensions,
                    actual: vector.count
                )
            }
            guard vector.allSatisfy({ $0.isFinite }) else {
                throw VectorIndexError.invalidArgument(
                    "IVF training vector \(vectorIndex) contains a non-finite element"
                )
            }
        }

        let clustering = KMeansClustering(
            k: parameters.nlist,
            dimensions: dimensions,
            maxIterations: parameters.kmeansIterations
        )

        let centroids = clustering.train(vectors: vectors)
        let storedEntries = try await loadStoredListEntries(
            transaction: transaction
        )
        let centroidVectors = try centroids.map { centroid in
            try Vector(float32: centroid)
        }
        let reassignedEntries = try storedEntries.map { entry in
            var bestIndex = 0
            var bestDistance = Double.infinity
            for (index, centroid) in centroidVectors.enumerated() {
                let distance = try VectorConversion.distance(
                    metric: .euclidean,
                    from: centroid,
                    to: entry.vector
                )
                if distance < bestDistance {
                    bestDistance = distance
                    bestIndex = index
                }
            }
            return ReassignedListEntry(
                primaryKey: entry.primaryKey,
                payload: entry.payload,
                clusterID: bestIndex
            )
        }

        try await storeCentroids(centroids, transaction: transaction)
        try rebuildStoredLists(
            reassignedEntries,
            transaction: transaction
        )

        // Store metadata
        let metadata = IVFMetadata(
            nlist: parameters.nlist,
            dimensions: dimensions,
            trained: true,
            vectorCount: vectors.count
        )
        try await storeMetadata(metadata, transaction: transaction)
    }

    /// Check if the index has been trained
    public func isTrained(transaction: any TransactionAccess) async throws -> Bool {
        guard let metadata = try await loadMetadata(transaction: transaction) else {
            return false
        }
        return metadata.trained
    }

    // MARK: - Search

    /// Search for k nearest neighbors using IVF
    ///
    /// **Algorithm**:
    /// 1. Find nprobe nearest centroids
    /// 2. Scan vectors in those clusters
    /// 3. Return k nearest vectors
    ///
    /// - Parameters:
    ///   - queryVector: Query vector
    ///   - k: Number of neighbors to return
    ///   - transaction: FDB transaction
    /// - Returns: Array of (primaryKey, distance) sorted by distance
    public func search(
        queryVector: [Float],
        k: Int,
        transaction: any TransactionAccess
    ) async throws -> [(primaryKey: [any TupleElement], distance: Double)] {
        let retainedQuery = try Vector(float32: queryVector)
        return try await search(
            queryVector: retainedQuery,
            k: k,
            transaction: transaction
        )
    }

    func search(
        queryVector: Vector,
        k: Int,
        transaction: any TransactionAccess
    ) async throws -> [(primaryKey: [any TupleElement], distance: Double)] {
        try await IVFIndexReader(
            subspace: subspace,
            dimensions: dimensions,
            metric: metric,
            parameters: parameters
        ).search(
            queryVector: queryVector,
            k: k,
            transaction: transaction
        )
    }

    // MARK: - Private Methods

    /// Remove a vector from its inverted list
    private func removeFromInvertedList(
        id: Tuple,
        transaction: any TransactionAccess
    ) async throws {
        // Get current cluster assignment
        let assignmentSubspace = subspace.subspace(IVFIndexStorageKey.assignments.rawValue)
        let assignmentKey = assignmentSubspace.pack(id)

        guard let assignmentData = try await transaction.getValue(for: assignmentKey) else {
            return // Not in any cluster
        }

        let assignmentElements: [any TupleElement]
        do {
            assignmentElements = try Tuple.unpack(from: assignmentData)
        } catch {
            throw VectorIndexError.invalidStructure("Invalid IVF assignment payload")
        }

        guard let clusterId = assignmentElements.first as? Int64,
              clusterId >= 0
        else {
            throw VectorIndexError.invalidStructure("Invalid IVF assignment cluster id")
        }

        // Remove from inverted list
        let listSubspace = subspace.subspace(IVFIndexStorageKey.lists.rawValue)
        let listKey = listSubspace.subspace(Int(clusterId)).pack(id)
        try transaction.clear(key: listKey)

        // Remove assignment
        try transaction.clear(key: assignmentKey)
    }

    /// Add a vector to its inverted list
    private func addToInvertedList(
        id: Tuple,
        vector: Vector,
        item: Item,
        transaction: any TransactionAccess
    ) async throws {
        // Load centroids
        let centroids = try await IVFIndexReader(
            subspace: subspace,
            dimensions: dimensions,
            metric: metric,
            parameters: parameters
        ).loadCentroids(transaction: transaction)

        // If not trained, store in cluster 0 (will be reorganized after training)
        let clusterId: Int
        if centroids.isEmpty {
            clusterId = 0
        } else {
            var bestIndex = 0
            var bestDistance = Double.infinity
            for (index, centroid) in centroids.enumerated() {
                let distance = try VectorConversion.distance(
                    metric: .euclidean,
                    from: vector,
                    to: centroid
                )
                if distance < bestDistance {
                    bestDistance = distance
                    bestIndex = index
                }
            }
            clusterId = bestIndex
        }

        // Add to inverted list
        let listSubspace = subspace.subspace(IVFIndexStorageKey.lists.rawValue)
        let listKey = listSubspace.subspace(clusterId).pack(id)
        let vectorValue = try VectorConversion.float32VectorToBytes(vector)
        try transaction.setValue(vectorValue, for: listKey)

        // Store assignment
        let assignmentSubspace = subspace.subspace(IVFIndexStorageKey.assignments.rawValue)
        let assignmentKey = assignmentSubspace.pack(id)
        let assignmentValue = Tuple([Int64(clusterId)]).pack()
        try transaction.setValue(assignmentValue, for: assignmentKey)
    }

    /// Extract vector from item using VectorConversion
    private func extractVector(from item: Item) throws -> Vector {
        let fieldValues = try DataAccess.evaluate(
            item: item,
            expression: index.rootExpression
        )

        let vector = try VectorConversion.extractFloat32Vector(
            from: fieldValues
        )

        guard vector.count == dimensions else {
            throw VectorIndexError.dimensionMismatch(
                expected: dimensions,
                actual: vector.count
            )
        }

        return vector
    }

    /// Store centroids
    private func storeCentroids(
        _ centroids: [[Float]],
        transaction: any TransactionAccess
    ) async throws {
        let centroidSubspace = subspace.subspace(IVFIndexStorageKey.centroids.rawValue)
        let (begin, end) = centroidSubspace.range()
        try transaction.clearRange(beginKey: begin, endKey: end)

        // Store each centroid with its index
        for (i, centroid) in centroids.enumerated() {
            let key = centroidSubspace.pack(Tuple([i]))
            let value = try VectorConversion.floatMatrixToBytesForPersistence(
                [centroid],
                columnCount: dimensions
            )
            try transaction.setValue(value, for: key)
        }
    }

    /// Loads and validates the complete list/assignment state before training
    /// mutates it. Persisted vector payloads remain retained byte views.
    private func loadStoredListEntries(
        transaction: any TransactionAccess
    ) async throws -> [StoredListEntry] {
        let listSubspace = subspace.subspace(
            IVFIndexStorageKey.lists.rawValue
        )
        let assignmentSubspace = subspace.subspace(
            IVFIndexStorageKey.assignments.rawValue
        )
        let listRange = listSubspace.range()
        let assignmentRange = assignmentSubspace.range()
        let listEntries = try await TransactionRangeCollection.collect(
            using: transaction,
            from: .firstGreaterOrEqual(listRange.begin),
            to: .firstGreaterOrEqual(listRange.end),
            limit: 0,
            reverse: false,
            snapshot: false,
            streamingMode: .wantAll
        )
        let assignmentEntries = try await TransactionRangeCollection.collect(
            using: transaction,
            from: .firstGreaterOrEqual(assignmentRange.begin),
            to: .firstGreaterOrEqual(assignmentRange.end),
            limit: 0,
            reverse: false,
            snapshot: false,
            streamingMode: .wantAll
        )

        var assignmentsByPrimaryKey: [ByteString: Int] = [:]
        assignmentsByPrimaryKey.reserveCapacity(assignmentEntries.count)
        for (key, value) in assignmentEntries {
            let primaryKey: Tuple
            let clusterID: Int
            do {
                primaryKey = try assignmentSubspace.unpack(key)
                let assignment = try Tuple(packed: value)
                guard assignment.count == 1,
                      case .signedInteger(let encodedClusterID) = try assignment.value(at: 0),
                      let decodedClusterID = Int(exactly: encodedClusterID),
                      decodedClusterID >= 0 else {
                    throw VectorIndexError.invalidStructure(
                        "Invalid IVF assignment payload"
                    )
                }
                clusterID = decodedClusterID
            } catch let error as VectorIndexError {
                throw error
            } catch {
                throw VectorIndexError.invalidStructure(
                    "Invalid IVF assignment state"
                )
            }
            let packedPrimaryKey = primaryKey.pack()
            guard assignmentsByPrimaryKey.updateValue(
                clusterID,
                forKey: packedPrimaryKey
            ) == nil else {
                throw VectorIndexError.invalidStructure(
                    "Duplicate IVF assignment primary key"
                )
            }
        }

        var storedEntries: [StoredListEntry] = []
        storedEntries.reserveCapacity(listEntries.count)
        var observedPrimaryKeys: Set<ByteString> = []
        observedPrimaryKeys.reserveCapacity(listEntries.count)
        for (key, value) in listEntries {
            let listKey: Tuple
            let clusterID: Int
            let primaryKey: Tuple
            do {
                listKey = try listSubspace.unpack(key)
                guard listKey.count >= 2,
                      case .signedInteger(let encodedClusterID) = try listKey.value(at: 0),
                      let decodedClusterID = Int(exactly: encodedClusterID),
                      decodedClusterID >= 0 else {
                    throw VectorIndexError.invalidStructure(
                        "Invalid IVF list key"
                    )
                }
                clusterID = decodedClusterID
                primaryKey = Tuple(
                    try listKey.elements(in: 1..<listKey.count)
                )
            } catch let error as VectorIndexError {
                throw error
            } catch {
                throw VectorIndexError.invalidStructure(
                    "Invalid IVF list key"
                )
            }

            let packedPrimaryKey = primaryKey.pack()
            guard observedPrimaryKeys.insert(packedPrimaryKey).inserted else {
                throw VectorIndexError.invalidStructure(
                    "Duplicate IVF list primary key"
                )
            }
            guard assignmentsByPrimaryKey[packedPrimaryKey] == clusterID else {
                throw VectorIndexError.invalidStructure(
                    "IVF list and assignment state disagree"
                )
            }
            storedEntries.append(
                StoredListEntry(
                    primaryKey: primaryKey,
                    payload: value,
                    vector: try VectorConversion.persistedVector(
                        value,
                        expectedCount: dimensions
                    )
                )
            )
        }

        guard observedPrimaryKeys.count == assignmentsByPrimaryKey.count else {
            throw VectorIndexError.invalidStructure(
                "IVF assignment state contains an orphan primary key"
            )
        }
        return storedEntries
    }

    /// Rebuilds list membership and assignments together in the caller's
    /// transaction after every training pass.
    private func rebuildStoredLists(
        _ entries: [ReassignedListEntry],
        transaction: any TransactionAccess
    ) throws {
        let listSubspace = subspace.subspace(
            IVFIndexStorageKey.lists.rawValue
        )
        let assignmentSubspace = subspace.subspace(
            IVFIndexStorageKey.assignments.rawValue
        )
        let listRange = listSubspace.range()
        let assignmentRange = assignmentSubspace.range()
        try transaction.clearRange(
            beginKey: listRange.begin,
            endKey: listRange.end
        )
        try transaction.clearRange(
            beginKey: assignmentRange.begin,
            endKey: assignmentRange.end
        )

        for entry in entries {
            let listKey = listSubspace
                .subspace(entry.clusterID)
                .pack(entry.primaryKey)
            try transaction.setValue(entry.payload, for: listKey)
            try transaction.setValue(
                Tuple(Int64(entry.clusterID)).pack(),
                for: assignmentSubspace.pack(entry.primaryKey)
            )
        }
    }

    /// Store metadata
    private func storeMetadata(
        _ metadata: IVFMetadata,
        transaction: any TransactionAccess
    ) async throws {
        let metadataKey = subspace.pack(Tuple([IVFIndexStorageKey.metadata.rawValue]))
        let encoded = Tuple(
            IVFMetadata.formatVersion,
            Int64(metadata.nlist),
            Int64(metadata.dimensions),
            metadata.trained,
            Int64(metadata.vectorCount)
        ).pack()
        try transaction.setValue(encoded, for: metadataKey)
    }

    /// Load metadata
    private func loadMetadata(
        transaction: any TransactionAccess
    ) async throws -> IVFMetadata? {
        let metadataKey = subspace.pack(Tuple([IVFIndexStorageKey.metadata.rawValue]))
        guard let data = try await transaction.getValue(for: metadataKey) else {
            return nil
        }
        do {
            return try IVFMetadata(packed: data)
        } catch {
            throw VectorIndexError.invalidStructure("Invalid IVF metadata")
        }
    }

}

// MARK: - IVF Metadata

/// Metadata for IVF index
private struct IVFMetadata: Sendable {
    static let formatVersion: Int64 = 1

    let nlist: Int
    let dimensions: Int
    let trained: Bool
    let vectorCount: Int

    init(
        nlist: Int,
        dimensions: Int,
        trained: Bool,
        vectorCount: Int
    ) {
        self.nlist = nlist
        self.dimensions = dimensions
        self.trained = trained
        self.vectorCount = vectorCount
    }

    init(packed bytes: ByteString) throws {
        let tuple = try Tuple(packed: bytes)
        guard tuple.count == 5,
              case .signedInteger(Self.formatVersion) = try tuple.value(at: 0),
              case .signedInteger(let nlist) = try tuple.value(at: 1),
              case .signedInteger(let dimensions) = try tuple.value(at: 2),
              case .boolean(let trained) = try tuple.value(at: 3),
              case .signedInteger(let vectorCount) = try tuple.value(at: 4),
              let decodedNlist = Int(exactly: nlist),
              let decodedDimensions = Int(exactly: dimensions),
              let decodedVectorCount = Int(exactly: vectorCount),
              decodedNlist > 0,
              decodedDimensions > 0,
              decodedVectorCount >= 0 else {
            throw VectorIndexError.invalidStructure("Invalid IVF metadata")
        }
        self.init(
            nlist: decodedNlist,
            dimensions: decodedDimensions,
            trained: trained,
            vectorCount: decodedVectorCount
        )
    }
}
