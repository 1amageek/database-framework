// IVFIndexMaintainer.swift
// VectorIndex - IVF (Inverted File Index) maintainer for vector indexes
//
// Reference: Jégou et al., "Product Quantization for Nearest Neighbor Search",
// IEEE Transactions on Pattern Analysis and Machine Intelligence, 2011

import Foundation
import Core
import DatabaseEngine
import FoundationDB
import Vector

/// Maintainer for IVF (Inverted File Index) vector indexes
///
/// **Algorithm**:
/// IVF partitions the vector space into nlist clusters using K-means.
/// Each vector is assigned to its nearest cluster centroid.
/// At query time, only nprobe nearest clusters are searched.
///
/// **Storage Layout**:
/// ```
/// [subspace]/centroids = Tuple([centroid1], [centroid2], ...)
/// [subspace]/metadata = JSON { nlist, dimensions, trained, vectorCount }
/// [subspace]/lists/[clusterId]/[primaryKey] = Tuple(vector...)
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
    // MARK: - Properties

    public let index: Index
    public let subspace: Subspace
    public let idExpression: KeyExpression

    private let dimensions: Int
    private let metric: VectorMetric
    private let parameters: IVFParameters

    // Subspace keys
    private enum SubspaceKey: Int {
        case centroids = 0
        case metadata = 1
        case lists = 2
        case assignments = 3
    }

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
        transaction: any TransactionProtocol
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
                try await addToInvertedList(id: newId, vector: vector, transaction: transaction)
            } catch DataAccessError.nilValueCannotBeIndexed {
                // Sparse index: nil vector is not indexed
            }
        }
    }

    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        do {
            let vector = try extractVector(from: item)
            try await addToInvertedList(id: id, vector: vector, transaction: transaction)
        } catch DataAccessError.nilValueCannotBeIndexed {
            // Sparse index: nil vector is not indexed
        }
    }

    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [FDB.Bytes] {
        // IVF stores data in inverted lists, not directly by primary key
        // Return the assignment key for verification
        let assignmentSubspace = subspace.subspace(SubspaceKey.assignments.rawValue)
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
        transaction: any TransactionProtocol
    ) async throws {
        let clustering = KMeansClustering(
            k: parameters.nlist,
            dimensions: dimensions,
            maxIterations: parameters.kmeansIterations
        )

        let centroids = clustering.train(vectors: vectors)
        try await storeCentroids(centroids, transaction: transaction)

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
    public func isTrained(transaction: any TransactionProtocol) async throws -> Bool {
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
        transaction: any TransactionProtocol
    ) async throws -> [(primaryKey: [any TupleElement], distance: Double)] {
        guard queryVector.count == dimensions else {
            throw VectorIndexError.dimensionMismatch(expected: dimensions, actual: queryVector.count)
        }

        guard k > 0 else {
            throw VectorIndexError.invalidArgument("k must be positive")
        }

        // Load centroids
        let centroids = try await loadCentroids(transaction: transaction)
        guard !centroids.isEmpty else {
            throw VectorIndexError.invalidStructure("IVF index not trained")
        }

        // Find nprobe nearest centroids
        let clustering = KMeansClustering(k: parameters.nlist, dimensions: dimensions)
        let nearestClusters = clustering.findNearestCentroids(
            query: queryVector,
            centroids: centroids,
            nprobe: parameters.nprobe
        )

        // Search in the selected clusters
        var heap = MinHeap<(primaryKey: [any TupleElement], distance: Double)>(
            maxSize: k,
            heapType: .max,
            comparator: { $0.distance > $1.distance }
        )

        for clusterId in nearestClusters {
            let listSubspace = subspace
                .subspace(SubspaceKey.lists.rawValue)
                .subspace(clusterId)

            let (begin, end) = listSubspace.range()
            let sequence = transaction.getRange(begin: begin, end: end, snapshot: true)

            for try await (key, value) in sequence {
                // Decode primary key
                guard let pkTuple = try? listSubspace.unpack(key) else { continue }

                // Decode vector
                guard let vectorElements = try? Tuple.unpack(from: value) else { continue }
                let vector = tupleToVector(vectorElements)
                guard vector.count == dimensions else { continue }

                // Calculate distance
                let distance = calculateDistance(queryVector, vector)

                // Convert Tuple to [any TupleElement]
                var primaryKey: [any TupleElement] = []
                for i in 0..<pkTuple.count {
                    if let element = pkTuple[i] {
                        primaryKey.append(element)
                    }
                }

                // Insert into heap
                heap.insert((primaryKey: primaryKey, distance: distance))
            }
        }

        return heap.sorted()
    }

    // MARK: - Private Methods

    /// Remove a vector from its inverted list
    private func removeFromInvertedList(
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        // Get current cluster assignment
        let assignmentSubspace = subspace.subspace(SubspaceKey.assignments.rawValue)
        let assignmentKey = assignmentSubspace.pack(id)

        guard let assignmentData = try await transaction.getValue(for: assignmentKey) else {
            return // Not in any cluster
        }

        guard let assignmentTuple = try? Tuple.unpack(from: assignmentData),
              let clusterId = assignmentTuple[0] as? Int64 else {
            return
        }

        // Remove from inverted list
        let listSubspace = subspace.subspace(SubspaceKey.lists.rawValue)
        let listKey = listSubspace.subspace(Int(clusterId)).pack(id)
        transaction.clear(key: listKey)

        // Remove assignment
        transaction.clear(key: assignmentKey)
    }

    /// Add a vector to its inverted list
    private func addToInvertedList(
        id: Tuple,
        vector: [Float],
        transaction: any TransactionProtocol
    ) async throws {
        // Load centroids
        let centroids = try await loadCentroids(transaction: transaction)

        // If not trained, store in cluster 0 (will be reorganized after training)
        let clusterId: Int
        if centroids.isEmpty {
            clusterId = 0
        } else {
            let clustering = KMeansClustering(k: parameters.nlist, dimensions: dimensions)
            clusterId = clustering.assignToNearestCentroid(vector: vector, centroids: centroids)
        }

        // Add to inverted list
        let listSubspace = subspace.subspace(SubspaceKey.lists.rawValue)
        let listKey = listSubspace.subspace(clusterId).pack(id)
        let vectorValue = vectorToTuple(vector).pack()
        transaction.setValue(vectorValue, for: listKey)

        // Store assignment
        let assignmentSubspace = subspace.subspace(SubspaceKey.assignments.rawValue)
        let assignmentKey = assignmentSubspace.pack(id)
        let assignmentValue = Tuple([Int64(clusterId)]).pack()
        transaction.setValue(assignmentValue, for: assignmentKey)
    }

    /// Extract vector from item
    private func extractVector(from item: Item) throws -> [Float] {
        let fieldValues = try DataAccess.evaluateIndexFields(
            from: item,
            keyPaths: index.keyPaths,
            expression: index.rootExpression
        )

        var floatArray: [Float] = []
        for element in fieldValues {
            if let array = element as? [Float] {
                floatArray.append(contentsOf: array)
            } else if let array = element as? [Double] {
                floatArray.append(contentsOf: array.map { Float($0) })
            } else if let f = element as? Float {
                floatArray.append(f)
            } else if let d = element as? Double {
                floatArray.append(Float(d))
            }
        }

        guard floatArray.count == dimensions else {
            throw VectorIndexError.dimensionMismatch(expected: dimensions, actual: floatArray.count)
        }

        return floatArray
    }

    /// Store centroids
    private func storeCentroids(
        _ centroids: [[Float]],
        transaction: any TransactionProtocol
    ) async throws {
        let centroidSubspace = subspace.subspace(SubspaceKey.centroids.rawValue)

        // Store each centroid with its index
        for (i, centroid) in centroids.enumerated() {
            let key = centroidSubspace.pack(Tuple([i]))
            let value = vectorToTuple(centroid).pack()
            transaction.setValue(value, for: key)
        }
    }

    /// Load centroids
    private func loadCentroids(
        transaction: any TransactionProtocol
    ) async throws -> [[Float]] {
        let centroidSubspace = subspace.subspace(SubspaceKey.centroids.rawValue)
        let (begin, end) = centroidSubspace.range()
        let sequence = transaction.getRange(begin: begin, end: end, snapshot: true)

        var centroids: [[Float]] = []
        for try await (_, value) in sequence {
            guard let elements = try? Tuple.unpack(from: value) else { continue }
            let vector = tupleToVector(elements)
            centroids.append(vector)
        }

        return centroids
    }

    /// Store metadata
    private func storeMetadata(
        _ metadata: IVFMetadata,
        transaction: any TransactionProtocol
    ) async throws {
        let metadataKey = subspace.pack(Tuple([SubspaceKey.metadata.rawValue]))
        let encoder = JSONEncoder()
        let data = try encoder.encode(metadata)
        transaction.setValue([UInt8](data), for: metadataKey)
    }

    /// Load metadata
    private func loadMetadata(
        transaction: any TransactionProtocol
    ) async throws -> IVFMetadata? {
        let metadataKey = subspace.pack(Tuple([SubspaceKey.metadata.rawValue]))
        guard let data = try await transaction.getValue(for: metadataKey) else {
            return nil
        }
        let decoder = JSONDecoder()
        return try? decoder.decode(IVFMetadata.self, from: Data(data))
    }

    /// Convert vector to tuple
    private func vectorToTuple(_ vector: [Float]) -> Tuple {
        let elements: [any TupleElement] = vector.map { $0 as any TupleElement }
        return Tuple(elements)
    }

    /// Convert tuple elements to vector
    private func tupleToVector(_ elements: [any TupleElement]) -> [Float] {
        var vector: [Float] = []
        for element in elements {
            if let f = element as? Float {
                vector.append(f)
            } else if let d = element as? Double {
                vector.append(Float(d))
            } else if let i64 = element as? Int64 {
                vector.append(Float(i64))
            } else if let i = element as? Int {
                vector.append(Float(i))
            }
        }
        return vector
    }

    /// Calculate distance between vectors
    private func calculateDistance(_ v1: [Float], _ v2: [Float]) -> Double {
        switch metric {
        case .cosine:
            return cosineDistance(v1, v2)
        case .euclidean:
            return euclideanDistance(v1, v2)
        case .dotProduct:
            return dotProductDistance(v1, v2)
        }
    }

    private func cosineDistance(_ v1: [Float], _ v2: [Float]) -> Double {
        let dotProduct = zip(v1, v2).map { Double($0) * Double($1) }.reduce(0, +)
        let norm1 = sqrt(v1.map { Double($0) * Double($0) }.reduce(0, +))
        let norm2 = sqrt(v2.map { Double($0) * Double($0) }.reduce(0, +))

        guard norm1 > 0 && norm2 > 0 else { return 2.0 }
        return 1.0 - dotProduct / (norm1 * norm2)
    }

    private func euclideanDistance(_ v1: [Float], _ v2: [Float]) -> Double {
        var sum: Double = 0.0
        for (a, b) in zip(v1, v2) {
            let diff = Double(a) - Double(b)
            sum += diff * diff
        }
        return sqrt(sum)
    }

    private func dotProductDistance(_ v1: [Float], _ v2: [Float]) -> Double {
        let dotProduct = zip(v1, v2).map { Double($0) * Double($1) }.reduce(0, +)
        return -dotProduct
    }
}

// MARK: - IVF Metadata

/// Metadata for IVF index
private struct IVFMetadata: Codable {
    let nlist: Int
    let dimensions: Int
    let trained: Bool
    let vectorCount: Int
}
