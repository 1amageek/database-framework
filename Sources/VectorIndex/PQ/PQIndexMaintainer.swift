// PQIndexMaintainer.swift
// VectorIndex - Product Quantization index maintainer for FDB
//
// Reference: Jégou et al., "Product Quantization for Nearest Neighbor Search",
// IEEE Transactions on Pattern Analysis and Machine Intelligence, 2011

import Foundation
import Core
import DatabaseEngine
import FoundationDB
import Vector

/// Maintainer for Product Quantization vector indexes
///
/// **Algorithm**:
/// Product Quantization compresses vectors by splitting them into subspaces
/// and encoding each subspace with a single byte (centroid index).
///
/// **Storage Layout**:
/// ```
/// [subspace]/codebooks/[m] = Tuple([Float]...)      // 256 × dsub floats per subspace
/// [subspace]/metadata = JSON { m, dimensions, trained }
/// [subspace]/codes/[primaryKey] = Data([UInt8] × M) // Compressed codes
/// [subspace]/vectors/[primaryKey] = Tuple(Float...) // Original vectors (for retraining)
/// ```
///
/// **Performance**:
/// - Training: O(n × M × 256 × dsub × iterations)
/// - Insert: O(M × 256 × dsub) encoding
/// - Query: O(M × 256 × dsub) precompute + O(n × M) scan
/// - Storage: M bytes per vector (vs d × 4 bytes for flat)
///
/// **Usage**:
/// ```swift
/// let maintainer = PQIndexMaintainer<Product>(
///     index: vectorIndex,
///     dimensions: 384,
///     metric: .euclidean,
///     subspace: vectorSubspace,
///     idExpression: FieldKeyExpression(fieldName: "id"),
///     parameters: PQParameters(m: 8)
/// )
/// ```
public struct PQIndexMaintainer<Item: Persistable>: IndexMaintainer {
    // MARK: - Properties

    public let index: Index
    public let subspace: Subspace
    public let idExpression: KeyExpression

    private let dimensions: Int
    private let metric: VectorMetric
    private let parameters: PQParameters

    // Subspace keys
    private enum SubspaceKey: Int {
        case codebooks = 0
        case metadata = 1
        case codes = 2
        case vectors = 3  // Store original vectors for retraining
    }

    // MARK: - Initialization

    /// Create PQ index maintainer
    ///
    /// - Parameters:
    ///   - index: Index definition
    ///   - dimensions: Vector dimensions (must be divisible by m)
    ///   - metric: Distance metric
    ///   - subspace: FDB subspace for this index
    ///   - idExpression: Expression for extracting item's unique identifier
    ///   - parameters: PQ algorithm parameters
    public init(
        index: Index,
        dimensions: Int,
        metric: VectorMetric,
        subspace: Subspace,
        idExpression: KeyExpression,
        parameters: PQParameters
    ) {
        precondition(dimensions % parameters.m == 0,
            "Dimensions (\(dimensions)) must be divisible by m (\(parameters.m))")

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
        // Remove old entry
        if let oldItem = oldItem {
            do {
                let oldId = try DataAccess.extractId(from: oldItem, using: idExpression)
                try await removeEntry(id: oldId, transaction: transaction)
            } catch DataAccessError.nilValueCannotBeIndexed {
                // Sparse index: nil vector was not indexed
            }
        }

        // Add new entry
        if let newItem = newItem {
            do {
                let newId = try DataAccess.extractId(from: newItem, using: idExpression)
                let vector = try extractVector(from: newItem)
                try await addEntry(id: newId, vector: vector, transaction: transaction)
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
            try await addEntry(id: id, vector: vector, transaction: transaction)
        } catch DataAccessError.nilValueCannotBeIndexed {
            // Sparse index
        }
    }

    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [FDB.Bytes] {
        let codesSubspace = subspace.subspace(SubspaceKey.codes.rawValue)
        return [codesSubspace.pack(id)]
    }

    // MARK: - Training

    /// Train PQ codebooks from existing vectors
    ///
    /// Should be called after inserting a representative sample of vectors.
    ///
    /// - Parameter transaction: FDB transaction
    public func train(transaction: any TransactionProtocol) async throws {
        // Load all vectors from storage
        let vectors = try await loadAllVectors(transaction: transaction)
        guard !vectors.isEmpty else {
            throw VectorIndexError.invalidArgument("No vectors to train on")
        }

        // Create and train quantizer
        let quantizer = ProductQuantizer(dimensions: dimensions, parameters: parameters)
        let trainedQuantizer = quantizer.train(vectors: vectors)

        // Store codebooks
        try await storeCodebooks(trainedQuantizer.getCodebooks(), transaction: transaction)

        // Re-encode all vectors with new codebooks
        for (i, vector) in vectors.enumerated() {
            let codes = trainedQuantizer.encode(vector: vector)
            let pk = try await getPrimaryKeyForVectorIndex(i, transaction: transaction)
            if let pk = pk {
                try await storeCodes(codes, for: pk, transaction: transaction)
            }
        }

        // Update metadata
        let metadata = PQMetadata(
            m: parameters.m,
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

    /// Search for k nearest neighbors using PQ
    ///
    /// Uses Asymmetric Distance Computation (ADC):
    /// 1. Precompute distance table from query to all centroids
    /// 2. Scan all codes and sum up distances from table
    /// 3. Return k nearest
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

        // Load codebooks and create quantizer
        let codebooks = try await loadCodebooks(transaction: transaction)
        guard !codebooks.isEmpty else {
            throw VectorIndexError.invalidStructure("PQ index not trained")
        }

        let quantizer = ProductQuantizer(dimensions: dimensions, codebooks: codebooks)

        // Precompute distance table
        let distanceTable = quantizer.computeDistanceTable(query: queryVector)

        // Scan all codes
        var heap = MinHeap<(primaryKey: [any TupleElement], distance: Double)>(
            maxSize: k,
            heapType: .max,
            comparator: { $0.distance > $1.distance }
        )

        let codesSubspace = subspace.subspace(SubspaceKey.codes.rawValue)
        let (begin, end) = codesSubspace.range()
        let sequence = transaction.getRange(begin: begin, end: end, snapshot: true)

        for try await (key, value) in sequence {
            // Decode primary key
            guard let pkTuple = try? codesSubspace.unpack(key) else { continue }

            // Decode codes
            let codes = [UInt8](value)
            guard codes.count == parameters.m else { continue }

            // Compute distance using precomputed table
            let sqDistance = quantizer.computeDistance(codes: codes, table: distanceTable)
            let distance = adjustDistance(Double(sqDistance))

            // Convert Tuple to [any TupleElement]
            var primaryKey: [any TupleElement] = []
            for i in 0..<pkTuple.count {
                if let element = pkTuple[i] {
                    primaryKey.append(element)
                }
            }

            heap.insert((primaryKey: primaryKey, distance: distance))
        }

        return heap.sorted()
    }

    // MARK: - Private Methods

    /// Remove entry for a vector
    private func removeEntry(
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        // Remove codes
        let codesSubspace = subspace.subspace(SubspaceKey.codes.rawValue)
        let codesKey = codesSubspace.pack(id)
        transaction.clear(key: codesKey)

        // Remove original vector
        let vectorsSubspace = subspace.subspace(SubspaceKey.vectors.rawValue)
        let vectorKey = vectorsSubspace.pack(id)
        transaction.clear(key: vectorKey)
    }

    /// Add entry for a vector
    private func addEntry(
        id: Tuple,
        vector: [Float],
        transaction: any TransactionProtocol
    ) async throws {
        // Store original vector (for retraining)
        let vectorsSubspace = subspace.subspace(SubspaceKey.vectors.rawValue)
        let vectorKey = vectorsSubspace.pack(id)
        let vectorValue = vectorToTuple(vector).pack()
        transaction.setValue(vectorValue, for: vectorKey)

        // If trained, also store codes
        if let codebooks = try? await loadCodebooks(transaction: transaction),
           !codebooks.isEmpty {
            let quantizer = ProductQuantizer(dimensions: dimensions, codebooks: codebooks)
            let codes = quantizer.encode(vector: vector)
            try await storeCodes(codes, for: id, transaction: transaction)
        }
    }

    /// Store codes for a primary key
    private func storeCodes(
        _ codes: [UInt8],
        for id: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        let codesSubspace = subspace.subspace(SubspaceKey.codes.rawValue)
        let key = codesSubspace.pack(id)
        transaction.setValue(codes, for: key)
    }

    /// Store codebooks
    private func storeCodebooks(
        _ codebooks: [[[Float]]],
        transaction: any TransactionProtocol
    ) async throws {
        let codebooksSubspace = subspace.subspace(SubspaceKey.codebooks.rawValue)

        for (m, subspaceCodebook) in codebooks.enumerated() {
            // Flatten centroids for this subspace: [256][dsub] -> [256 * dsub]
            var flattened: [Float] = []
            for centroid in subspaceCodebook {
                flattened.append(contentsOf: centroid)
            }

            let key = codebooksSubspace.pack(Tuple([m]))
            let value = floatArrayToBytes(flattened)
            transaction.setValue(value, for: key)
        }
    }

    /// Load codebooks
    private func loadCodebooks(
        transaction: any TransactionProtocol
    ) async throws -> [[[Float]]] {
        let codebooksSubspace = subspace.subspace(SubspaceKey.codebooks.rawValue)
        let (begin, end) = codebooksSubspace.range()
        let sequence = transaction.getRange(begin: begin, end: end, snapshot: true)

        var codebooks: [[[Float]]] = []

        for try await (_, value) in sequence {
            let flattened = bytesToFloatArray(value)
            let dsub = dimensions / parameters.m

            // Unflatten: [256 * dsub] -> [256][dsub]
            var centroids: [[Float]] = []
            for i in 0..<parameters.ksub {
                let start = i * dsub
                let end = start + dsub
                centroids.append(Array(flattened[start..<end]))
            }
            codebooks.append(centroids)
        }

        return codebooks
    }

    /// Load all vectors for training
    private func loadAllVectors(
        transaction: any TransactionProtocol
    ) async throws -> [[Float]] {
        let vectorsSubspace = subspace.subspace(SubspaceKey.vectors.rawValue)
        let (begin, end) = vectorsSubspace.range()
        let sequence = transaction.getRange(begin: begin, end: end, snapshot: true)

        var vectors: [[Float]] = []

        for try await (_, value) in sequence {
            guard let elements = try? Tuple.unpack(from: value) else { continue }
            let vector = tupleElementsToVector(elements)
            if vector.count == dimensions {
                vectors.append(vector)
            }
        }

        return vectors
    }

    /// Get primary key for a vector by index (for re-encoding after training)
    private func getPrimaryKeyForVectorIndex(
        _ index: Int,
        transaction: any TransactionProtocol
    ) async throws -> Tuple? {
        let vectorsSubspace = subspace.subspace(SubspaceKey.vectors.rawValue)
        let (begin, end) = vectorsSubspace.range()
        let sequence = transaction.getRange(begin: begin, end: end, snapshot: true)

        var count = 0
        for try await (key, _) in sequence {
            if count == index {
                return try? vectorsSubspace.unpack(key)
            }
            count += 1
        }
        return nil
    }

    /// Store metadata
    private func storeMetadata(
        _ metadata: PQMetadata,
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
    ) async throws -> PQMetadata? {
        let metadataKey = subspace.pack(Tuple([SubspaceKey.metadata.rawValue]))
        guard let data = try await transaction.getValue(for: metadataKey) else {
            return nil
        }
        let decoder = JSONDecoder()
        return try? decoder.decode(PQMetadata.self, from: Data(data))
    }

    /// Extract vector from item using VectorConversion
    private func extractVector(from item: Item) throws -> [Float] {
        let fieldValues = try DataAccess.evaluateIndexFields(
            from: item,
            keyPaths: index.keyPaths,
            expression: index.rootExpression
        )

        let floatArray = try VectorConversion.extractFloatArray(from: fieldValues)

        guard floatArray.count == dimensions else {
            throw VectorIndexError.dimensionMismatch(expected: dimensions, actual: floatArray.count)
        }

        return floatArray
    }

    /// Adjust distance based on metric
    private func adjustDistance(_ sqDistance: Double) -> Double {
        switch metric {
        case .euclidean:
            return sqrt(sqDistance)
        case .cosine:
            // PQ computes squared Euclidean; for cosine, we'd need normalized vectors
            // This is an approximation
            return sqDistance
        case .dotProduct:
            // PQ is designed for Euclidean distance
            // For dot product, results may not be accurate
            return -sqDistance
        }
    }

    // MARK: - Serialization Helpers

    /// Convert vector to tuple using VectorConversion
    private func vectorToTuple(_ vector: [Float]) -> Tuple {
        VectorConversion.vectorToTuple(vector)
    }

    /// Convert tuple elements to vector using VectorConversion
    private func tupleElementsToVector(_ elements: [any TupleElement]) -> [Float] {
        VectorConversion.tupleToVector(elements)
    }

    /// Convert float array to bytes using VectorConversion
    private func floatArrayToBytes(_ floats: [Float]) -> [UInt8] {
        VectorConversion.floatArrayToBytes(floats)
    }

    /// Convert bytes to float array using VectorConversion
    private func bytesToFloatArray(_ bytes: [UInt8]) -> [Float] {
        VectorConversion.bytesToFloatArray(bytes)
    }
}

// MARK: - PQ Metadata

/// Metadata for PQ index
private struct PQMetadata: Codable {
    let m: Int
    let dimensions: Int
    let trained: Bool
    let vectorCount: Int
}
