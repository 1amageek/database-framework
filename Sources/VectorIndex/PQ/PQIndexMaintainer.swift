// PQIndexMaintainer.swift
// VectorIndex - Product Quantization index maintainer for FDB
//
// Reference: Jégou et al., "Product Quantization for Nearest Neighbor Search",
// IEEE Transactions on Pattern Analysis and Machine Intelligence, 2011

import DatabaseTypes
import DatabaseKit
import DatabaseEngine
import StorageKit

/// Maintainer for Product Quantization vector indexes
///
/// **Algorithm**:
/// Product Quantization compresses vectors by splitting them into subspaces
/// and encoding each subspace with a single byte (centroid index).
///
/// **Storage Layout**:
/// ```
/// [subspace]/codebooks/[m] = Float32 binary payload // 256 × dsub floats per subspace
/// [subspace]/metadata = JSON { m, dimensions, trained }
/// [subspace]/codes/[primaryKey] = M-byte compressed code
/// [subspace]/vectors/[primaryKey] = Float32 binary payload // Original vectors for retraining
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
    private struct StoredVector: Sendable {
        let primaryKey: Tuple
        let vector: [Float]
    }

    // MARK: - Properties

    public let index: Index
    public let subspace: Subspace
    public let idExpression: KeyExpression

    private let dimensions: Int
    private let metric: VectorMetric
    private let parameters: PQParameters

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
    ) throws(VectorIndexError) {
        guard dimensions > 0 else {
            throw .invalidArgument("PQ vector dimensions must be positive")
        }
        guard dimensions % parameters.m == 0 else {
            throw .invalidArgument(
                "PQ vector dimensions \(dimensions) are not divisible by \(parameters.m) subquantizers"
            )
        }

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
        transaction: any TransactionAccess
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
    ) async throws -> [ByteString] {
        let codesSubspace = subspace.subspace(PQIndexStorageKey.codes.rawValue)
        return [codesSubspace.pack(id)]
    }

    // MARK: - Training

    /// Train PQ codebooks from existing vectors
    ///
    /// Should be called after inserting a representative sample of vectors.
    ///
    /// - Parameter transaction: FDB transaction
    public func train(transaction: any TransactionAccess) async throws {
        // Load all vectors from storage
        let storedVectors = try await loadAllVectorEntries(transaction: transaction)
        guard !storedVectors.isEmpty else {
            throw VectorIndexError.invalidArgument("No vectors to train on")
        }
        let vectors = storedVectors.map(\.vector)

        // Create and train quantizer
        let quantizer = try ProductQuantizer(dimensions: dimensions, parameters: parameters)
        let trainedQuantizer = try quantizer.train(vectors: vectors)

        // Store codebooks
        try await storeCodebooks(trainedQuantizer.trainedCodebooks, transaction: transaction)

        // Re-encode all vectors with new codebooks
        for storedVector in storedVectors {
            let codes = try trainedQuantizer.encode(vector: storedVector.vector)
            try await storeCodes(codes, for: storedVector.primaryKey, transaction: transaction)
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
    public func isTrained(transaction: any TransactionAccess) async throws -> Bool {
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
        transaction: any TransactionAccess
    ) async throws -> [(primaryKey: [any TupleElement], distance: Double)] {
        try await PQIndexReader(
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

    /// Remove entry for a vector
    private func removeEntry(
        id: Tuple,
        transaction: any TransactionAccess
    ) async throws {
        // Remove codes
        let codesSubspace = subspace.subspace(PQIndexStorageKey.codes.rawValue)
        let codesKey = codesSubspace.pack(id)
        try transaction.clear(key: codesKey)

        // Remove original vector
        let vectorsSubspace = subspace.subspace(PQIndexStorageKey.vectors.rawValue)
        let vectorKey = vectorsSubspace.pack(id)
        try transaction.clear(key: vectorKey)
    }

    /// Add entry for a vector
    private func addEntry(
        id: Tuple,
        vector: [Float],
        transaction: any TransactionAccess
    ) async throws {
        // Store original vector (for retraining)
        let vectorsSubspace = subspace.subspace(PQIndexStorageKey.vectors.rawValue)
        let vectorKey = vectorsSubspace.pack(id)
        let vectorValue = floatArrayToBytes(vector)
        try transaction.setValue(vectorValue, for: vectorKey)

        // If trained, also store codes
        let codebooks = try await PQIndexReader(
            subspace: subspace,
            dimensions: dimensions,
            metric: metric,
            parameters: parameters
        ).loadCodebooks(transaction: transaction)
        if !codebooks.isEmpty {
            let quantizer = try ProductQuantizer(dimensions: dimensions, codebooks: codebooks)
            let codes = try quantizer.encode(vector: vector)
            try await storeCodes(codes, for: id, transaction: transaction)
        }
    }

    /// Store codes for a primary key
    private func storeCodes(
        _ codes: [UInt8],
        for id: Tuple,
        transaction: any TransactionAccess
    ) async throws {
        let codesSubspace = subspace.subspace(PQIndexStorageKey.codes.rawValue)
        let key = codesSubspace.pack(id)
        try transaction.setValue(ByteString(codes), for: key)
    }

    /// Store codebooks
    private func storeCodebooks(
        _ codebooks: [[[Float]]],
        transaction: any TransactionAccess
    ) async throws {
        let codebooksSubspace = subspace.subspace(PQIndexStorageKey.codebooks.rawValue)

        for (m, subspaceCodebook) in codebooks.enumerated() {
            // Flatten centroids for this subspace: [256][dsub] -> [256 * dsub]
            var flattened: [Float] = []
            for centroid in subspaceCodebook {
                flattened.append(contentsOf: centroid)
            }

            let key = codebooksSubspace.pack(Tuple([m]))
            let value = floatArrayToBytes(flattened)
            try transaction.setValue(value, for: key)
        }
    }

    /// Load all vectors and primary keys for training.
    private func loadAllVectorEntries(
        transaction: any TransactionAccess
    ) async throws -> [StoredVector] {
        let vectorsSubspace = subspace.subspace(PQIndexStorageKey.vectors.rawValue)
        let (begin, end) = vectorsSubspace.range()
        let sequence = try await TransactionRangeCollection.collect(using: transaction,
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            limit: 0,
            reverse: false,
            snapshot: true,
            streamingMode: .wantAll
        )

        var vectors: [StoredVector] = []

        for (key, value) in sequence {
            let primaryKey: Tuple
            do {
                primaryKey = try vectorsSubspace.unpack(key)
            } catch {
                throw VectorIndexError.invalidStructure("Invalid PQ vector primary key")
            }
            let vector = try VectorConversion.decodeFloatArray(value, expectedCount: dimensions)
            vectors.append(StoredVector(primaryKey: primaryKey, vector: vector))
        }

        return vectors
    }

    /// Store metadata
    private func storeMetadata(
        _ metadata: PQMetadata,
        transaction: any TransactionAccess
    ) async throws {
        let metadataKey = subspace.pack(Tuple([PQIndexStorageKey.metadata.rawValue]))
        let encoded = Tuple(
            PQMetadata.formatVersion,
            Int64(metadata.m),
            Int64(metadata.dimensions),
            metadata.trained,
            Int64(metadata.vectorCount)
        ).pack()
        try transaction.setValue(encoded, for: metadataKey)
    }

    /// Load metadata
    private func loadMetadata(
        transaction: any TransactionAccess
    ) async throws -> PQMetadata? {
        let metadataKey = subspace.pack(Tuple([PQIndexStorageKey.metadata.rawValue]))
        guard let data = try await transaction.getValue(for: metadataKey) else {
            return nil
        }
        do {
            return try PQMetadata(packed: data)
        } catch {
            throw VectorIndexError.invalidStructure("Invalid PQ metadata")
        }
    }

    /// Extract vector from item using VectorConversion
    private func extractVector(from item: Item) throws -> [Float] {
        let fieldValues = try DataAccess.evaluate(
            item: item,
            expression: index.rootExpression
        )

        let floatArray = try VectorConversion.extractFloatArray(from: fieldValues)

        guard floatArray.count == dimensions else {
            throw VectorIndexError.dimensionMismatch(expected: dimensions, actual: floatArray.count)
        }

        return floatArray
    }

    // MARK: - Serialization Helpers

    /// Convert float array to bytes using VectorConversion
    private func floatArrayToBytes(_ floats: [Float]) -> ByteString {
        VectorConversion.floatArrayToBytes(floats)
    }

}

// MARK: - PQ Metadata

/// Metadata for PQ index
private struct PQMetadata: Sendable {
    static let formatVersion: Int64 = 1

    let m: Int
    let dimensions: Int
    let trained: Bool
    let vectorCount: Int

    init(
        m: Int,
        dimensions: Int,
        trained: Bool,
        vectorCount: Int
    ) {
        self.m = m
        self.dimensions = dimensions
        self.trained = trained
        self.vectorCount = vectorCount
    }

    init(packed bytes: ByteString) throws {
        let tuple = try Tuple(packed: bytes)
        guard tuple.count == 5,
              case .signedInteger(Self.formatVersion) = try tuple.value(at: 0),
              case .signedInteger(let m) = try tuple.value(at: 1),
              case .signedInteger(let dimensions) = try tuple.value(at: 2),
              case .boolean(let trained) = try tuple.value(at: 3),
              case .signedInteger(let vectorCount) = try tuple.value(at: 4),
              let decodedM = Int(exactly: m),
              let decodedDimensions = Int(exactly: dimensions),
              let decodedVectorCount = Int(exactly: vectorCount),
              decodedM > 0,
              decodedDimensions > 0,
              decodedVectorCount >= 0 else {
            throw VectorIndexError.invalidStructure("Invalid PQ metadata")
        }
        self.init(
            m: decodedM,
            dimensions: decodedDimensions,
            trained: trained,
            vectorCount: decodedVectorCount
        )
    }
}
