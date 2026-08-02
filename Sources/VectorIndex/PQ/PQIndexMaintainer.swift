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
    private let trainingResourceLimits: VectorTrainingResourceLimits

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
        parameters: PQParameters,
        trainingResourceLimits: VectorTrainingResourceLimits = .default
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
        self.trainingResourceLimits = trainingResourceLimits
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
        throw IndexVerificationError.expectedKeysUnsupported
    }

    public func computeIndexKeys(
        for item: Item,
        id: Tuple,
        transaction: any TransactionAccess
    ) async throws -> [ByteString] {
        do {
            _ = try extractVector(from: item)
        } catch DataAccessError.nilValueCannotBeIndexed {
            return []
        }

        let codesSubspace = subspace.subspace(PQIndexStorageKey.codes.rawValue)
        let vectorsSubspace = subspace.subspace(PQIndexStorageKey.vectors.rawValue)
        let vectorKey = vectorsSubspace.pack(id)
        guard try await isTrained(transaction: transaction) else {
            return [vectorKey]
        }
        return [vectorKey, codesSubspace.pack(id)]
    }

    public func finalizeBuild(
        transaction: any TransactionAccess
    ) async throws {
        try await train(transaction: transaction)
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
            try await storeMetadata(
                PQMetadata(
                    m: parameters.m,
                    dimensions: dimensions,
                    trained: false,
                    vectorCount: 0
                ),
                transaction: transaction
            )
            return
        }
        try admitTraining(storedVectorCount: storedVectors.count)
        let vectors = storedVectors.map { $0.vector }

        // Create and train quantizer
        let quantizer = try ProductQuantizer(dimensions: dimensions, parameters: parameters)
        let trainedQuantizer = try quantizer.train(vectors: vectors)

        // Store codebooks
        try await storeCodebooks(trainedQuantizer.trainedCodebooks, transaction: transaction)

        // Rebuild the entire code set so retraining cannot retain an orphan or
        // a code produced by an older codebook generation.
        let codesSubspace = subspace.subspace(
            PQIndexStorageKey.codes.rawValue
        )
        let codesRange = codesSubspace.range()
        try transaction.clearRange(
            beginKey: codesRange.begin,
            endKey: codesRange.end
        )

        // Re-encode all vectors with new codebooks.
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
        let existed = try await transaction.getValue(
            for: vectorKey,
            snapshot: false
        ) != nil
        try transaction.clear(key: vectorKey)
        if existed {
            try await updateTrainedVectorCount(
                by: -1,
                transaction: transaction
            )
        }
    }

    /// Add entry for a vector
    private func addEntry(
        id: Tuple,
        vector: Vector,
        transaction: any TransactionAccess
    ) async throws {
        // Store original vector (for retraining)
        let vectorsSubspace = subspace.subspace(PQIndexStorageKey.vectors.rawValue)
        let vectorKey = vectorsSubspace.pack(id)
        let existed = try await transaction.getValue(
            for: vectorKey,
            snapshot: false
        ) != nil
        let vectorValue = try VectorConversion.float32VectorToBytes(vector)
        try transaction.setValue(vectorValue, for: vectorKey)

        // If trained, also store codes
        let codebooks = try await PQIndexReader(
            subspace: subspace,
            dimensions: dimensions,
            metric: metric,
            parameters: parameters
        ).loadCodebookViews(transaction: transaction)
        if !codebooks.isEmpty {
            let quantizer = try PersistedProductQuantizer(
                dimensions: dimensions,
                subquantizerCount: parameters.m,
                centroidCount: parameters.ksub,
                codebooks: codebooks
            )
            let codes = try quantizer.encode(vector)
            try await storeCodes(codes, for: id, transaction: transaction)
        }

        guard !existed, let metadata = try await loadMetadata(
            transaction: transaction
        ) else {
            return
        }
        guard metadata.m == parameters.m,
              metadata.dimensions == dimensions else {
            throw VectorIndexError.invalidStructure(
                "PQ metadata does not match the configured index"
            )
        }
        if metadata.trained {
            guard !codebooks.isEmpty else {
                throw VectorIndexError.invalidStructure(
                    "PQ trained metadata has no codebooks"
                )
            }
        } else if !codebooks.isEmpty {
            throw VectorIndexError.invalidStructure(
                "PQ untrained metadata has persisted codebooks"
            )
        }
        try await updateTrainedVectorCount(
            by: 1,
            transaction: transaction
        )
    }

    private func updateTrainedVectorCount(
        by delta: Int,
        transaction: any TransactionAccess
    ) async throws {
        guard let metadata = try await loadMetadata(transaction: transaction) else {
            return
        }
        guard metadata.m == parameters.m,
              metadata.dimensions == dimensions else {
            throw VectorIndexError.invalidStructure(
                "PQ metadata does not match the configured index"
            )
        }
        let (vectorCount, overflow) = metadata.vectorCount
            .addingReportingOverflow(delta)
        guard !overflow, vectorCount >= 0 else {
            throw VectorIndexError.invalidStructure(
                "PQ metadata vector count is inconsistent"
            )
        }
        try await storeMetadata(
            PQMetadata(
                m: metadata.m,
                dimensions: metadata.dimensions,
                trained: metadata.trained,
                vectorCount: vectorCount
            ),
            transaction: transaction
        )
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
        let range = codebooksSubspace.range()
        try transaction.clearRange(beginKey: range.begin, endKey: range.end)

        for (m, subspaceCodebook) in codebooks.enumerated() {
            let key = codebooksSubspace.pack(Tuple([m]))
            let value = try VectorConversion.floatMatrixToBytesForPersistence(
                subspaceCodebook,
                columnCount: dimensions / parameters.m
            )
            try transaction.setValue(value, for: key)
        }
    }

    /// Load all vectors and primary keys for training.
    private func loadAllVectorEntries(
        transaction: any TransactionAccess
    ) async throws -> [StoredVector] {
        let vectorsSubspace = subspace.subspace(PQIndexStorageKey.vectors.rawValue)
        let (begin, end) = vectorsSubspace.range()
        try trainingResourceLimits.validate()
        var cursor = transaction.rangeCursor(
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            limit: 0,
            reverse: false,
            snapshot: false,
            streamingMode: .iterator
        )

        var vectors: [StoredVector] = []
        var payloadByteCount = 0
        try await cursor.consume { key, value in
            guard vectors.count < trainingResourceLimits.maximumVectorCount else {
                throw VectorIndexError.invalidArgument(
                    "PQ training exceeds the configured vector count limit"
                )
            }
            payloadByteCount = try checkedSum(
                payloadByteCount,
                value.count,
                message: "PQ stored vector payload size overflow"
            )
            guard payloadByteCount
                    <= trainingResourceLimits.maximumVectorPayloadByteCount else {
                throw VectorIndexError.invalidArgument(
                    "PQ training exceeds the configured vector payload limit"
                )
            }
            let primaryKey: Tuple
            do {
                primaryKey = try vectorsSubspace.unpack(key)
            } catch {
                throw VectorIndexError.invalidStructure("Invalid PQ vector primary key")
            }
            // PQ training updates and revisits every element over multiple
            // iterations, so it requires independent mutable-indexed arrays.
            // This is an offline training boundary, not a search-path copy.
            let vector = try VectorConversion.materializeFloatArrayForTraining(
                value,
                expectedCount: dimensions
            )
            vectors.append(StoredVector(primaryKey: primaryKey, vector: vector))
        }

        return vectors
    }

    private func admitTraining(storedVectorCount: Int) throws {
        try trainingResourceLimits.validate()
        guard storedVectorCount <= trainingResourceLimits.maximumVectorCount else {
            throw VectorIndexError.invalidArgument(
                "PQ training exceeds the configured vector count limit"
            )
        }
        let vectorScalars = try checkedProduct(
            storedVectorCount,
            dimensions,
            message: "PQ training payload size overflow"
        )
        let payloadBytes = try checkedProduct(
            vectorScalars,
            MemoryLayout<Float>.stride,
            message: "PQ training payload size overflow"
        )
        guard payloadBytes <= trainingResourceLimits.maximumVectorPayloadByteCount else {
            throw VectorIndexError.invalidArgument(
                "PQ training exceeds the configured vector payload limit"
            )
        }
        let codebookScalars = try checkedProduct(
            parameters.ksub,
            dimensions,
            message: "PQ codebook size overflow"
        )
        let codebookBytes = try checkedProduct(
            codebookScalars,
            MemoryLayout<Float>.stride,
            message: "PQ codebook size overflow"
        )
        let payloadWorkingBytes = try checkedProduct(
            payloadBytes,
            3,
            message: "PQ working memory estimate overflow"
        )
        let codebookWorkingBytes = try checkedProduct(
            codebookBytes,
            3,
            message: "PQ working memory estimate overflow"
        )
        let workingBytes = try checkedSum(
            payloadWorkingBytes,
            codebookWorkingBytes,
            message: "PQ working memory estimate overflow"
        )
        guard workingBytes <= trainingResourceLimits.maximumWorkingByteCount else {
            throw VectorIndexError.invalidArgument(
                "PQ training exceeds the configured working memory limit"
            )
        }
        let codeBytes = try checkedProduct(
            storedVectorCount,
            parameters.m,
            message: "PQ code mutation size overflow"
        )
        let keyOverheadBytes = try checkedProduct(
            storedVectorCount,
            128,
            message: "PQ mutation size estimate overflow"
        )
        var mutationBytes = try checkedSum(
            codebookBytes,
            codeBytes,
            message: "PQ mutation size estimate overflow"
        )
        mutationBytes = try checkedSum(
            mutationBytes,
            keyOverheadBytes,
            message: "PQ mutation size estimate overflow"
        )
        guard mutationBytes <= trainingResourceLimits.maximumTransactionMutationByteCount else {
            throw VectorIndexError.invalidArgument(
                "PQ training exceeds the configured transaction mutation limit"
            )
        }
    }

    private func checkedSum(
        _ lhs: Int,
        _ rhs: Int,
        message: String
    ) throws -> Int {
        let (result, overflow) = lhs.addingReportingOverflow(rhs)
        guard !overflow else {
            throw VectorIndexError.invalidArgument(message)
        }
        return result
    }

    private func checkedProduct(
        _ lhs: Int,
        _ rhs: Int,
        message: String
    ) throws -> Int {
        let (result, overflow) = lhs.multipliedReportingOverflow(by: rhs)
        guard !overflow else {
            throw VectorIndexError.invalidArgument(message)
        }
        return result
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

}

// MARK: - PQ Metadata

/// Metadata for PQ index
struct PQMetadata: Sendable {
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
