// HNSWIndexMaintainer.swift
// VectorIndex - HNSW index maintainer using swift-hnsw library
//
// Provides high-performance approximate nearest neighbor search using the
// SwiftHNSW library (https://github.com/1amageek/swift-hnsw).

import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit
import SwiftHNSW

// MARK: - HNSW Constants

/// Maximum nodes allowed for inline indexing (updateIndex).
/// Beyond this limit, use batch indexing (scanItem) instead.
public let hnswMaxInlineNodes: Int64 = 10_000

private struct HNSWStagedVector: Sendable {
    let label: UInt64
    let vector: Vector
}

// MARK: - HNSW Parameters

/// HNSW construction parameters
///
/// **M (Maximum Connections)**: Number of bi-directional links created for every new element
/// - Higher M → Better recall, slower build, more memory
/// - Typical: 16-64
///
/// **efConstruction**: Size of dynamic candidate list during construction
/// - Higher efConstruction → Better recall, slower build
/// - Typical: 100-400
///
/// **efSearch**: Default size of dynamic candidate list during search
/// - Higher efSearch → Better recall, slower search
/// - Typical: 50-200
public struct HNSWParameters: Sendable {
    public let m: Int
    public let efConstruction: Int
    public let efSearch: Int

    public static let `default` = HNSWParameters(m: 16, efConstruction: 200, efSearch: 50)

    public init(m: Int = 16, efConstruction: Int = 200, efSearch: Int = 50) {
        self.m = m
        self.efConstruction = efConstruction
        self.efSearch = efSearch
    }

    /// Convert to SwiftHNSW configuration
    internal var hnswConfiguration: HNSWConfiguration {
        HNSWConfiguration(
            m: m,
            efConstruction: efConstruction,
            efSearch: efSearch,
            allowReplaceDeleted: true
        )
    }
}

/// Search-time parameters for HNSW
///
/// **ef (exploration factor)**: Size of dynamic candidate list during search
/// - Higher ef → Better recall, slower search
/// - Lower ef → Faster search, worse recall
/// - Must be >= k (number of nearest neighbors)
/// - Typical: ef = k * 1.5 to k * 3
public struct HNSWSearchParameters: Sendable {
    /// Size of dynamic candidate list during search
    ///
    /// **Recommendation**: ef >= k (k = number of results)
    /// - For recall ~90%: ef ≈ k * 1.5
    /// - For recall ~95%: ef ≈ k * 2
    /// - For recall ~99%: ef ≈ k * 3
    public let ef: Int

    /// Initialize search parameters
    ///
    /// - Parameter ef: Exploration factor (default: 50)
    public init(ef: Int = 50) {
        self.ef = ef
    }
}

// MARK: - HNSW Index Maintainer

/// HNSW (Hierarchical Navigable Small World) index maintainer using SwiftHNSW
///
/// **Architecture**:
/// This maintainer uses the SwiftHNSW library for high-performance ANN search.
/// The HNSW graph is stored serialized in FDB and loaded into memory for search.
///
/// **Storage Layout**:
/// ```
/// [indexSubspace]/vectors/[label] = Float32 binary payload    // Vector storage
/// [indexSubspace]/labels/[primaryKey] = UInt64                // PK to label mapping
/// [indexSubspace]/pks/[label] = primaryKey                    // Label to PK mapping
/// [indexSubspace]/_graphMetadata = Tuple                      // Chunked graph metadata
/// [indexSubspace]/_graphChunks/[chunk] = Data                  // Chunked graph snapshot
/// [indexSubspace]/metadata = JSON                             // Index metadata
/// ```
///
/// **Usage**:
/// - For small datasets (<10K vectors): inline indexing via updateIndex()
/// - For large datasets: batch indexing via OnlineIndexer with scanItems()
public struct HNSWIndexMaintainer<Item: PersistedEntityValue>: IndexMaintainer {
    public let index: ResolvedIndex
    public let subspace: Subspace
    public let idExpression: KeyExpression

    // HNSW parameters
    private let parameters: HNSWParameters
    private let dimensions: Int
    private let storage: HNSWIndexStorage

    private var vectorsSubspace: Subspace { storage.vectorsSubspace }
    private var labelsSubspace: Subspace { storage.labelsSubspace }
    private var primaryKeysSubspace: Subspace { storage.primaryKeysSubspace }
    private var nextLabelKey: ByteString { storage.nextLabelKey }

    public init(
        index: ResolvedIndex,
        dimensions: Int,
        metric: VectorMetric,
        subspace: Subspace,
        idExpression: KeyExpression,
        parameters: HNSWParameters = .default,
        resourceLimits: HNSWGraphResourceLimits = .default
    ) {
        self.init(
            index: index,
            dimensions: dimensions,
            metric: metric,
            subspace: subspace,
            idExpression: idExpression,
            parameters: parameters,
            graphCache: HNSWGraphCache(),
            resourceLimits: resourceLimits
        )
    }

    internal init(
        index: ResolvedIndex,
        dimensions: Int,
        metric: VectorMetric,
        subspace: Subspace,
        idExpression: KeyExpression,
        parameters: HNSWParameters,
        graphCache: HNSWGraphCache,
        resourceLimits: HNSWGraphResourceLimits
    ) {
        self.index = index
        self.subspace = subspace
        self.idExpression = idExpression
        self.parameters = parameters
        self.dimensions = dimensions
        self.storage = HNSWIndexStorage(
            subspace: subspace,
            dimensions: dimensions,
            metric: metric,
            parameters: parameters,
            graphCache: graphCache,
            resourceLimits: resourceLimits
        )
    }

    // MARK: - IndexMaintainer Protocol

    public func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionAccess
    ) async throws {
        if let oldItem, let newItem {
            let oldID = try DataAccess.extractId(
                from: oldItem,
                using: idExpression
            )
            let newID = try DataAccess.extractId(
                from: newItem,
                using: idExpression
            )
            if oldID.pack() == newID.pack() {
                do {
                    let vector = try extractVector(from: newItem)
                    try await insertVector(
                        primaryKey: newID,
                        vector: vector,
                        transaction: transaction
                    )
                } catch DataAccessError.nilValueCannotBeIndexed {
                    try await deleteVector(
                        primaryKey: oldID,
                        transaction: transaction
                    )
                }
                return
            }
        }

        // Handle deletion
        // Sparse index: if vector field was nil, there's no entry to delete
        if let oldItem = oldItem {
            let oldId = try DataAccess.extractId(from: oldItem, using: idExpression)
            try await deleteVector(primaryKey: oldId, transaction: transaction)
        }

        // Handle insertion/update
        // Sparse index: if vector field is nil, skip indexing
        if let newItem = newItem {
            do {
                let primaryKey = try DataAccess.extractId(from: newItem, using: idExpression)
                let vector = try extractVector(from: newItem)
                try await insertVector(primaryKey: primaryKey, vector: vector, transaction: transaction)
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
        // Sparse index: if vector field is nil, skip indexing
        do {
            let vector = try extractVector(from: item)
            try await insertVector(primaryKey: id, vector: vector, transaction: transaction)
        } catch DataAccessError.nilValueCannotBeIndexed {
            // Sparse index: nil vector is not indexed
        }
    }

    public func scanItems(
        _ items: [(item: Item, id: Tuple)],
        transaction: any TransactionAccess
    ) async throws {
        var stagedVectors: [HNSWStagedVector] = []
        stagedVectors.reserveCapacity(items.count)

        for entry in items {
            do {
                let vector = try extractVector(from: entry.item)
                let stagedVector = try await stageVector(
                    primaryKey: entry.id,
                    vector: vector,
                    transaction: transaction
                )
                stagedVectors.append(stagedVector)
            } catch DataAccessError.nilValueCannotBeIndexed {
                // Sparse index: nil vector is not indexed
            }
        }

        guard !stagedVectors.isEmpty else {
            return
        }

        let hnswIndex = try await loadOrCreateIndex(
            transaction: transaction
        )
        try add(stagedVectors, to: hnswIndex)
        try await saveIndex(hnswIndex, transaction: transaction)
    }

    /// HNSW cannot compute index keys without a transaction: the persistent key
    /// uses a monotonically allocated `UInt64` label, and the label mapping is
    /// stored in the index subspace.
    ///
    /// The scrubber's real entry point is the transaction-aware overload below,
    /// which this maintainer overrides to produce the actual key. Keeping this
    /// variant explicit (rather than inheriting the `IndexMaintainer` default)
    /// Returning an empty key set would incorrectly identify every non-sparse
    /// item as intentionally unindexed, so this overload reports the missing
    /// verification capability explicitly.
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

        let labelKey = labelsSubspace.pack(id)
        guard let label = try await getLabelForPrimaryKey(
            primaryKey: id,
            transaction: transaction
        ) else {
            throw VectorIndexError.invalidStructure(
                "HNSW label mapping is missing; a full index rebuild is required"
            )
        }
        try await storage.validateStoredEntry(
            label: label,
            primaryKey: id,
            transaction: transaction
        )
        return [
            labelKey,
            vectorsSubspace.pack(HNSWLabelCodec.tuple(label)),
            primaryKeysSubspace.pack(HNSWLabelCodec.tuple(label)),
        ]
    }

    // MARK: - Vector Operations

    /// Insert a vector into the index
    private func insertVector(
        primaryKey: Tuple,
        vector: Vector,
        transaction: any TransactionAccess
    ) async throws {
        let stagedVector = try await stageVector(
            primaryKey: primaryKey,
            vector: vector,
            transaction: transaction
        )

        // Load existing graph, add vector, and save back.
        let hnswIndex = try await loadOrCreateIndex(
            transaction: transaction
        )
        try add([stagedVector], to: hnswIndex)
        try await saveIndex(hnswIndex, transaction: transaction)
    }

    /// Stage vector storage and label mappings inside the current transaction.
    private func stageVector(
        primaryKey: Tuple,
        vector: Vector,
        transaction: any TransactionAccess
    ) async throws -> HNSWStagedVector {
        // Get or create label for this primary key
        let label = try await getOrCreateLabel(for: primaryKey, transaction: transaction)

        // Store vector data
        let vectorKey = vectorsSubspace.pack(HNSWLabelCodec.tuple(label))
        try transaction.setValue(
            VectorConversion.float32VectorToBytes(vector),
            for: vectorKey
        )

        // Store bidirectional mapping
        let labelKey = labelsSubspace.pack(primaryKey)
        try transaction.setValue(HNSWLabelCodec.tuple(label).pack(), for: labelKey)

        let pkKey = primaryKeysSubspace.pack(HNSWLabelCodec.tuple(label))
        try transaction.setValue(primaryKey.pack(), for: pkKey)

        return HNSWStagedVector(
            label: label,
            vector: try storage.graphVector(from: vector)
        )
    }

    /// Add staged vectors to an in-memory HNSW graph.
    private func add(
        _ stagedVectors: [HNSWStagedVector],
        to hnswIndex: HNSWIndexF32
    ) throws {
        for stagedVector in stagedVectors {
            guard let result = try stagedVector.vector.withFloat32Elements({ buffer in
                do {
                    try hnswIndex.add(buffer, label: stagedVector.label)
                } catch HNSWError.capacityExceeded {
                    try ensureCapacity(hnswIndex, additionalCount: 1)
                    try hnswIndex.add(buffer, label: stagedVector.label)
                }
                return ()
            }) else {
                throw VectorIndexError.invalidStructure(
                    "Staged HNSW vector does not contain Float32 elements"
                )
            }
            _ = result
        }
    }

    /// Delete a vector from the index
    private func deleteVector(
        primaryKey: Tuple,
        transaction: any TransactionAccess
    ) async throws {
        // Get label for this primary key
        guard let label = try await getLabelForPrimaryKey(primaryKey: primaryKey, transaction: transaction) else {
            return  // Not found, nothing to delete
        }

        // Clear vector data
        let vectorKey = vectorsSubspace.pack(HNSWLabelCodec.tuple(label))
        try transaction.clear(key: vectorKey)

        // Clear bidirectional mapping
        let labelKey = labelsSubspace.pack(primaryKey)
        try transaction.clear(key: labelKey)

        let pkKey = primaryKeysSubspace.pack(HNSWLabelCodec.tuple(label))
        try transaction.clear(key: pkKey)

        // Load graph, mark as deleted, and save back
        let hnswIndex = try await loadOrCreateIndex(transaction: transaction)
        try hnswIndex.markDeleted(label: label)
        try await saveIndex(hnswIndex, transaction: transaction)
    }

    // MARK: - Search Operations

    /// Search for k nearest neighbors
    ///
    /// - Parameters:
    ///   - queryVector: Query vector (must match dimensions)
    ///   - k: Number of nearest neighbors to return
    ///   - searchParams: Search parameters (ef)
    ///   - transaction: FDB transaction
    /// - Returns: Array of (primaryKey, distance) sorted by distance ascending
    public func search(
        queryVector: [Float],
        k: Int,
        searchParams: HNSWSearchParameters,
        transaction: any TransactionReadAccess
    ) async throws -> [(primaryKey: [any TupleElement], distance: Double)] {
        let retainedQuery = try Vector(float32: queryVector)
        return try await search(
            queryVector: retainedQuery,
            k: k,
            searchParams: searchParams,
            transaction: transaction
        )
    }

    func search(
        queryVector: Vector,
        k: Int,
        searchParams: HNSWSearchParameters,
        transaction: any TransactionReadAccess
    ) async throws -> [(primaryKey: [any TupleElement], distance: Double)] {
        try await HNSWIndexReader(storage: storage).search(
            queryVector: queryVector,
            k: k,
            parameters: searchParams,
            transaction: transaction
        )
    }

    /// Search with default parameters
    public func search(
        queryVector: [Float],
        k: Int,
        transaction: any TransactionReadAccess
    ) async throws -> [(primaryKey: [any TupleElement], distance: Double)] {
        let searchParams = HNSWSearchParameters(ef: max(k, parameters.efSearch))
        return try await search(
            queryVector: queryVector,
            k: k,
            searchParams: searchParams,
            transaction: transaction
        )
    }

    func search(
        queryVector: Vector,
        k: Int,
        transaction: any TransactionReadAccess
    ) async throws -> [(primaryKey: [any TupleElement], distance: Double)] {
        let searchParams = HNSWSearchParameters(
            ef: max(k, parameters.efSearch)
        )
        return try await search(
            queryVector: queryVector,
            k: k,
            searchParams: searchParams,
            transaction: transaction
        )
    }

    // MARK: - Post-Filtered Search

    /// Search an expanded HNSW candidate set and then apply a predicate.
    ///
    /// Uses expanded ef to ensure sufficient candidates pass the filter.
    ///
    /// - Parameters:
    ///   - queryVector: Query vector for similarity search
    ///   - k: Number of nearest neighbors to return
    ///   - predicate: Filter predicate
    ///   - fetchItem: Function to fetch item by primary key
    ///   - postFilterParameters: Candidate expansion and predicate evaluation limits
    ///   - searchParams: HNSW search parameters
    ///   - transaction: FDB transaction
    /// - Returns: Array of (primaryKey, distance) for items passing the predicate
    public func searchWithPostFilter(
        queryVector: [Float],
        k: Int,
        predicate: @escaping @Sendable (Item) async throws -> Bool,
        fetchItem: @escaping @Sendable (Tuple) async throws -> Item?,
        postFilterParameters: HNSWPostFilterParameters = .default,
        searchParams: HNSWSearchParameters = HNSWSearchParameters(),
        transaction: any TransactionReadAccess
    ) async throws -> [(primaryKey: [any TupleElement], distance: Double)] {
        let retainedQuery = try Vector(float32: queryVector)
        return try await searchWithPostFilter(
            queryVector: retainedQuery,
            k: k,
            predicate: predicate,
            fetchItem: fetchItem,
            postFilterParameters: postFilterParameters,
            searchParams: searchParams,
            transaction: transaction
        )
    }

    func searchWithPostFilter(
        queryVector: Vector,
        k: Int,
        predicate: @escaping @Sendable (Item) async throws -> Bool,
        fetchItem: @escaping @Sendable (Tuple) async throws -> Item?,
        postFilterParameters: HNSWPostFilterParameters = .default,
        searchParams: HNSWSearchParameters = HNSWSearchParameters(),
        transaction: any TransactionReadAccess
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

        guard searchParams.ef > 0 else {
            throw VectorIndexError.invalidArgument("ef must be positive")
        }

        guard postFilterParameters.expansionFactor > 0 else {
            throw VectorIndexError.invalidArgument(
                "post-filter expansion factor must be positive"
            )
        }
        if let maximum = postFilterParameters.maxPredicateEvaluations,
           maximum < 0 {
            throw VectorIndexError.invalidArgument(
                "maximum predicate evaluations must not be negative"
            )
        }
        let (candidateMultiplier, multiplierOverflow) =
            postFilterParameters.expansionFactor.multipliedReportingOverflow(by: 2)
        let (expandedK, candidateCountOverflow) = k.multipliedReportingOverflow(
            by: candidateMultiplier
        )
        guard !multiplierOverflow, !candidateCountOverflow else {
            throw VectorIndexError.invalidArgument(
                "post-filter candidate count exceeds the current platform limit"
            )
        }
        let (expandedEf, explorationOverflow) = max(
            expandedK,
            searchParams.ef
        ).multipliedReportingOverflow(
            by: postFilterParameters.expansionFactor
        )
        guard !explorationOverflow else {
            throw VectorIndexError.invalidArgument(
                "post-filter exploration count exceeds the current platform limit"
            )
        }

        let graphQueryVector = try storage.graphVector(from: queryVector)
        let snapshot = try await storage.loadSearchSnapshot(transaction: transaction)
        let results = try snapshot.search(
            queryVector: graphQueryVector,
            k: expandedK,
            efSearch: expandedEf
        )

        // Filter results
        var output: [(primaryKey: [any TupleElement], distance: Double)] = []
        var predicateEvaluations = 0

        for result in results {
            // Check predicate evaluation limit
            if let maxEvals = postFilterParameters.maxPredicateEvaluations,
               predicateEvaluations >= maxEvals {
                break
            }

            guard let pk = snapshot.primaryKeysByLabel[result.label] else {
                throw VectorIndexError.invalidStructure(
                    "HNSW filtered search result references an unknown primary-key label"
                )
            }

            // Fetch item and evaluate predicate
            if let item = try await fetchItem(pk) {
                predicateEvaluations += 1
                let passes = try await predicate(item)

                if passes {
                    let elements = try pk.elements()
                    output.append(
                        (
                            primaryKey: elements,
                            distance: try storage.canonicalDistance(
                                from: result.distance
                            )
                        )
                    )

                    // Stop if we have enough results
                    if output.count >= k {
                        break
                    }
                }
            }
        }

        return output
    }

    // MARK: - Label Management

    /// Get or create a label for a primary key
    private func getOrCreateLabel(
        for primaryKey: Tuple,
        transaction: any TransactionAccess
    ) async throws -> UInt64 {
        // Check if label already exists
        let labelKey = labelsSubspace.pack(primaryKey)
        if let existingValue = try await transaction.getValue(for: labelKey, snapshot: false) {
            return try decodeLabel(existingValue)
        }

        // Allocate new label atomically
        let nextLabel = try await getNextLabel(transaction: transaction)
        return nextLabel
    }

    /// Get the next available label
    private func getNextLabel(transaction: any TransactionAccess) async throws -> UInt64 {
        let currentValue = try await transaction.getValue(for: nextLabelKey, snapshot: false)
        let current: UInt64
        if let value = currentValue {
            do {
                current = try bytesToUInt64(value)
            } catch {
                throw VectorIndexError.invalidStructure(
                    "Invalid HNSW next-label value"
                )
            }
        } else {
            current = 0
        }

        let (next, overflow) = current.addingReportingOverflow(1)
        guard !overflow else {
            throw VectorIndexError.invalidStructure("HNSW label space exhausted")
        }
        try transaction.setValue(uint64ToBytes(next), for: nextLabelKey)
        return current
    }

    /// Get label for a primary key
    private func getLabelForPrimaryKey(
        primaryKey: Tuple,
        transaction: (any TransactionAccess)?
    ) async throws -> UInt64? {
        guard let tx = transaction else { return nil }

        let labelKey = labelsSubspace.pack(primaryKey)
        guard let value = try await tx.getValue(for: labelKey, snapshot: true) else {
            return nil
        }

        return try decodeLabel(value)
    }

    private func decodeLabel(_ value: ByteString) throws -> UInt64 {
        try HNSWLabelCodec.decodePacked(value)
    }

    // MARK: - Index Persistence

    /// Load or create HNSW index
    private func loadOrCreateIndex(
        transaction: any TransactionAccess,
        additionalCapacity: Int = 0
    ) async throws -> HNSWIndexF32 {
        try await storage.loadOrCreateIndex(
            transaction: transaction,
            additionalCapacity: additionalCapacity
        )
    }

    /// Resize the graph before adding a batch when the saved capacity is full.
    private func ensureCapacity(
        _ index: HNSWIndexF32,
        additionalCount: Int
    ) throws {
        try storage.ensureCapacity(index, additionalCount: additionalCount)
    }

    /// Save HNSW index to FDB
    private func saveIndex(
        _ index: HNSWIndexF32,
        transaction: any TransactionAccess
    ) async throws {
        try await storage.saveIndex(index, transaction: transaction)
    }

    // MARK: - Helper Methods

    /// Get the current node count in the HNSW index
    ///
    /// - Parameter transaction: FDB transaction
    /// - Returns: Number of nodes in the index
    public func getNodeCount(
        transaction: any TransactionAccess
    ) async throws -> Int {
        let hnswIndex = try await loadOrCreateIndex(transaction: transaction)
        return hnswIndex.count
    }

    /// Extract vector from item using VectorConversion
    public func extractVector(from item: Item) throws -> Vector {
        let fieldValues = try DataAccess.evaluate(
            item: item,
            expression: index.rootExpression
        )

        let result = try VectorConversion.extractFloat32Vector(
            from: fieldValues
        )

        guard result.count == dimensions else {
            throw VectorIndexError.dimensionMismatch(
                expected: dimensions,
                actual: result.count
            )
        }

        return result
    }

    // MARK: - Byte Conversion

    private func uint64ToBytes(_ value: UInt64) -> ByteString {
        VectorConversion.uint64ToBytes(value)
    }

    private func bytesToUInt64(_ bytes: ByteString) throws -> UInt64 {
        try VectorConversion.bytesToUInt64(bytes)
    }

}

// MARK: - VectorMetric Extension

extension VectorMetric {
    /// Convert to SwiftHNSW distance metric
    var toHNSWMetric: DistanceMetric {
        switch self {
        case .cosine:
            return .cosine
        case .euclidean:
            return .l2
        case .dotProduct:
            return .innerProduct
        }
    }
}
