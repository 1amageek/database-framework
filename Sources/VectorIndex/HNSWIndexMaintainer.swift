// HNSWIndexMaintainer.swift
// VectorIndex - HNSW index maintainer using swift-hnsw library
//
// Provides high-performance approximate nearest neighbor search using the
// SwiftHNSW library (https://github.com/1amageek/swift-hnsw).

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import Core
import DatabaseEngine
import StorageKit
import Logging
import Synchronization
import SwiftHNSW
import Vector

// MARK: - HNSW Constants

/// Maximum nodes allowed for inline indexing (updateIndex).
/// Beyond this limit, use batch indexing (scanItem) instead.
public let hnswMaxInlineNodes: Int64 = 10_000

private let hnswGraphSnapshotVersion: Int64 = 1
private let hnswGraphSnapshotChunkSize = 80 * 1024

private struct HNSWStagedVector: Sendable {
    let label: UInt64
    let vector: [Float]
}

private struct HNSWGraphMetadata: Sendable {
    let version: Int64
    let byteCount: Int
    let chunkSize: Int
    let chunkCount: Int
    let revision: Int64
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
        HNSWConfiguration(m: m, efConstruction: efConstruction, efSearch: efSearch)
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
public struct HNSWIndexMaintainer<Item: Persistable>: IndexMaintainer {
    public let index: Index
    public let subspace: Subspace
    public let idExpression: KeyExpression

    // HNSW parameters
    private let parameters: HNSWParameters
    private let dimensions: Int
    private let metric: VectorMetric
    private let graphCache: HNSWGraphCache

    // Subspace keys
    private let vectorsSubspace: Subspace
    private let labelsSubspace: Subspace
    private let primaryKeysSubspace: Subspace
    private let graphChunksSubspace: Subspace
    private let graphMetadataKey: Bytes
    private let nextLabelKey: Bytes

    public init(
        index: Index,
        dimensions: Int,
        metric: VectorMetric,
        subspace: Subspace,
        idExpression: KeyExpression,
        parameters: HNSWParameters = .default
    ) {
        self.init(
            index: index,
            dimensions: dimensions,
            metric: metric,
            subspace: subspace,
            idExpression: idExpression,
            parameters: parameters,
            graphCache: HNSWGraphCache()
        )
    }

    internal init(
        index: Index,
        dimensions: Int,
        metric: VectorMetric,
        subspace: Subspace,
        idExpression: KeyExpression,
        parameters: HNSWParameters,
        graphCache: HNSWGraphCache
    ) {
        self.index = index
        self.subspace = subspace
        self.idExpression = idExpression
        self.parameters = parameters
        self.dimensions = dimensions
        self.metric = metric
        self.graphCache = graphCache

        // Initialize subspaces
        self.vectorsSubspace = subspace.subspace("v")
        self.labelsSubspace = subspace.subspace("l")
        self.primaryKeysSubspace = subspace.subspace("p")
        self.graphChunksSubspace = subspace.subspace("_graphChunks")
        self.graphMetadataKey = subspace.pack(Tuple("_graphMetadata"))
        self.nextLabelKey = subspace.pack(Tuple("_nextLabel"))
    }

    // MARK: - IndexMaintainer Protocol

    public func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionAccess
    ) async throws {
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
            transaction: transaction,
            additionalCapacity: stagedVectors.count
        )
        try add(stagedVectors, to: hnswIndex)
        try await saveIndex(hnswIndex, transaction: transaction)
    }

    /// HNSW cannot compute index keys without a transaction: the persistent key
    /// uses a monotonically-allocated `UInt64` label, and the label-for-primary-key
    /// mapping lives in FDB under `labelsSubspace`. Without transactional access
    /// we cannot read the mapping (see `getLabelForPrimaryKey` — it returns `nil`
    /// whenever `transaction == nil`), so this variant always returns `[]`, which
    /// the `OnlineIndexScrubber` interprets as "verification opted out for this
    /// item through this code path".
    ///
    /// The scrubber's real entry point is the transaction-aware overload below,
    /// which this maintainer overrides to produce the actual key. Keeping this
    /// variant explicit (rather than inheriting the `IndexMaintainer` default)
    /// documents the opt-out at the point of decision so future refactors don't
    /// mistake it for an oversight.
    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [Bytes] {
        return []
    }

    public func computeIndexKeys(
        for item: Item,
        id: Tuple,
        transaction: any TransactionAccess
    ) async throws -> [Bytes] {
        guard let label = try await getLabelForPrimaryKey(primaryKey: id, transaction: transaction) else {
            // No label assigned yet — either the item was never indexed (sparse/unseen)
            // or it was deleted. Either way there is no key to verify.
            return []
        }
        return [vectorsSubspace.pack(Tuple(Int64(label)))]
    }

    // MARK: - Vector Operations

    /// Insert a vector into the index
    private func insertVector(
        primaryKey: Tuple,
        vector: [Float],
        transaction: any TransactionAccess
    ) async throws {
        let stagedVector = try await stageVector(
            primaryKey: primaryKey,
            vector: vector,
            transaction: transaction
        )

        // Load existing graph, add vector, and save back.
        let hnswIndex = try await loadOrCreateIndex(
            transaction: transaction,
            additionalCapacity: 1
        )
        try add([stagedVector], to: hnswIndex)
        try await saveIndex(hnswIndex, transaction: transaction)
    }

    /// Stage vector storage and label mappings inside the current transaction.
    private func stageVector(
        primaryKey: Tuple,
        vector: [Float],
        transaction: any TransactionAccess
    ) async throws -> HNSWStagedVector {
        // Get or create label for this primary key
        let label = try await getOrCreateLabel(for: primaryKey, transaction: transaction)

        // Store vector data
        let vectorKey = vectorsSubspace.pack(Tuple(Int64(label)))
        try transaction.setValue(VectorConversion.floatArrayToBytes(vector), for: vectorKey)

        // Store bidirectional mapping
        let labelKey = labelsSubspace.pack(primaryKey)
        try transaction.setValue(Tuple(Int64(label)).pack(), for: labelKey)

        let pkKey = primaryKeysSubspace.pack(Tuple(Int64(label)))
        try transaction.setValue(primaryKey.pack(), for: pkKey)

        return HNSWStagedVector(label: label, vector: vector)
    }

    /// Add staged vectors to an in-memory HNSW graph.
    private func add(
        _ stagedVectors: [HNSWStagedVector],
        to hnswIndex: HNSWIndexF32
    ) throws {
        try ensureCapacity(
            hnswIndex,
            additionalCount: stagedVectors.count
        )

        for stagedVector in stagedVectors {
            try stagedVector.vector.withUnsafeBufferPointer { buffer in
                try hnswIndex.add(buffer, label: stagedVector.label)
            }
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
        let vectorKey = vectorsSubspace.pack(Tuple(Int64(label)))
        try transaction.clear(key: vectorKey)

        // Clear bidirectional mapping
        let labelKey = labelsSubspace.pack(primaryKey)
        try transaction.clear(key: labelKey)

        let pkKey = primaryKeysSubspace.pack(Tuple(Int64(label)))
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

        let snapshot = try await loadSearchSnapshot(transaction: transaction)
        let results = try snapshot.search(
            queryVector: queryVector,
            k: k,
            efSearch: searchParams.ef
        )

        var output: [(primaryKey: [any TupleElement], distance: Double)] = []
        output.reserveCapacity(results.count)

        for result in results {
            guard let pk = snapshot.primaryKeysByLabel[result.label] else {
                throw VectorIndexError.invalidStructure(
                    "HNSW search result references an unknown primary-key label"
                )
            }
            let elements = try pk.elements()
            output.append((primaryKey: elements, distance: Double(result.distance)))
        }

        return output
    }

    /// Search with default parameters
    public func search(
        queryVector: [Float],
        k: Int,
        transaction: any TransactionAccess
    ) async throws -> [(primaryKey: [any TupleElement], distance: Double)] {
        let searchParams = HNSWSearchParameters(ef: max(k, parameters.efSearch))
        return try await search(
            queryVector: queryVector,
            k: k,
            searchParams: searchParams,
            transaction: transaction
        )
    }

    // MARK: - ACORN Filtered Search

    /// Search with predicate filter (ACORN algorithm)
    ///
    /// Uses expanded ef to ensure sufficient candidates pass the filter.
    ///
    /// - Parameters:
    ///   - queryVector: Query vector for similarity search
    ///   - k: Number of nearest neighbors to return
    ///   - predicate: Filter predicate
    ///   - fetchItem: Function to fetch item by primary key
    ///   - acornParams: ACORN parameters
    ///   - searchParams: HNSW search parameters
    ///   - transaction: FDB transaction
    /// - Returns: Array of (primaryKey, distance) for items passing the predicate
    public func searchWithFilter(
        queryVector: [Float],
        k: Int,
        predicate: @escaping @Sendable (Item) async throws -> Bool,
        fetchItem: @escaping @Sendable (Tuple, any TransactionAccess) async throws -> Item?,
        acornParams: ACORNParameters = .default,
        searchParams: HNSWSearchParameters = HNSWSearchParameters(),
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

        // Expand ef for filtered search
        let expandedK = k * acornParams.expansionFactor * 2
        let expandedEf = max(expandedK, searchParams.ef) * acornParams.expansionFactor

        let snapshot = try await loadSearchSnapshot(transaction: transaction)
        let results = try snapshot.search(
            queryVector: queryVector,
            k: expandedK,
            efSearch: expandedEf
        )

        // Filter results
        var output: [(primaryKey: [any TupleElement], distance: Double)] = []
        var predicateEvaluations = 0

        for result in results {
            // Check predicate evaluation limit
            if let maxEvals = acornParams.maxPredicateEvaluations,
               predicateEvaluations >= maxEvals {
                break
            }

            guard let pk = snapshot.primaryKeysByLabel[result.label] else {
                throw VectorIndexError.invalidStructure(
                    "HNSW filtered search result references an unknown primary-key label"
                )
            }

            // Fetch item and evaluate predicate
            if let item = try await fetchItem(pk, transaction) {
                predicateEvaluations += 1
                let passes = try await predicate(item)

                if passes {
                    let elements = try pk.elements()
                    output.append((primaryKey: elements, distance: Double(result.distance)))

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
            let labelTuple = try Tuple.unpack(from: existingValue)
            if let label = labelTuple[0] as? Int64 {
                return UInt64(label)
            }
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
            current = try bytesToUInt64(value)
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

        let labelTuple = try Tuple.unpack(from: value)
        if let label = labelTuple[0] as? Int64 {
            return UInt64(label)
        }
        return nil
    }

    // MARK: - Index Persistence

    /// Load or create HNSW index
    private func loadOrCreateIndex(
        transaction: any TransactionAccess,
        additionalCapacity: Int = 0
    ) async throws -> HNSWIndexF32 {
        // Try to load existing index
        if let graphData = try await loadGraphSnapshotData(transaction: transaction) {
            let index = try loadPersistedIndex(from: graphData)
            try ensureCapacity(index, additionalCount: additionalCapacity)
            return index
        }

        // Create new index
        // Estimate max elements based on current data or use default
        let maxElements = max(
            try await estimateMaxElements(transaction: transaction),
            additionalCapacity
        )
        let index = try HNSWIndexF32(
            dimensions: dimensions,
            maxElements: maxElements,
            metric: metric.toHNSWMetric,
            configuration: parameters.hnswConfiguration
        )
        return index
    }

    /// Resize the graph before adding a batch when the saved capacity is full.
    private func ensureCapacity(
        _ index: HNSWIndexF32,
        additionalCount: Int
    ) throws {
        guard additionalCount > 0 else {
            return
        }

        let requiredCapacity = index.count + additionalCount
        guard requiredCapacity > index.capacity else {
            return
        }

        var nextCapacity = max(index.capacity, 1)
        while nextCapacity < requiredCapacity {
            nextCapacity *= 2
        }

        try index.resize(to: nextCapacity)
    }

    /// Save HNSW index to FDB
    private func saveIndex(
        _ index: HNSWIndexF32,
        transaction: any TransactionAccess
    ) async throws {
        let graphData = try index.serialize()
        try await saveGraphSnapshot(graphData, transaction: transaction)
    }

    /// Load a cached search snapshot or construct one from the persisted graph.
    private func loadSearchSnapshot(transaction: any TransactionAccess) async throws -> HNSWGraphCache.Snapshot {
        if let metadataBytes = try await transaction.getValue(for: graphMetadataKey, snapshot: true) {
            let cacheKey = HNSWGraphCache.Key(
                subspacePrefix: subspace.prefix,
                dimensions: dimensions,
                metric: metric.toHNSWMetric.rawValue,
                metadata: metadataBytes
            )

            if let cached = graphCache.get(cacheKey) {
                return cached
            }

            let graphData = try await loadChunkedGraphSnapshot(metadata: metadataBytes, transaction: transaction)
            let index = try loadPersistedIndex(from: graphData)
            let primaryKeysByLabel = try await loadPrimaryKeysByLabel(transaction: transaction)
            let snapshot = HNSWGraphCache.Snapshot(index: index, primaryKeysByLabel: primaryKeysByLabel)
            graphCache.set(
                snapshot,
                for: cacheKey,
                cost: graphData.count + primaryKeysByLabel.count * 64
            )
            return snapshot
        }

        let index = try await loadOrCreateIndex(transaction: transaction)
        let primaryKeysByLabel = try await loadPrimaryKeysByLabel(transaction: transaction)
        return HNSWGraphCache.Snapshot(index: index, primaryKeysByLabel: primaryKeysByLabel)
    }

    /// Decode a persisted graph snapshot and surface corruption as a typed vector-index error.
    private func loadPersistedIndex(from graphBytes: Bytes) throws -> HNSWIndexF32 {
        do {
            return try HNSWIndexF32.load(
                from: dataRetaining(graphBytes),
                dimensions: dimensions,
                metric: metric.toHNSWMetric,
                maxElements: 0
            )
        } catch let error as HNSWError {
            throw VectorIndexError.invalidStructure(
                "Invalid HNSW graph snapshot: \(String(describing: error))"
            )
        } catch {
            throw VectorIndexError.invalidStructure("Invalid HNSW graph snapshot: \(error)")
        }
    }

    /// Load a graph snapshot from storage without consulting the process cache.
    private func loadGraphSnapshotData(transaction: any TransactionAccess) async throws -> Bytes? {
        guard let metadataBytes = try await transaction.getValue(
            for: graphMetadataKey,
            snapshot: true
        ) else {
            return nil
        }
        return try await loadChunkedGraphSnapshot(
            metadata: metadataBytes,
            transaction: transaction
        )
    }

    /// Save a graph snapshot as bounded chunks so backend value-size limits do not corrupt large graphs.
    private func saveGraphSnapshot(
        _ graphData: Data,
        transaction: any TransactionAccess
    ) async throws {
        let chunkSize = hnswGraphSnapshotChunkSize
        let byteCount = graphData.count
        let chunkCount = byteCount == 0
            ? 0
            : (byteCount + chunkSize - 1) / chunkSize

        let range = graphChunksSubspace.range()
        try transaction.clearRange(beginKey: range.begin, endKey: range.end)

        let graphBytes = Bytes(
            retaining: GraphSnapshotByteOwner(data: graphData)
        )

        for chunkIndex in 0..<chunkCount {
            let start = chunkIndex * chunkSize
            let end = min(start + chunkSize, byteCount)
            let chunkKey = graphChunksSubspace.pack(Tuple(Int64(chunkIndex)))
            let chunk = graphBytes[start..<end]
            try transaction.setValue(chunk, for: chunkKey)
        }

        let revision = try await nextGraphSnapshotRevision(transaction: transaction)
        let metadata = Tuple(
            hnswGraphSnapshotVersion,
            Int64(byteCount),
            Int64(chunkSize),
            Int64(chunkCount),
            revision
        )
        try transaction.setValue(metadata.pack(), for: graphMetadataKey)
    }

    /// Decode chunk metadata and reassemble a graph snapshot.
    private func loadChunkedGraphSnapshot(
        metadata: Bytes,
        transaction: any TransactionAccess
    ) async throws -> Bytes {
        let decoded = try decodeGraphMetadata(metadata)

        let byteCount = decoded.byteCount
        let chunkSize = decoded.chunkSize
        let chunkCount = decoded.chunkCount
        let expectedChunkCount = byteCount == 0 ? 0 : (byteCount + chunkSize - 1) / chunkSize
        guard chunkCount == expectedChunkCount else {
            throw VectorIndexError.invalidStructure("HNSW graph snapshot chunk count does not match byte count")
        }

        var output = [UInt8](repeating: 0, count: byteCount)
        var loadedByteCount = 0

        for chunkIndex in 0..<chunkCount {
            let chunkKey = graphChunksSubspace.pack(Tuple(Int64(chunkIndex)))
            guard let chunk = try await transaction.getValue(for: chunkKey, snapshot: true) else {
                throw VectorIndexError.invalidStructure("Missing HNSW graph snapshot chunk \(chunkIndex)")
            }
            let expectedChunkSize = min(
                chunkSize,
                byteCount - loadedByteCount
            )
            guard chunk.count == expectedChunkSize else {
                throw VectorIndexError.invalidStructure(
                    "HNSW graph snapshot chunk \(chunkIndex) has an invalid size"
                )
            }
            output.withUnsafeMutableBytes { destination in
                chunk.withUnsafeBytes { source in
                    let target = UnsafeMutableRawBufferPointer(
                        rebasing: destination[
                            loadedByteCount..<(loadedByteCount + source.count)
                        ]
                    )
                    target.copyMemory(from: source)
                }
            }
            loadedByteCount += expectedChunkSize
        }

        guard loadedByteCount == byteCount else {
            throw VectorIndexError.invalidStructure("HNSW graph snapshot byte count mismatch")
        }
        return Bytes(output)
    }

    /// Decode persisted graph metadata.
    private func decodeGraphMetadata(_ metadata: Bytes) throws -> HNSWGraphMetadata {
        let tuple = try Tuple.unpack(from: metadata)
        guard tuple.count == 4 || tuple.count == 5,
              let version = tuple[0] as? Int64,
              let byteCountValue = tuple[1] as? Int64,
              let chunkSizeValue = tuple[2] as? Int64,
              let chunkCountValue = tuple[3] as? Int64
        else {
            throw VectorIndexError.invalidStructure("Invalid HNSW graph snapshot metadata")
        }

        guard version == hnswGraphSnapshotVersion else {
            throw VectorIndexError.invalidStructure("Unsupported HNSW graph snapshot version \(version)")
        }

        let revisionValue: Int64
        if tuple.count == 5 {
            guard let value = tuple[4] as? Int64 else {
                throw VectorIndexError.invalidStructure("Invalid HNSW graph snapshot revision")
            }
            revisionValue = value
        } else {
            revisionValue = 0
        }

        guard byteCountValue >= 0,
              chunkSizeValue > 0,
              chunkCountValue >= 0,
              byteCountValue <= Int64(Int.max),
              chunkSizeValue <= Int64(Int.max),
              chunkCountValue <= Int64(Int.max)
        else {
            throw VectorIndexError.invalidStructure("Invalid HNSW graph snapshot chunk dimensions")
        }

        return HNSWGraphMetadata(
            version: version,
            byteCount: Int(byteCountValue),
            chunkSize: Int(chunkSizeValue),
            chunkCount: Int(chunkCountValue),
            revision: revisionValue
        )
    }

    /// Allocate a monotonic graph snapshot revision for cache invalidation.
    private func nextGraphSnapshotRevision(transaction: any TransactionAccess) async throws -> Int64 {
        guard let currentMetadata = try await transaction.getValue(for: graphMetadataKey, snapshot: false) else {
            return 1
        }

        let current = try decodeGraphMetadata(currentMetadata)
        return current.revision + 1
    }

    /// Load label-to-primary-key mappings in one range scan for search result materialization.
    private func loadPrimaryKeysByLabel(transaction: any TransactionAccess) async throws -> [UInt64: Tuple] {
        let (begin, end) = primaryKeysSubspace.range()
        let entries = try await transaction.collectRange(
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            snapshot: true
        )

        var primaryKeysByLabel: [UInt64: Tuple] = [:]
        primaryKeysByLabel.reserveCapacity(entries.count)

        for (key, value) in entries {
            let labelTuple = try primaryKeysSubspace.unpack(key)
            guard let labelValue = labelTuple[0] as? Int64 else {
                throw VectorIndexError.invalidStructure("Invalid HNSW primary-key label")
            }
            let primaryKey = try Tuple.unpack(from: value)
            primaryKeysByLabel[UInt64(labelValue)] = Tuple(primaryKey)
        }

        return primaryKeysByLabel
    }

    /// Estimate maximum elements for index sizing
    private func estimateMaxElements(
        transaction: any TransactionAccess
    ) async throws -> Int {
        // Count existing vectors
        let (begin, end) = vectorsSubspace.range()
        let sequence = try await transaction.collectRange(
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            snapshot: true
        )

        var count = 0
        for _ in sequence {
            count += 1
            if count > 100_000 {
                break  // Cap the count for performance
            }
        }

        // Return at least 1000, or 2x current count for growth
        return max(1000, count * 2)
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
    public func extractVector(from item: Item) throws -> [Float] {
        let fieldValues = try DataAccess.evaluate(
            item: item,
            expression: index.rootExpression
        )

        let result = try VectorConversion.extractFloatArray(from: fieldValues)

        guard result.count == dimensions else {
            throw VectorIndexError.dimensionMismatch(
                expected: dimensions,
                actual: result.count
            )
        }

        return result
    }

    // MARK: - Byte Conversion

    private func uint64ToBytes(_ value: UInt64) -> Bytes {
        VectorConversion.uint64ToBytes(value)
    }

    private func bytesToUInt64(_ bytes: Bytes) throws -> UInt64 {
        try VectorConversion.bytesToUInt64(bytes)
    }

    /// Creates a read-only Data view while retaining the Bytes owner.
    private func dataRetaining(_ bytes: Bytes) -> Data {
        guard !bytes.isEmpty else {
            return Data()
        }
        return bytes.withUnsafeBytes { buffer in
            guard let baseAddress = buffer.baseAddress else {
                preconditionFailure("Non-empty graph bytes have no base address")
            }
            return Data(
                bytesNoCopy: UnsafeMutableRawPointer(mutating: baseAddress),
                count: buffer.count,
                deallocator: .custom { _, _ in
                    withExtendedLifetime(bytes) {}
                }
            )
        }
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
