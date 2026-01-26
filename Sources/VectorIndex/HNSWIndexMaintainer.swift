// HNSWIndexMaintainer.swift
// VectorIndex - HNSW index maintainer using swift-hnsw library
//
// Provides high-performance approximate nearest neighbor search using the
// SwiftHNSW library (https://github.com/1amageek/swift-hnsw).

import Foundation
import Core
import DatabaseEngine
import FoundationDB
import Logging
import Synchronization
import SwiftHNSW
import Vector

// MARK: - HNSW Constants

/// Maximum nodes allowed for inline indexing (updateIndex).
/// Beyond this limit, use batch indexing (scanItem) instead.
public let hnswMaxInlineNodes: Int64 = 10_000

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
/// [indexSubspace]/vectors/[label] = Tuple(Float, Float, ...)  // Vector storage
/// [indexSubspace]/labels/[primaryKey] = UInt64                // PK to label mapping
/// [indexSubspace]/pks/[label] = primaryKey                    // Label to PK mapping
/// [indexSubspace]/graph = Data                                // Serialized HNSW graph
/// [indexSubspace]/metadata = JSON                             // Index metadata
/// ```
///
/// **Usage**:
/// - For small datasets (<10K vectors): inline indexing via updateIndex()
/// - For large datasets: batch indexing via OnlineIndexer with scanItem()
public struct HNSWIndexMaintainer<Item: Persistable>: IndexMaintainer {
    public let index: Index
    public let subspace: Subspace
    public let idExpression: KeyExpression

    // HNSW parameters
    private let parameters: HNSWParameters
    private let dimensions: Int
    private let metric: VectorMetric

    // Subspace keys
    private let vectorsSubspace: Subspace
    private let labelsSubspace: Subspace
    private let primaryKeysSubspace: Subspace
    private let graphKey: [UInt8]
    private let metadataKey: [UInt8]
    private let nextLabelKey: [UInt8]

    public init(
        index: Index,
        dimensions: Int,
        metric: VectorMetric,
        subspace: Subspace,
        idExpression: KeyExpression,
        parameters: HNSWParameters = .default
    ) {
        self.index = index
        self.subspace = subspace
        self.idExpression = idExpression
        self.parameters = parameters
        self.dimensions = dimensions
        self.metric = metric

        // Initialize subspaces
        self.vectorsSubspace = subspace.subspace("v")
        self.labelsSubspace = subspace.subspace("l")
        self.primaryKeysSubspace = subspace.subspace("p")
        self.graphKey = subspace.pack(Tuple("_graph"))
        self.metadataKey = subspace.pack(Tuple("_metadata"))
        self.nextLabelKey = subspace.pack(Tuple("_nextLabel"))
    }

    // MARK: - IndexMaintainer Protocol

    public func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionProtocol
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
        transaction: any TransactionProtocol
    ) async throws {
        // Sparse index: if vector field is nil, skip indexing
        do {
            let vector = try extractVector(from: item)
            try await insertVector(primaryKey: id, vector: vector, transaction: transaction)
        } catch DataAccessError.nilValueCannotBeIndexed {
            // Sparse index: nil vector is not indexed
        }
    }

    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [FDB.Bytes] {
        // Return the vector storage key
        guard let label = try? await getLabelForPrimaryKey(primaryKey: id, transaction: nil) else {
            return []
        }
        return [vectorsSubspace.pack(Tuple(Int64(label)))]
    }

    // MARK: - Vector Operations

    /// Insert a vector into the index
    private func insertVector(
        primaryKey: Tuple,
        vector: [Float],
        transaction: any TransactionProtocol
    ) async throws {
        // Get or create label for this primary key
        let label = try await getOrCreateLabel(for: primaryKey, transaction: transaction)

        // Store vector data
        let vectorKey = vectorsSubspace.pack(Tuple(Int64(label)))
        let tupleElements: [any TupleElement] = vector.map { $0 as any TupleElement }
        let vectorValue = Tuple(tupleElements).pack()
        transaction.setValue(vectorValue, for: vectorKey)

        // Store bidirectional mapping
        let labelKey = labelsSubspace.pack(primaryKey)
        transaction.setValue(Tuple(Int64(label)).pack(), for: labelKey)

        let pkKey = primaryKeysSubspace.pack(Tuple(Int64(label)))
        transaction.setValue(primaryKey.pack(), for: pkKey)

        // Load existing graph, add vector, and save back
        let hnswIndex = try await loadOrCreateIndex(transaction: transaction)
        try hnswIndex.add(vector, label: label)
        try await saveIndex(hnswIndex, transaction: transaction)
    }

    /// Delete a vector from the index
    private func deleteVector(
        primaryKey: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        // Get label for this primary key
        guard let label = try await getLabelForPrimaryKey(primaryKey: primaryKey, transaction: transaction) else {
            return  // Not found, nothing to delete
        }

        // Clear vector data
        let vectorKey = vectorsSubspace.pack(Tuple(Int64(label)))
        transaction.clear(key: vectorKey)

        // Clear bidirectional mapping
        let labelKey = labelsSubspace.pack(primaryKey)
        transaction.clear(key: labelKey)

        let pkKey = primaryKeysSubspace.pack(Tuple(Int64(label)))
        transaction.clear(key: pkKey)

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
        transaction: any TransactionProtocol
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

        // Load HNSW index
        let hnswIndex = try await loadOrCreateIndex(transaction: transaction)

        // Set search ef
        hnswIndex.setEfSearch(searchParams.ef)

        // Search
        let results: [SearchResult]
        do {
            results = try hnswIndex.search(queryVector, k: k)
        } catch {
            // Empty index or other error
            return []
        }

        // Convert labels to primary keys
        var output: [(primaryKey: [any TupleElement], distance: Double)] = []
        for result in results {
            if let pk = try await getPrimaryKeyForLabel(label: result.label, transaction: transaction) {
                let elements = try Tuple.unpack(from: pk.pack())
                output.append((primaryKey: elements, distance: Double(result.distance)))
            }
        }

        return output
    }

    /// Search with default parameters
    public func search(
        queryVector: [Float],
        k: Int,
        transaction: any TransactionProtocol
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
        fetchItem: @escaping @Sendable (Tuple, any TransactionProtocol) async throws -> Item?,
        acornParams: ACORNParameters = .default,
        searchParams: HNSWSearchParameters = HNSWSearchParameters(),
        transaction: any TransactionProtocol
    ) async throws -> [(primaryKey: [any TupleElement], distance: Double)] {
        guard queryVector.count == dimensions else {
            throw VectorIndexError.dimensionMismatch(
                expected: dimensions,
                actual: queryVector.count
            )
        }

        // Expand ef for filtered search
        let expandedK = k * acornParams.expansionFactor * 2
        let expandedEf = max(expandedK, searchParams.ef) * acornParams.expansionFactor

        // Load HNSW index
        let hnswIndex = try await loadOrCreateIndex(transaction: transaction)
        hnswIndex.setEfSearch(expandedEf)

        // Search with expanded k
        let results: [SearchResult]
        do {
            results = try hnswIndex.search(queryVector, k: expandedK)
        } catch {
            return []
        }

        // Filter results
        var output: [(primaryKey: [any TupleElement], distance: Double)] = []
        var predicateEvaluations = 0

        for result in results {
            // Check predicate evaluation limit
            if let maxEvals = acornParams.maxPredicateEvaluations,
               predicateEvaluations >= maxEvals {
                break
            }

            guard let pk = try await getPrimaryKeyForLabel(label: result.label, transaction: transaction) else {
                continue
            }

            // Fetch item and evaluate predicate
            if let item = try await fetchItem(pk, transaction) {
                predicateEvaluations += 1
                let passes = try await predicate(item)

                if passes {
                    let elements = try Tuple.unpack(from: pk.pack())
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
        transaction: any TransactionProtocol
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
    private func getNextLabel(transaction: any TransactionProtocol) async throws -> UInt64 {
        let currentValue = try await transaction.getValue(for: nextLabelKey, snapshot: false)
        let current: UInt64
        if let value = currentValue {
            current = bytesToUInt64(value)
        } else {
            current = 0
        }

        let next = current + 1
        transaction.setValue(uint64ToBytes(next), for: nextLabelKey)
        return current
    }

    /// Get label for a primary key
    private func getLabelForPrimaryKey(
        primaryKey: Tuple,
        transaction: (any TransactionProtocol)?
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

    /// Get primary key for a label
    private func getPrimaryKeyForLabel(
        label: UInt64,
        transaction: any TransactionProtocol
    ) async throws -> Tuple? {
        let pkKey = primaryKeysSubspace.pack(Tuple(Int64(label)))
        guard let value = try await transaction.getValue(for: pkKey, snapshot: true) else {
            return nil
        }

        let elements = try Tuple.unpack(from: value)
        return Tuple(elements)
    }

    // MARK: - Index Persistence

    /// Load or create HNSW index
    private func loadOrCreateIndex(
        transaction: any TransactionProtocol
    ) async throws -> HNSWIndexF32 {
        // Try to load existing index
        if let graphData = try await transaction.getValue(for: graphKey, snapshot: true) {
            do {
                let index = try HNSWIndexF32.load(
                    from: Data(graphData),
                    dimensions: dimensions,
                    metric: metric.toHNSWMetric,
                    maxElements: 0  // Use saved value
                )
                return index
            } catch {
                // Corrupted graph, create new
            }
        }

        // Create new index
        // Estimate max elements based on current data or use default
        let maxElements = try await estimateMaxElements(transaction: transaction)
        let index = try HNSWIndexF32(
            dimensions: dimensions,
            maxElements: maxElements,
            metric: metric.toHNSWMetric,
            configuration: parameters.hnswConfiguration
        )
        return index
    }

    /// Save HNSW index to FDB
    private func saveIndex(
        _ index: HNSWIndexF32,
        transaction: any TransactionProtocol
    ) async throws {
        let graphData = try index.serialize()
        transaction.setValue(Array(graphData), for: graphKey)
    }

    /// Estimate maximum elements for index sizing
    private func estimateMaxElements(
        transaction: any TransactionProtocol
    ) async throws -> Int {
        // Count existing vectors
        let (begin, end) = vectorsSubspace.range()
        let sequence = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(begin),
            endSelector: .firstGreaterOrEqual(end),
            snapshot: true
        )

        var count = 0
        for try await _ in sequence {
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
        transaction: any TransactionProtocol
    ) async throws -> Int {
        let hnswIndex = try await loadOrCreateIndex(transaction: transaction)
        return hnswIndex.count
    }

    /// Extract vector from item using VectorConversion
    public func extractVector(from item: Item) throws -> [Float] {
        let fieldValues = try DataAccess.evaluateIndexFields(
            from: item,
            keyPaths: index.keyPaths,
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

    private func uint64ToBytes(_ value: UInt64) -> [UInt8] {
        VectorConversion.uint64ToBytes(value)
    }

    private func bytesToUInt64(_ bytes: [UInt8]) -> UInt64 {
        VectorConversion.bytesToUInt64(bytes)
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
