// QuantizedSimilar.swift
// VectorIndex - Quantized vector similarity search query for Fusion
//
// Uses pre-trained quantizers for memory-efficient vector search.
// Follows the same FusionQuery pattern as Similar.swift.
//
// References:
// - Product Quantization: Jégou et al., "Product Quantization for Nearest Neighbor Search", IEEE TPAMI 2011
// - Asymmetric Distance Computation: Jégou et al., "Searching in one billion vectors", IEEE ICME 2011

import Foundation
import Core
import DatabaseEngine
import FoundationDB
import Vector

// MARK: - QuantizedSimilar

/// Quantized vector similarity search query for Fusion
///
/// Uses pre-trained quantizers (PQ/SQ/BQ) for memory-efficient search.
/// ADC (Asymmetric Distance Computation) provides fast approximate distances.
///
/// **Usage**:
/// ```swift
/// // Train quantizer first
/// var pq = ProductQuantizer(config: .forDimensions(384), dimensions: 384)
/// try await pq.train(vectors: sampleVectors)
///
/// // Search using quantized vectors
/// let results = try await context.fuse(Product.self) {
///     QuantizedSimilar(\.embedding, dimensions: 384, quantizer: pq)
///         .nearest(to: queryVector, k: 100)
/// }
/// .execute()
/// ```
///
/// **Performance Characteristics**:
/// - ADC precomputes O(M*K) distance table per query
/// - Distance computation is O(M) per candidate instead of O(D)
/// - Memory usage reduced by compression ratio (e.g., 32x for PQ)
///
/// **Note**: Quantizer must be trained before use. See `CodebookTrainer`.
public struct QuantizedSimilar<T: Persistable, Q: VectorQuantizer>: FusionQuery, Sendable
    where Q.Code == [UInt8]
{
    public typealias Item = T

    private let queryContext: IndexQueryContext
    private let fieldName: String
    private let dimensions: Int
    private let quantizer: Q
    private var queryVector: [Float]?
    private var k: Int = 10

    // MARK: - Initialization

    /// Create a QuantizedSimilar query for a vector field
    ///
    /// Uses FusionContext.current for context (automatically set by `context.fuse { }`).
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the [Float] field
    ///   - dimensions: Number of dimensions in the vectors
    ///   - quantizer: Pre-trained quantizer instance
    ///
    /// **Usage**:
    /// ```swift
    /// context.fuse(Product.self) {
    ///     QuantizedSimilar(\.embedding, dimensions: 384, quantizer: pq)
    ///         .nearest(to: vector, k: 100)
    /// }
    /// ```
    public init(_ keyPath: KeyPath<T, [Float]>, dimensions: Int, quantizer: Q) {
        guard let context = FusionContext.current else {
            fatalError("QuantizedSimilar must be used within context.fuse { } block")
        }
        self.fieldName = T.fieldName(for: keyPath)
        self.dimensions = dimensions
        self.quantizer = quantizer
        self.queryContext = context
    }

    /// Create a QuantizedSimilar query for an optional vector field
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the optional [Float] field
    ///   - dimensions: Number of dimensions in the vectors
    ///   - quantizer: Pre-trained quantizer instance
    public init(_ keyPath: KeyPath<T, [Float]?>, dimensions: Int, quantizer: Q) {
        guard let context = FusionContext.current else {
            fatalError("QuantizedSimilar must be used within context.fuse { } block")
        }
        self.fieldName = T.fieldName(for: keyPath)
        self.dimensions = dimensions
        self.quantizer = quantizer
        self.queryContext = context
    }

    /// Create a QuantizedSimilar query with explicit context
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the [Float] field
    ///   - dimensions: Number of dimensions in the vectors
    ///   - quantizer: Pre-trained quantizer instance
    ///   - context: IndexQueryContext for database access
    public init(_ keyPath: KeyPath<T, [Float]>, dimensions: Int, quantizer: Q, context: IndexQueryContext) {
        self.fieldName = T.fieldName(for: keyPath)
        self.dimensions = dimensions
        self.quantizer = quantizer
        self.queryContext = context
    }

    // MARK: - Configuration

    /// Find nearest neighbors to a query vector
    ///
    /// - Parameters:
    ///   - vector: The query vector to find neighbors for
    ///   - k: Number of nearest neighbors to return
    /// - Returns: Updated query
    public func nearest(to vector: [Float], k: Int) -> Self {
        var copy = self
        copy.queryVector = vector
        copy.k = k
        return copy
    }

    // MARK: - Index Discovery

    /// Find the index descriptor using kindIdentifier and fieldName
    private func findIndexDescriptor() -> IndexDescriptor? {
        T.indexDescriptors.first { descriptor in
            guard descriptor.kindIdentifier == VectorIndexKind<T>.identifier else {
                return false
            }
            guard let kind = descriptor.kind as? VectorIndexKind<T> else {
                return false
            }
            return kind.fieldNames.contains(fieldName)
        }
    }

    // MARK: - FusionQuery

    public func execute(candidates: Set<String>?) async throws -> [ScoredResult<T>] {
        guard let vector = queryVector else { return [] }

        // Verify quantizer is trained
        guard quantizer.isTrained else {
            throw QuantizerError.notTrained
        }

        // Find index descriptor
        guard let descriptor = findIndexDescriptor() else {
            throw FusionQueryError.indexNotFound(
                type: T.persistableType,
                field: fieldName,
                kind: "vector"
            )
        }

        let indexName = descriptor.name

        // Execute search with candidate-aware strategy
        let searchResults: [(item: T, distance: Float)]

        if let candidateIds = candidates, !candidateIds.isEmpty {
            searchResults = try await executeWithCandidates(
                indexName: indexName,
                queryVector: vector,
                candidateIds: candidateIds
            )
        } else {
            searchResults = try await executeQuantizedSearch(
                indexName: indexName,
                queryVector: vector,
                k: k
            )
        }

        // Normalize distances to scores
        return normalizeDistancesToScores(searchResults)
    }

    // MARK: - Quantized Search

    /// Execute quantized vector search using ADC
    ///
    /// Index structure for quantized vectors:
    /// - Key: `[indexSubspace]/q/[primaryKey]`
    /// - Value: `Tuple(UInt8, UInt8, ..., UInt8)` (quantized codes)
    private func executeQuantizedSearch(
        indexName: String,
        queryVector: [Float],
        k: Int
    ) async throws -> [(item: T, distance: Float)] {
        // Get index subspace
        let typeSubspace = try await queryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(indexName)
        let quantizedSubspace = indexSubspace.subspace("q")

        // Prepare ADC distance tables (O(M*K) precomputation)
        let prepared = try quantizer.prepareQuery(queryVector)

        // Execute search within transaction
        let primaryKeysWithDistances: [(pk: Tuple, distance: Float)] = try await queryContext.withTransaction { transaction in
            try await self.searchQuantizedVectors(
                prepared: prepared,
                k: k,
                quantizedSubspace: quantizedSubspace,
                transaction: transaction
            )
        }

        // Fetch items by primary keys
        let items = try await queryContext.fetchItems(ids: primaryKeysWithDistances.map(\.pk), type: T.self)

        // Build lookup map using packed key bytes for proper composite key comparison
        // This handles both single and multi-element primary keys correctly
        var distanceByPackedKey: [Data: Float] = [:]
        distanceByPackedKey.reserveCapacity(primaryKeysWithDistances.count)
        for result in primaryKeysWithDistances {
            let packedKey = Data(result.pk.pack())
            distanceByPackedKey[packedKey] = result.distance
        }

        // Match items with distances using packed key comparison
        let idExpression = FieldKeyExpression(fieldName: "id")
        var results: [(item: T, distance: Float)] = []
        results.reserveCapacity(items.count)

        for item in items {
            let itemKey = try DataAccess.extractId(from: item, using: idExpression)
            let packedItemKey = Data(itemKey.pack())
            if let distance = distanceByPackedKey[packedItemKey] {
                results.append((item: item, distance: distance))
            }
        }

        // Results should already be in distance order from search, but verify
        return results.sorted { $0.distance < $1.distance }
    }

    /// Search quantized vectors using ADC with heap-based top-k
    ///
    /// Algorithm:
    /// 1. Maintain a max-heap of size k (O(k) memory)
    /// 2. For each quantized code: compute ADC distance (O(M) per candidate)
    /// 3. If distance < heap max, replace heap root
    /// 4. Final result: k smallest distances in O(n log k) time
    ///
    /// Reference: Jégou et al., "Product Quantization for Nearest Neighbor Search", IEEE TPAMI 2011
    private func searchQuantizedVectors(
        prepared: PreparedQuery,
        k: Int,
        quantizedSubspace: Subspace,
        transaction: any TransactionProtocol
    ) async throws -> [(pk: Tuple, distance: Float)] {
        let (begin, end) = quantizedSubspace.range()
        let sequence = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(begin),
            endSelector: .firstGreaterOrEqual(end),
            snapshot: true
        )

        // Max-heap to maintain k smallest distances
        // Root = largest distance among k best; new smaller distances evict it
        var heap = TopKMaxHeap<(pk: Tuple, distance: Float)>(k: k) { $0.distance }

        for try await (key, value) in sequence {
            // Decode primary key
            guard quantizedSubspace.contains(key),
                  let keyTuple = try? quantizedSubspace.unpack(key) else {
                continue
            }

            // Decode quantized code
            guard let codeTuple = try? Tuple.unpack(from: value) else {
                continue
            }

            // Convert Tuple to [UInt8] code
            var code: [UInt8] = []
            code.reserveCapacity(quantizer.codeSize)

            for i in 0..<quantizer.codeSize {
                guard i < codeTuple.count else { break }
                if let byte = codeTuple[i] as? Int64 {
                    code.append(UInt8(clamping: byte))
                } else if let byte = codeTuple[i] as? Int {
                    code.append(UInt8(clamping: byte))
                }
            }

            guard code.count == quantizer.codeSize else { continue }

            // Compute distance using ADC (O(M) instead of O(D))
            let distance = quantizer.distanceWithPrepared(prepared, code: code)

            // Insert into heap - O(log k)
            heap.insert((pk: keyTuple, distance: distance))
        }

        // Extract results sorted by distance (smallest first)
        return heap.toSortedArray()
    }

    /// Normalize distances to scores [0, 1] where higher is better
    private func normalizeDistancesToScores(_ results: [(item: T, distance: Float)]) -> [ScoredResult<T>] {
        guard !results.isEmpty else { return [] }

        let distances = results.map(\.distance)
        guard let minDist = distances.min(),
              let maxDist = distances.max(),
              maxDist != minDist else {
            return results.map { ScoredResult(item: $0.item, score: 1.0) }
        }

        let range = Double(maxDist - minDist)
        return results.map { result in
            let score = Double(maxDist - result.distance) / range
            return ScoredResult(item: result.item, score: score)
        }
    }

    // MARK: - Candidate-Aware Search

    /// Execute quantized search with candidate filtering
    private func executeWithCandidates(
        indexName: String,
        queryVector: [Float],
        candidateIds: Set<String>
    ) async throws -> [(item: T, distance: Float)] {
        let bruteForceThreshold = 1000

        if candidateIds.count <= bruteForceThreshold {
            // Small candidate set: fetch items and compute distances
            return try await computeDistancesForCandidates(
                queryVector: queryVector,
                candidateIds: candidateIds
            )
        } else {
            // Large candidate set: expanded search with post-filtering
            let sqrtScaled = Int(Double(candidateIds.count).squareRoot()) * k
            let expandedK = min(
                candidateIds.count,
                max(k * 10, candidateIds.count / 2, k + 2000, sqrtScaled)
            )

            var results = try await executeQuantizedSearch(
                indexName: indexName,
                queryVector: queryVector,
                k: expandedK
            )

            results = results.filter { candidateIds.contains("\($0.item.id)") }

            if results.count > k {
                results = Array(results.prefix(k))
            }

            return results
        }
    }

    /// Compute distances for candidate items using quantizer
    private func computeDistancesForCandidates(
        queryVector: [Float],
        candidateIds: Set<String>
    ) async throws -> [(item: T, distance: Float)] {
        let items = try await queryContext.fetchItemsByStringIds(type: T.self, ids: Array(candidateIds))

        // Prepare ADC distance tables
        let prepared = try quantizer.prepareQuery(queryVector)

        var results: [(item: T, distance: Float)] = []

        for item in items {
            guard let vector = item[dynamicMember: fieldName] as? [Float] else {
                continue
            }

            guard vector.count == dimensions else {
                continue
            }

            // Encode and compute distance
            do {
                let code = try quantizer.encode(vector)
                let distance = quantizer.distanceWithPrepared(prepared, code: code)
                results.append((item: item, distance: distance))
            } catch {
                continue
            }
        }

        results.sort { $0.distance < $1.distance }
        if results.count > k {
            results = Array(results.prefix(k))
        }

        return results
    }
}

// MARK: - TopKMaxHeap

/// Max-heap for maintaining k smallest elements with O(log k) insert
///
/// For k-NN search: root is the largest distance among k best.
/// When a smaller distance is found, it replaces the root.
///
/// Time complexity:
/// - Insert: O(log k) amortized
/// - ToSortedArray: O(k log k)
///
/// Space complexity: O(k)
private struct TopKMaxHeap<Element> {
    private var elements: [Element] = []
    private let k: Int
    private let keyExtractor: (Element) -> Float

    /// Create a max-heap for top-k smallest values
    /// - Parameters:
    ///   - k: Maximum number of elements to keep
    ///   - keyExtractor: Function to extract the comparison key (e.g., distance)
    init(k: Int, keyExtractor: @escaping (Element) -> Float) {
        self.k = k
        self.keyExtractor = keyExtractor
        self.elements.reserveCapacity(k)
    }

    var count: Int { elements.count }
    var isEmpty: Bool { elements.isEmpty }

    /// Insert element, maintaining only k smallest values
    mutating func insert(_ element: Element) {
        let newKey = keyExtractor(element)

        if elements.count < k {
            // Heap not full - just add
            elements.append(element)
            siftUp(elements.count - 1)
        } else if newKey < keyExtractor(elements[0]) {
            // New element has smaller key than heap max - replace root
            elements[0] = element
            siftDown(0)
        }
        // Otherwise: new element is larger than all k best, ignore
    }

    /// Returns elements sorted by key (smallest first)
    func toSortedArray() -> [Element] {
        return elements.sorted { keyExtractor($0) < keyExtractor($1) }
    }

    // MARK: - Private Heap Operations

    private mutating func siftUp(_ index: Int) {
        var i = index
        while i > 0 {
            let parent = (i - 1) / 2
            // Max-heap: parent should have larger key
            if keyExtractor(elements[i]) > keyExtractor(elements[parent]) {
                elements.swapAt(i, parent)
                i = parent
            } else {
                break
            }
        }
    }

    private mutating func siftDown(_ index: Int) {
        var i = index
        while true {
            let left = 2 * i + 1
            let right = 2 * i + 2
            var largest = i

            // Find largest among node and its children
            if left < elements.count && keyExtractor(elements[left]) > keyExtractor(elements[largest]) {
                largest = left
            }
            if right < elements.count && keyExtractor(elements[right]) > keyExtractor(elements[largest]) {
                largest = right
            }

            if largest != i {
                elements.swapAt(i, largest)
                i = largest
            } else {
                break
            }
        }
    }
}
