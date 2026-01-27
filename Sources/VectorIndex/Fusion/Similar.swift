// Similar.swift
// VectorIndex - Vector similarity search query for Fusion
//
// This file is part of VectorIndex module, not DatabaseEngine.
// DatabaseEngine does not know about VectorIndexKind.

import Foundation
import Core
import DatabaseEngine
import FoundationDB
import Vector

/// Vector similarity search query for Fusion
///
/// Searches vector embeddings using distance metrics.
/// Supports ACORN filtering when candidates are provided.
///
/// **Usage**:
/// ```swift
/// let results = try await context.fuse(Product.self) {
///     Similar(\.embedding, dimensions: 384)
///         .nearest(to: queryVector, k: 100)
///         .metric(.cosine)
/// }
/// .execute()
/// ```
public struct Similar<T: Persistable>: FusionQuery, Sendable {
    public typealias Item = T

    private let queryContext: IndexQueryContext
    private let fieldName: String
    private let dimensions: Int
    private var queryVector: [Float]?
    private var k: Int = 10
    private var metric: VectorDistanceMetric = .cosine

    // MARK: - Initialization

    /// Create a Similar query for a vector field
    ///
    /// Uses FusionContext.current for context (automatically set by `context.fuse { }`).
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the [Float] field
    ///   - dimensions: Number of dimensions in the vectors
    ///
    /// **Usage**:
    /// ```swift
    /// context.fuse(Product.self) {
    ///     Similar(\.embedding, dimensions: 384).nearest(to: vector, k: 100)
    /// }
    /// ```
    public init(_ keyPath: KeyPath<T, [Float]>, dimensions: Int) {
        guard let context = FusionContext.current else {
            fatalError("Similar must be used within context.fuse { } block")
        }
        self.fieldName = T.fieldName(for: keyPath)
        self.dimensions = dimensions
        self.queryContext = context
    }

    /// Create a Similar query for an optional vector field
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the optional [Float] field
    ///   - dimensions: Number of dimensions in the vectors
    public init(_ keyPath: KeyPath<T, [Float]?>, dimensions: Int) {
        guard let context = FusionContext.current else {
            fatalError("Similar must be used within context.fuse { } block")
        }
        self.fieldName = T.fieldName(for: keyPath)
        self.dimensions = dimensions
        self.queryContext = context
    }

    /// Create a Similar query with explicit context
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the [Float] field
    ///   - dimensions: Number of dimensions in the vectors
    ///   - context: IndexQueryContext for database access
    public init(_ keyPath: KeyPath<T, [Float]>, dimensions: Int, context: IndexQueryContext) {
        self.fieldName = T.fieldName(for: keyPath)
        self.dimensions = dimensions
        self.queryContext = context
    }

    /// Create a Similar query for an optional vector field with explicit context
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the optional [Float] field
    ///   - dimensions: Number of dimensions in the vectors
    ///   - context: IndexQueryContext for database access
    public init(_ keyPath: KeyPath<T, [Float]?>, dimensions: Int, context: IndexQueryContext) {
        self.fieldName = T.fieldName(for: keyPath)
        self.dimensions = dimensions
        self.queryContext = context
    }

    /// Create a Similar query with a field name string and explicit context
    ///
    /// - Parameters:
    ///   - fieldName: The field name to search
    ///   - dimensions: Number of dimensions in the vectors
    ///   - context: IndexQueryContext for database access
    public init(fieldName: String, dimensions: Int, context: IndexQueryContext) {
        self.fieldName = fieldName
        self.dimensions = dimensions
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

    /// Set the distance metric
    ///
    /// - Parameter metric: Distance metric (.cosine, .euclidean, .dotProduct)
    /// - Returns: Updated query
    public func metric(_ metric: VectorDistanceMetric) -> Self {
        var copy = self
        copy.metric = metric
        return copy
    }

    // MARK: - Index Discovery

    /// Find the index descriptor using kindIdentifier and fieldName
    ///
    /// This approach:
    /// 1. Filters by kindIdentifier ("vector") for efficiency
    /// 2. Matches by fieldName within the kind
    private func findIndexDescriptor() -> IndexDescriptor? {
        T.indexDescriptors.first { descriptor in
            // 1. Filter by kindIdentifier
            guard descriptor.kindIdentifier == VectorIndexKind<T>.identifier else {
                return false
            }
            // 2. Match by fieldName
            guard let kind = descriptor.kind as? VectorIndexKind<T> else {
                return false
            }
            return kind.fieldNames.contains(fieldName)
        }
    }

    // MARK: - FusionQuery

    public func execute(candidates: Set<String>?) async throws -> [ScoredResult<T>] {
        guard let vector = queryVector else { return [] }

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
        let searchResults: [(item: T, distance: Double)]

        if let candidateIds = candidates, !candidateIds.isEmpty {
            // Candidate-aware search strategies:
            // 1. Small candidate set: Brute-force (guarantees recall)
            // 2. Large candidate set: Expanded-k with post-filtering
            searchResults = try await executeWithCandidates(
                indexName: indexName,
                queryVector: vector,
                candidateIds: candidateIds
            )
        } else {
            // No candidates - standard kNN search via index
            searchResults = try await executeVectorSearch(
                indexName: indexName,
                queryVector: vector,
                k: k
            )
        }

        // Convert distance to score using min-max normalization
        // This handles both positive distances (euclidean, cosine) and negative distances (dotProduct)
        return normalizeDistancesToScores(searchResults)
    }

    // MARK: - Vector Index Reading

    /// Execute vector search by reading index directly
    ///
    /// Index structure (Flat/HNSW shared):
    /// - Key: `[indexSubspace][primaryKey]`
    /// - Value: `Tuple(Float, Float, ..., Float)`
    private func executeVectorSearch(
        indexName: String,
        queryVector: [Float],
        k: Int
    ) async throws -> [(item: T, distance: Double)] {
        // Get index subspace
        let typeSubspace = try await queryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(indexName)

        // Execute search within transaction
        let primaryKeysWithDistances: [(pk: Tuple, distance: Double)] = try await queryContext.withTransaction { transaction in
            try await self.searchVectors(
                queryVector: queryVector,
                k: k,
                indexSubspace: indexSubspace,
                transaction: transaction
            )
        }

        // Fetch items by primary keys
        let items = try await queryContext.fetchItems(ids: primaryKeysWithDistances.map(\.pk), type: T.self)

        // Match items with distances
        var results: [(item: T, distance: Double)] = []
        for item in items {
            // Find matching pk in results
            for result in primaryKeysWithDistances {
                if let pkId = result.pk[0] as? String, "\(item.id)" == pkId {
                    results.append((item: item, distance: result.distance))
                    break
                } else if let pkId = result.pk[0] as? Int64, "\(item.id)" == "\(pkId)" {
                    results.append((item: item, distance: result.distance))
                    break
                }
            }
        }

        // Sort by distance
        return results.sorted { $0.distance < $1.distance }
    }

    /// Search vectors using brute-force scan
    ///
    /// Algorithm:
    /// 1. Scan all vectors in the index
    /// 2. Calculate distance to query vector
    /// 3. Keep top-k smallest distances using min-heap
    private func searchVectors(
        queryVector: [Float],
        k: Int,
        indexSubspace: Subspace,
        transaction: any TransactionProtocol
    ) async throws -> [(pk: Tuple, distance: Double)] {
        let (begin, end) = indexSubspace.range()
        let sequence = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(begin),
            endSelector: .firstGreaterOrEqual(end),
            snapshot: true
        )

        var results: [(pk: Tuple, distance: Double)] = []

        for try await (key, value) in sequence {
            // Skip HNSW metadata keys
            if let keyStr = String(data: Data(key), encoding: .utf8),
               keyStr.contains("hnsw") {
                continue
            }

            // Decode primary key
            guard indexSubspace.contains(key),
                  let keyTuple = try? indexSubspace.unpack(key) else {
                continue
            }

            // Decode vector
            guard let vectorTuple = try? Tuple.unpack(from: value) else {
                continue
            }

            var vector: [Float] = []
            vector.reserveCapacity(dimensions)
            var isValid = true

            for i in 0..<dimensions {
                guard i < vectorTuple.count else {
                    isValid = false
                    break
                }

                let element = vectorTuple[i]
                if let floatValue = TypeConversion.asFloat(element) {
                    vector.append(floatValue)
                } else {
                    isValid = false
                    break
                }
            }

            guard isValid else { continue }

            // Calculate distance
            let distance = computeDistance(queryVector, vector)

            // Insert into results (simple heap would be better for large k)
            results.append((pk: keyTuple, distance: distance))
        }

        // Sort by distance and take top k
        results.sort { $0.distance < $1.distance }
        if results.count > k {
            results = Array(results.prefix(k))
        }

        return results
    }

    /// Normalize distances to scores [0, 1] where higher is better
    ///
    /// Handles all distance metrics correctly:
    /// - Euclidean/Cosine: distances are positive, smaller = better
    /// - DotProduct: distances are negative (computed as -dot), smaller (more negative) = better
    ///
    /// Uses min-max normalization: score = (maxDist - distance) / (maxDist - minDist)
    private func normalizeDistancesToScores(_ results: [(item: T, distance: Double)]) -> [ScoredResult<T>] {
        guard !results.isEmpty else { return [] }

        let distances = results.map(\.distance)
        guard let minDist = distances.min(),
              let maxDist = distances.max(),
              maxDist != minDist else {
            // All distances are the same - assign equal scores
            return results.map { ScoredResult(item: $0.item, score: 1.0) }
        }

        // Min-max normalization: smaller distance = higher score
        // score = (maxDist - distance) / (maxDist - minDist)
        // When distance = minDist: score = 1.0 (best)
        // When distance = maxDist: score = 0.0 (worst)
        let range = maxDist - minDist
        return results.map { result in
            let score = (maxDist - result.distance) / range
            return ScoredResult(item: result.item, score: score)
        }
    }

    // MARK: - Candidate-Aware Search

    /// Execute vector search with candidate awareness
    ///
    /// Strategies:
    /// 1. Small candidate set (≤ bruteForceThreshold): Fetch candidates and compute distances directly
    /// 2. Large candidate set: Use expanded kNN search with post-filtering
    ///
    /// Reference: For true ACORN-style filtering during HNSW traversal, use VectorQueryBuilder's
    /// filter() API instead.
    private func executeWithCandidates(
        indexName: String,
        queryVector: [Float],
        candidateIds: Set<String>
    ) async throws -> [(item: T, distance: Double)] {
        // Threshold for switching between brute-force and expanded-k
        let bruteForceThreshold = 1000

        if candidateIds.count <= bruteForceThreshold {
            // Small candidate set: brute-force guarantees recall
            return try await computeDistancesForCandidates(
                queryVector: queryVector,
                candidateIds: candidateIds
            )
        } else {
            // Large candidate set: expanded kNN with post-filtering
            //
            // k expansion formula considerations:
            // - k * 10: Base expansion for sparse distributions
            // - candidateIds.count / 2: Scale with candidate set size
            // - k + 2000: Minimum expansion to ensure good recall
            // - sqrt(candidateIds.count) * k: Sublinear scaling for very large sets
            //
            // Reference: Empirical studies show recall degrades gracefully when
            // expansion factor is at least sqrt(N) * k for N candidates.
            let sqrtScaled = Int(Double(candidateIds.count).squareRoot()) * k
            let expandedK = min(
                candidateIds.count,
                max(k * 10, candidateIds.count / 2, k + 2000, sqrtScaled)
            )

            var results = try await executeVectorSearch(
                indexName: indexName,
                queryVector: queryVector,
                k: expandedK
            )

            // Filter to candidates
            results = results.filter { result in
                candidateIds.contains("\(result.item.id)")
            }

            // Trim to k
            if results.count > k {
                results = Array(results.prefix(k))
            }

            return results
        }
    }

    /// Compute vector distances for a set of candidate items (brute-force)
    ///
    /// Fetches the candidate items and computes distances directly.
    /// Used when candidate set is small enough for brute-force approach.
    private func computeDistancesForCandidates(
        queryVector: [Float],
        candidateIds: Set<String>
    ) async throws -> [(item: T, distance: Double)] {
        // Fetch candidate items
        let items = try await queryContext.fetchItemsByStringIds(type: T.self, ids: Array(candidateIds))

        var results: [(item: T, distance: Double)] = []

        for item in items {
            // Get vector from the field
            guard let vector = item[dynamicMember: fieldName] as? [Float] else {
                continue
            }

            guard vector.count == dimensions else {
                continue
            }

            let distance = computeDistance(queryVector, vector)
            results.append((item: item, distance: distance))
        }

        // Sort by distance and take top k
        results.sort { $0.distance < $1.distance }
        if results.count > k {
            results = Array(results.prefix(k))
        }

        return results
    }

    /// Compute distance between two vectors
    private func computeDistance(_ a: [Float], _ b: [Float]) -> Double {
        guard a.count == b.count else { return Double.infinity }

        switch metric {
        case .euclidean:
            var sum: Float = 0
            for i in 0..<a.count {
                let diff = a[i] - b[i]
                sum += diff * diff
            }
            return Double(sum.squareRoot())

        case .cosine:
            var dot: Float = 0
            var normA: Float = 0
            var normB: Float = 0
            for i in 0..<a.count {
                dot += a[i] * b[i]
                normA += a[i] * a[i]
                normB += b[i] * b[i]
            }
            let denom = (normA.squareRoot() * normB.squareRoot())
            if denom == 0 { return 2.0 }  // Zero vector has no direction → maximum distance
            return Double(1.0 - dot / denom)

        case .dotProduct:
            var dot: Float = 0
            for i in 0..<a.count {
                dot += a[i] * b[i]
            }
            return Double(-dot)
        }
    }
}
