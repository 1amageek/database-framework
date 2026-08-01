// Similar.swift
// VectorIndex - Vector similarity search query for Fusion
//
// This file is part of VectorIndex module, not DatabaseEngine.
// DatabaseEngine does not own vector index execution.

import DatabaseKit
import DatabaseEngine
import DatabaseTypes

/// Vector similarity search query for Fusion
///
/// Searches vector embeddings using distance metrics.
/// Supports candidate-restricted vector filtering.
///
/// **Usage**:
/// ```swift
/// let results = try await context.fuse(Product.self) {
///     Similar(Product.fields.embedding, dimensions: 384)
///         .nearest(to: queryVector, k: 100)
///         .metric(.cosine)
/// }
/// .execute()
/// ```
public struct Similar<T: Persistable>: FusionQuery, Sendable {
    public typealias Item = T

    private let queryContext: IndexQueryContext
    private let fieldIdentity: FieldIdentity
    private let fieldName: String
    private let dimensions: Int
    private var queryElements: [Float]?
    private var retainedQueryVector: Vector?
    private var k: Int = 10
    private var metric: VectorDistanceMetric = .cosine

    // MARK: - Initialization

    /// Create a Similar query for a vector field
    ///
    /// Uses FusionContext.current for context (automatically set by `context.fuse { }`).
    ///
    /// - Parameters:
    ///   - field: Compiled vector field metadata
    ///   - dimensions: Number of dimensions in the vectors
    ///
    /// **Usage**:
    /// ```swift
    /// context.fuse(Product.self) {
    ///     Similar(Product.fields.embedding, dimensions: 384)
    ///         .nearest(to: vector, k: 100)
    /// }
    /// ```
    public init(_ field: Field<T, Vector>, dimensions: Int) {
        guard let context = FusionContext.current else {
            fatalError("Similar must be used within context.fuse { } block")
        }
        self.fieldIdentity = field.identity
        self.fieldName = field.name
        self.dimensions = dimensions
        self.queryContext = context
    }

    /// Create a Similar query for an optional vector field
    ///
    /// - Parameters:
    ///   - field: Compiled optional vector field metadata
    ///   - dimensions: Number of dimensions in the vectors
    public init(_ field: Field<T, Vector?>, dimensions: Int) {
        guard let context = FusionContext.current else {
            fatalError("Similar must be used within context.fuse { } block")
        }
        self.fieldIdentity = field.identity
        self.fieldName = field.name
        self.dimensions = dimensions
        self.queryContext = context
    }

    /// Create a Similar query with explicit context
    ///
    /// - Parameters:
    ///   - field: Compiled vector field metadata
    ///   - dimensions: Number of dimensions in the vectors
    ///   - context: IndexQueryContext for database access
    public init(
        _ field: Field<T, Vector>,
        dimensions: Int,
        context: IndexQueryContext
    ) {
        self.fieldIdentity = field.identity
        self.fieldName = field.name
        self.dimensions = dimensions
        self.queryContext = context
    }

    /// Create a Similar query for an optional vector field with explicit context
    ///
    /// - Parameters:
    ///   - field: Compiled optional vector field metadata
    ///   - dimensions: Number of dimensions in the vectors
    ///   - context: IndexQueryContext for database access
    public init(
        _ field: Field<T, Vector?>,
        dimensions: Int,
        context: IndexQueryContext
    ) {
        self.fieldIdentity = field.identity
        self.fieldName = field.name
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
        copy.queryElements = vector
        copy.retainedQueryVector = nil
        copy.k = k
        return copy
    }

    /// Finds nearest neighbors while retaining the canonical vector owner.
    /// No query element buffer is copied at this API boundary.
    public func nearest(to vector: Vector, k: Int) -> Self {
        var copy = self
        copy.queryElements = nil
        copy.retainedQueryVector = vector
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
    private func findIndexDescriptor() throws -> IndexDescriptor? {
        try queryContext.indexDescriptors(for: T.self).first { descriptor in
            guard descriptor.kindIdentifier
                    == VectorIndexSpecification.identifier,
                  descriptor.fieldNames == [fieldName] else {
                return false
            }
            let specification = try VectorIndexSpecification(descriptor.kind)
            return specification.dimensions == dimensions
                && specification.metric.rawValue == metric.rawValue
        }
    }

    // MARK: - FusionQuery

    public func execute(candidates: Set<T.ID>?) async throws -> [ScoredResult<T>] {
        guard dimensions > 0 else {
            throw FusionQueryError.invalidConfiguration(
                "Vector dimensions must be positive"
            )
        }
        guard k > 0 else {
            throw FusionQueryError.invalidConfiguration(
                "Vector result count must be positive"
            )
        }
        guard queryElements != nil || retainedQueryVector != nil else {
            throw FusionQueryError.invalidConfiguration(
                "A nearest-neighbor query vector is required"
            )
        }
        let queryCount = retainedQueryVector?.count ?? queryElements?.count ?? 0
        guard queryCount == dimensions else {
            throw FusionQueryError.invalidConfiguration(
                "Vector query dimension mismatch: expected \(dimensions), got \(queryCount)"
            )
        }
        let vector: Vector
        if let retainedQueryVector {
            guard retainedQueryVector.elementType == .float32 else {
                throw FusionQueryError.invalidConfiguration(
                    "Vector queries require Float32 elements"
                )
            }
            vector = retainedQueryVector
        } else {
            do {
                vector = try Vector(float32: queryElements ?? [])
            } catch {
                throw FusionQueryError.invalidConfiguration(
                    "Vector query contains a non-finite Float32 element: \(error)"
                )
            }
        }

        // Find index descriptor
        guard let descriptor = try findIndexDescriptor() else {
            throw FusionQueryError.indexNotFound(
                type: T.persistableType,
                field: fieldName,
                kind: "vector"
            )
        }

        let indexName = descriptor.name

        // Execute search with candidate-aware strategy
        let searchResults: [(item: T, distance: Double)]

        if let candidateIDs = candidates {
            guard !candidateIDs.isEmpty else {
                return []
            }
            // Candidate-aware search strategies:
            // 1. Small candidate set: Brute-force (guarantees recall)
            // 2. Large candidate set: Expanded-k with post-filtering
            searchResults = try await executeWithCandidates(
                indexName: indexName,
                queryVector: vector,
                candidateIDs: candidateIDs
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
        return try normalizeDistancesToScores(searchResults)
    }

    // MARK: - Vector Index Reading

    /// Executes the canonical vector query path so Fusion observes the same
    /// runtime policy, persisted layout, metric, and primary-key contract as
    /// ordinary vector queries.
    private func executeVectorSearch(
        indexName: String,
        queryVector: Vector,
        k: Int
    ) async throws -> [(item: T, distance: Double)] {
        let builder = VectorQueryBuilder<T>(
            queryContext: queryContext,
            fieldName: fieldName,
            dimensions: dimensions,
            selectedIndexName: indexName
        ).metric(metric)
        return try await builder
            .query(queryVector, k: k)
            .executeDirect()
    }

    /// Normalize distances to scores [0, 1] where higher is better
    ///
    /// Handles all distance metrics correctly:
    /// - Euclidean/Cosine: distances are positive, smaller = better
    /// - DotProduct: distances are negative (computed as -dot), smaller (more negative) = better
    ///
    /// Uses min-max normalization: score = (maxDist - distance) / (maxDist - minDist)
    private func normalizeDistancesToScores(
        _ results: [(item: T, distance: Double)]
    ) throws -> [ScoredResult<T>] {
        guard !results.isEmpty else { return [] }

        let distances = results.map(\.distance)
        guard distances.allSatisfy(\.isFinite) else {
            throw FusionQueryError.invalidConfiguration(
                "Vector search produced a non-finite distance"
            )
        }
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
    /// `VectorQueryBuilder.filter()` uses the same expanded-candidate post-filter
    /// contract when no candidate set has already been materialized.
    private func executeWithCandidates(
        indexName: String,
        queryVector: Vector,
        candidateIDs: Set<T.ID>
    ) async throws -> [(item: T, distance: Double)] {
        // Threshold for switching between brute-force and expanded-k
        let bruteForceThreshold = 1000

        if candidateIDs.count <= bruteForceThreshold {
            // Small candidate set: brute-force guarantees recall
            return try await computeDistancesForCandidates(
                queryVector: queryVector,
                candidateIDs: candidateIDs
            )
        } else {
            // Large candidate set: expanded kNN with post-filtering
            //
            // k expansion formula considerations:
            // - k * 10: Base expansion for sparse distributions
            // - candidateIds.count / 2: Scale with candidate set size
            // - k + 2000: Minimum expansion to ensure good recall
            // - sqrt(candidateIDs.count) * k: Sublinear scaling for very large sets
            //
            // Reference: Empirical studies show recall degrades gracefully when
            // expansion factor is at least sqrt(N) * k for N candidates.
            let candidateCount = candidateIDs.count
            let sqrtFactor = Int(Double(candidateCount).squareRoot())
            let expandedK = max(
                clampedProduct(k, 10, upperBound: candidateCount),
                candidateCount / 2,
                clampedSum(k, 2_000, upperBound: candidateCount),
                clampedProduct(k, sqrtFactor, upperBound: candidateCount)
            )

            var results = try await executeVectorSearch(
                indexName: indexName,
                queryVector: queryVector,
                k: expandedK
            )

            // Filter to candidates
            results = results.filter { result in
                candidateIDs.contains(result.item.id)
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
        queryVector: Vector,
        candidateIDs: Set<T.ID>
    ) async throws -> [(item: T, distance: Double)] {
        // Fetch candidate items
        let items = try await queryContext.fetchItems(
            identifiers: Array(candidateIDs),
            type: T.self
        )

        var results: [(item: T, distance: Double)] = []

        for item in items {
            guard let vector = try float32Vector(from: item) else {
                continue
            }

            guard vector.count == dimensions else {
                throw FusionQueryError.invalidConfiguration(
                    "Persisted vector field '\(fieldName)' has dimension \(vector.count); expected \(dimensions)"
                )
            }

            let distance = try VectorConversion.distance(
                metric: indexMetric,
                from: queryVector,
                to: vector
            )
            results.append((item: item, distance: distance))
        }

        // Sort by distance and take top k
        results.sort { $0.distance < $1.distance }
        if results.count > k {
            results = Array(results.prefix(k))
        }

        return results
    }

    private func float32Vector(from item: borrowing T) throws -> Vector? {
        guard let value = try PersistableFieldEncoder.value(
            for: fieldIdentity,
            in: item
        ) else {
            throw FusionQueryError.invalidConfiguration(
                "Persisted vector field '\(fieldName)' was not emitted"
            )
        }
        if case .null = value {
            return nil
        }
        guard case .vector(let vector) = value,
              vector.elementType == .float32 else {
            throw FusionQueryError.invalidConfiguration(
                "Persisted field '\(fieldName)' is not a Float32 vector"
            )
        }
        return vector
    }

    private var indexMetric: VectorMetric {
        switch metric {
        case .euclidean:
            return .euclidean
        case .cosine:
            return .cosine
        case .dotProduct:
            return .dotProduct
        }
    }

    private func clampedProduct(
        _ lhs: Int,
        _ rhs: Int,
        upperBound: Int
    ) -> Int {
        guard lhs > 0, rhs > 0 else { return 0 }
        guard lhs <= upperBound / rhs else { return upperBound }
        return min(lhs * rhs, upperBound)
    }

    private func clampedSum(
        _ lhs: Int,
        _ rhs: Int,
        upperBound: Int
    ) -> Int {
        guard lhs <= upperBound - min(rhs, upperBound) else {
            return upperBound
        }
        return min(lhs + rhs, upperBound)
    }
}
