// Similar.swift
// VectorIndex - Vector similarity search query for Fusion
//
// This file is part of VectorIndex module, not DatabaseEngine.
// DatabaseEngine does not own vector index execution.

import DatabaseKit
import DatabaseEngine
import DatabaseTypes
import StorageKit

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
    private var queryVector: [Float]?
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
    private func findIndexDescriptor() throws -> IndexDescriptor? {
        try T.indexDescriptors.first { descriptor in
            // 1. Filter by kindIdentifier
            guard descriptor.kindIdentifier
                    == VectorIndexSpecification.identifier else {
                return false
            }
            // 2. Match by fieldName
            return descriptor.fieldNames.contains(fieldName)
        }
    }

    // MARK: - FusionQuery

    public func execute(candidates: Set<T.ID>?) async throws -> [ScoredResult<T>] {
        guard let vector = queryVector else { return [] }

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

        if let candidateIDs = candidates, !candidateIDs.isEmpty {
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
        return normalizeDistancesToScores(searchResults)
    }

    // MARK: - Vector Index Reading

    /// Execute vector search by reading index directly
    ///
    /// Index structure (Flat/HNSW shared):
    /// - Key: `[indexSubspace][primaryKey]`
    /// - Value: Float32 binary payload, little-endian
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
        transaction: any TransactionAccess
    ) async throws -> [(pk: Tuple, distance: Double)] {
        let (begin, end) = indexSubspace.range()
        let sequence = try await TransactionRangeCollection.collect(using: transaction,
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            limit: 0,
            reverse: false,
            snapshot: true,
            streamingMode: .wantAll
        )

        var results: [(pk: Tuple, distance: Double)] = []

        for (key, value) in sequence {
            // Skip HNSW marker keys without materializing the binary key as Data/String.
            if containsHNSWMarker(in: key) {
                continue
            }

            guard indexSubspace.contains(key) else {
                throw VectorIndexError.invalidStructure("Vector index key is outside the expected subspace")
            }

            let keyTuple: Tuple
            do {
                keyTuple = try indexSubspace.unpack(key)
            } catch {
                throw VectorIndexError.invalidStructure("Invalid vector index primary key")
            }

            let vector = try VectorConversion.decodeFloatArray(value, expectedCount: dimensions)

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
    /// `VectorQueryBuilder.filter()` uses the same expanded-candidate post-filter
    /// contract when no candidate set has already been materialized.
    private func executeWithCandidates(
        indexName: String,
        queryVector: [Float],
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
            let sqrtScaled = Int(Double(candidateIDs.count).squareRoot()) * k
            let expandedK = min(
                candidateIDs.count,
                max(k * 10, candidateIDs.count / 2, k + 2000, sqrtScaled)
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
        queryVector: [Float],
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

    private func float32Vector(from item: borrowing T) throws -> [Float]? {
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
              vector.elementType == .float32,
              let elements = vector.withFloat32Elements({ source in
                  // Distance algorithms currently require owned mutable-index
                  // access. This is the single materialization boundary from
                  // the retained primitive vector into that execution API.
                  Array(source)
              }) else {
            throw FusionQueryError.invalidConfiguration(
                "Persisted field '\(fieldName)' is not a Float32 vector"
            )
        }
        return elements
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

/// Searches valid UTF-8 tuple-key bytes for the lower-case ASCII marker.
///
/// The previous `String(data:encoding:)` path rejected malformed UTF-8. This
/// validator preserves that behavior while avoiding both `Data` and `String`
/// materialization in the scan loop.
func containsHNSWMarker(in key: ByteString) -> Bool {
    key.withUnsafeBytes { bytes in
        var index = 0
        var containsMarker = false
        while index < bytes.count {
            if index + 3 < bytes.count,
               bytes[index] == 104,
               bytes[index + 1] == 110,
               bytes[index + 2] == 115,
               bytes[index + 3] == 119 {
                containsMarker = true
            }

            let leadingByte = bytes[index]
            let sequenceLength: Int
            switch leadingByte {
            case 0x00...0x7F:
                sequenceLength = 1
            case 0xC2...0xDF:
                sequenceLength = 2
            case 0xE0:
                guard index + 2 < bytes.count,
                      bytes[index + 1] >= 0xA0,
                      bytes[index + 1] <= 0xBF,
                      isUTF8Continuation(bytes[index + 2]) else {
                    return false
                }
                sequenceLength = 3
            case 0xE1...0xEC, 0xEE...0xEF:
                sequenceLength = 3
            case 0xED:
                guard index + 2 < bytes.count,
                      bytes[index + 1] >= 0x80,
                      bytes[index + 1] <= 0x9F,
                      isUTF8Continuation(bytes[index + 2]) else {
                    return false
                }
                sequenceLength = 3
            case 0xF0:
                guard index + 3 < bytes.count,
                      bytes[index + 1] >= 0x90,
                      bytes[index + 1] <= 0xBF,
                      isUTF8Continuation(bytes[index + 2]),
                      isUTF8Continuation(bytes[index + 3]) else {
                    return false
                }
                sequenceLength = 4
            case 0xF1...0xF3:
                sequenceLength = 4
            case 0xF4:
                guard index + 3 < bytes.count,
                      bytes[index + 1] >= 0x80,
                      bytes[index + 1] <= 0x8F,
                      isUTF8Continuation(bytes[index + 2]),
                      isUTF8Continuation(bytes[index + 3]) else {
                    return false
                }
                sequenceLength = 4
            default:
                return false
            }

            guard index + sequenceLength <= bytes.count else {
                return false
            }
            if leadingByte != 0xE0,
               leadingByte != 0xED,
               leadingByte != 0xF0,
               leadingByte != 0xF4 {
                for continuationIndex in (index + 1)..<(index + sequenceLength) {
                    guard isUTF8Continuation(bytes[continuationIndex]) else {
                        return false
                    }
                }
            }
            index += sequenceLength
        }
        return containsMarker
    }
}

private func isUTF8Continuation(_ byte: UInt8) -> Bool {
    byte >= 0x80 && byte <= 0xBF
}
