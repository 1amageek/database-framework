// IndexSearcher.swift
// QueryPlanner - Index search abstraction

import Foundation
import FoundationDB
import Core

/// Protocol for index-specific search operations
///
/// Each index type implements this protocol to encapsulate its key layout
/// and search logic. The implementation knows how to:
/// - Navigate its specific key structure
/// - Parse index entries
/// - Execute queries efficiently
///
/// **Design Principle**:
/// - IndexSearcher receives pre-resolved Subspace (via DirectoryLayer)
/// - Subspace resolution is done by IndexQueryContext based on Persistable type
/// - IndexSearcher uses StorageReader for raw KV access only
/// - Returns standardized IndexEntry results
///
/// **Usage**:
/// ```swift
/// // Get subspace via IndexQueryContext (resolves via DirectoryLayer)
/// let indexSubspace = try await queryContext.indexSubspace(for: Product.self)
///     .subspace(indexDescriptor.name)
///
/// // Search using the resolved subspace
/// let searcher = ScalarIndexSearcher()
/// let entries = try await searcher.search(
///     query: ScalarIndexQuery.equals(["electronics"]),
///     in: indexSubspace,
///     using: reader
/// )
/// ```
public protocol IndexSearcher: Sendable {
    /// The query type for this index
    associatedtype Query: Sendable

    /// Search the index
    ///
    /// - Parameters:
    ///   - query: The search query
    ///   - subspace: The index subspace (resolved via DirectoryLayer)
    ///   - reader: The storage reader for raw KV access
    /// - Returns: Array of matching index entries
    func search(
        query: Query,
        in subspace: Subspace,
        using reader: StorageReader
    ) async throws -> [IndexEntry]
}

// MARK: - Scalar Index Query

/// Query for scalar (value-based) indexes
///
/// Supports range scans with optional bounds.
public struct ScalarIndexQuery: Sendable {
    /// Start bound (nil for unbounded)
    public let start: [any TupleElement]?

    /// Whether start is inclusive
    public let startInclusive: Bool

    /// End bound (nil for unbounded)
    public let end: [any TupleElement]?

    /// Whether end is inclusive
    public let endInclusive: Bool

    /// Whether to scan in reverse order
    public let reverse: Bool

    /// Maximum number of results (nil for unlimited)
    public let limit: Int?

    public init(
        start: [any TupleElement]? = nil,
        startInclusive: Bool = true,
        end: [any TupleElement]? = nil,
        endInclusive: Bool = true,
        reverse: Bool = false,
        limit: Int? = nil
    ) {
        self.start = start
        self.startInclusive = startInclusive
        self.end = end
        self.endInclusive = endInclusive
        self.reverse = reverse
        self.limit = limit
    }

    /// Create an equality query
    public static func equals(_ values: [any TupleElement]) -> ScalarIndexQuery {
        ScalarIndexQuery(
            start: values,
            startInclusive: true,
            end: values,
            endInclusive: true
        )
    }

    /// Create a full scan query
    public static var all: ScalarIndexQuery {
        ScalarIndexQuery()
    }
}

// MARK: - Full-Text Index Query

/// Query for full-text indexes
///
/// Uses `TextMatchMode` from FieldConstraint.swift
public struct FullTextIndexQuery: Sendable {
    /// Search terms
    public let terms: [String]

    /// Match mode (uses existing TextMatchMode from FieldConstraint)
    public let matchMode: TextMatchMode

    /// Maximum number of results (nil for unlimited)
    public let limit: Int?

    public init(
        terms: [String],
        matchMode: TextMatchMode = .all,
        limit: Int? = nil
    ) {
        self.terms = terms
        self.matchMode = matchMode
        self.limit = limit
    }
}

// MARK: - Vector Index Query

/// Query for vector similarity search
public struct VectorIndexQuery: Sendable {
    /// The query vector
    public let queryVector: [Float]

    /// Number of nearest neighbors to return
    public let k: Int

    /// HNSW search parameter (exploration factor)
    public let efSearch: Int?

    public init(
        queryVector: [Float],
        k: Int,
        efSearch: Int? = nil
    ) {
        self.queryVector = queryVector
        self.k = k
        self.efSearch = efSearch
    }
}

// MARK: - Spatial Index Query

/// Query for spatial indexes
public struct SpatialIndexQuery: Sendable {
    /// The spatial constraint
    public let constraint: SpatialConstraint

    /// Maximum number of results (nil for unlimited)
    public let limit: Int?

    public init(
        constraint: SpatialConstraint,
        limit: Int? = nil
    ) {
        self.constraint = constraint
        self.limit = limit
    }
}

// MARK: - Aggregation Index Query

/// Query for aggregation indexes (count, sum, min, max)
public struct AggregationIndexQuery: Sendable {
    /// Group key values (for GROUP BY queries)
    public let groupKey: [any TupleElement]?

    public init(groupKey: [any TupleElement]? = nil) {
        self.groupKey = groupKey
    }

    /// Query for all groups
    public static var all: AggregationIndexQuery {
        AggregationIndexQuery(groupKey: nil)
    }

    /// Query for a specific group
    public static func group(_ key: [any TupleElement]) -> AggregationIndexQuery {
        AggregationIndexQuery(groupKey: key)
    }
}

// MARK: - Scalar Index Searcher

/// Searcher for scalar (VALUE) indexes
///
/// **Index Structure**:
/// ```
/// Key: [subspace]/[fieldValue1]/[fieldValue2]/.../[primaryKey]
/// Value: '' (non-covering) or Tuple(coveringFields...) (covering)
/// ```
///
/// **Usage**:
/// ```swift
/// // Get subspace via IndexQueryContext (resolves via DirectoryLayer)
/// let indexSubspace = try await queryContext.indexSubspace(for: Product.self)
///     .subspace(indexDescriptor.name)
///
/// let searcher = ScalarIndexSearcher()
/// let entries = try await searcher.search(
///     query: ScalarIndexQuery.equals(["electronics"]),
///     in: indexSubspace,
///     using: reader
/// )
/// ```
public struct ScalarIndexSearcher: IndexSearcher {
    public typealias Query = ScalarIndexQuery

    /// Number of key fields in the index (for extracting itemID from key)
    private let keyFieldCount: Int

    public init(keyFieldCount: Int = 1) {
        self.keyFieldCount = keyFieldCount
    }

    /// Search the scalar index
    ///
    /// - Parameters:
    ///   - query: The search query with bounds
    ///   - subspace: The index subspace (resolved via DirectoryLayer)
    ///   - reader: Storage reader for raw KV access
    /// - Returns: Matching index entries
    public func search(
        query: ScalarIndexQuery,
        in subspace: Subspace,
        using reader: StorageReader
    ) async throws -> [IndexEntry] {
        // Detect equals query (start == end with both non-nil)
        // For equals queries, use prefix matching since index keys include the ID suffix
        if let start = query.start, let end = query.end,
           Tuple(start).pack() == Tuple(end).pack() {
            return try await searchWithPrefix(
                subspace: subspace,
                prefix: start,
                limit: query.limit,
                reverse: query.reverse,
                using: reader
            )
        }

        // Build start/end tuples for range queries
        let startTuple: Tuple?
        if let start = query.start {
            startTuple = Tuple(start)
        } else {
            startTuple = nil
        }

        let endTuple: Tuple?
        if let end = query.end {
            endTuple = Tuple(end)
        } else {
            endTuple = nil
        }

        var results: [IndexEntry] = []

        for try await (key, value) in reader.scanRange(
            subspace: subspace,
            start: startTuple,
            end: endTuple,
            startInclusive: query.startInclusive,
            endInclusive: query.endInclusive,
            reverse: query.reverse
        ) {
            // Parse key to extract indexed values and itemID
            let entry = try parseIndexEntry(
                key: key,
                value: value,
                subspace: subspace
            )
            results.append(entry)

            // Apply limit if specified
            if let limit = query.limit, results.count >= limit {
                break
            }
        }

        return results
    }

    /// Search using prefix matching for equals queries
    ///
    /// Index keys have the structure: [subspace]/[value1]/[value2]/.../[id]
    /// For equals([value1, value2]), we need to find all keys that start with
    /// the prefix [value1, value2], regardless of the ID suffix.
    private func searchWithPrefix(
        subspace: Subspace,
        prefix: [any TupleElement],
        limit: Int?,
        reverse: Bool,
        using reader: StorageReader
    ) async throws -> [IndexEntry] {
        // Get the prefix key bytes (subspace prefix + tuple prefix)
        let prefixTuple = Tuple(prefix)
        let prefixKey = subspace.pack(prefixTuple)

        var results: [IndexEntry] = []

        // Scan the full subspace and filter by prefix
        for try await (key, value) in reader.scanSubspace(subspace) {
            // Check if key starts with prefix
            guard key.starts(with: prefixKey) else { continue }

            let entry = try parseIndexEntry(key: key, value: value, subspace: subspace)
            results.append(entry)

            if let limit = limit, results.count >= limit {
                break
            }
        }

        if reverse {
            results.reverse()
        }

        return results
    }

    /// Parse an index key/value into an IndexEntry
    ///
    /// Key structure: [subspace]/[fieldValue1]/[fieldValue2]/.../[primaryKey]
    private func parseIndexEntry(
        key: [UInt8],
        value: [UInt8],
        subspace: Subspace
    ) throws -> IndexEntry {
        // Unpack key relative to subspace
        let tuple = try subspace.unpack(key)

        // Key contains: [fieldValue1, fieldValue2, ..., idElement1, idElement2, ...]
        // We need to split into indexed values and itemID
        guard tuple.count > keyFieldCount else {
            throw IndexSearchError.invalidKeyStructure(
                message: "Key has \(tuple.count) elements, expected at least \(keyFieldCount + 1)"
            )
        }

        // Extract indexed values (first keyFieldCount elements) as Tuple
        var keyElements: [any TupleElement] = []
        for i in 0..<keyFieldCount {
            if let element = tuple[i] {
                keyElements.append(element)
            }
        }
        let keyValues = Tuple(keyElements)

        // Extract itemID (remaining elements)
        var idElements: [any TupleElement] = []
        for i in keyFieldCount..<tuple.count {
            if let element = tuple[i] {
                idElements.append(element)
            }
        }
        let itemID = Tuple(idElements)

        // Parse stored values from value (for covering indexes) as Tuple
        let storedValues: Tuple
        if !value.isEmpty {
            let valueElements = try Tuple.unpack(from: value)
            storedValues = Tuple(valueElements)
        } else {
            storedValues = Tuple()
        }

        return IndexEntry(
            itemID: itemID,
            keyValues: keyValues,
            storedValues: storedValues
        )
    }
}

// MARK: - Full-Text Index Searcher

/// Searcher for full-text indexes
///
/// **Index Structure**:
/// ```
/// Key: [subspace]["terms"][term][primaryKey]
/// Value: Tuple(position1, position2, ...) or '' (no positions)
/// ```
///
/// **Usage**:
/// ```swift
/// // Get subspace via IndexQueryContext (resolves via DirectoryLayer)
/// let indexSubspace = try await queryContext.indexSubspace(for: Article.self)
///     .subspace(indexDescriptor.name)
///
/// let searcher = FullTextIndexSearcher()
/// let entries = try await searcher.search(
///     query: FullTextIndexQuery(terms: ["swift", "concurrency"], matchMode: .all),
///     in: indexSubspace,
///     using: reader
/// )
/// ```
public struct FullTextIndexSearcher: IndexSearcher {
    public typealias Query = FullTextIndexQuery

    public init() {}

    /// Search the full-text index
    ///
    /// - Parameters:
    ///   - query: The search query with terms and match mode
    ///   - subspace: The index subspace (resolved via DirectoryLayer)
    ///   - reader: Storage reader for raw KV access
    /// - Returns: Matching index entries
    public func search(
        query: FullTextIndexQuery,
        in subspace: Subspace,
        using reader: StorageReader
    ) async throws -> [IndexEntry] {
        let termsSubspace = subspace.subspace("terms")

        guard !query.terms.isEmpty else {
            return []
        }

        // Normalize search terms
        let normalizedTerms = query.terms.map { $0.lowercased() }

        // Find documents for each term
        // Use [UInt8] (packed Tuple) as Set element for Hashable compatibility
        var termDocumentSets: [Set<[UInt8]>] = []

        for term in normalizedTerms {
            let termSubspace = termsSubspace.subspace(term)
            var documentsForTerm: Set<[UInt8]> = []

            for try await (key, _) in reader.scanSubspace(termSubspace) {
                // Extract primary key from key
                guard let keyTuple = try? termSubspace.unpack(key) else {
                    continue
                }

                // Build primary key tuple and pack to bytes for Set storage
                var idElements: [any TupleElement] = []
                for i in 0..<keyTuple.count {
                    if let element = keyTuple[i] {
                        idElements.append(element)
                    }
                }
                let itemID = Tuple(idElements)
                documentsForTerm.insert(itemID.pack())
            }

            termDocumentSets.append(documentsForTerm)
        }

        // Combine results based on match mode
        let matchingPackedIDs: Set<[UInt8]>
        switch query.matchMode {
        case .all:
            // All terms must be present
            matchingPackedIDs = intersectSets(termDocumentSets)
        case .any:
            // Any term can be present
            matchingPackedIDs = unionSets(termDocumentSets)
        case .phrase:
            // For phrase matching, we'd need position data - for now treat as .all
            matchingPackedIDs = intersectSets(termDocumentSets)
        }

        // Build result entries
        var results: [IndexEntry] = []
        for packedID in matchingPackedIDs {
            // Unpack the ID back to Tuple
            let idElements = try Tuple.unpack(from: packedID)
            let itemID = Tuple(idElements)

            // keyValues contains the matched terms
            let keyValues = Tuple(normalizedTerms.map { $0 as any TupleElement })
            let entry = IndexEntry(
                itemID: itemID,
                keyValues: keyValues,
                storedValues: Tuple()
            )
            results.append(entry)

            // Apply limit if specified
            if let limit = query.limit, results.count >= limit {
                break
            }
        }

        return results
    }

    /// Intersect multiple sets (AND operation)
    private func intersectSets(_ sets: [Set<[UInt8]>]) -> Set<[UInt8]> {
        guard let first = sets.first else { return [] }
        return sets.dropFirst().reduce(first) { $0.intersection($1) }
    }

    /// Union multiple sets (OR operation)
    private func unionSets(_ sets: [Set<[UInt8]>]) -> Set<[UInt8]> {
        return sets.reduce(Set<[UInt8]>()) { $0.union($1) }
    }
}

// MARK: - Vector Index Searcher

/// Searcher for vector similarity indexes (flat scan / brute force version)
///
/// **Index Structure** (Flat):
/// ```
/// Key: [subspace]/[primaryKey]
/// Value: Tuple(float1, float2, ..., floatN)  // vector components
/// ```
///
/// **Usage**:
/// ```swift
/// // Get subspace via IndexQueryContext (resolves via DirectoryLayer)
/// let indexSubspace = try await queryContext.indexSubspace(for: Product.self)
///     .subspace(indexDescriptor.name)
///
/// let searcher = VectorIndexSearcher(dimensions: 128, metric: .cosine)
/// let entries = try await searcher.search(
///     query: VectorIndexQuery(queryVector: queryVec, k: 10),
///     in: indexSubspace,
///     using: reader
/// )
/// ```
///
/// **Note**: This is a brute-force implementation that scans all vectors.
/// For large-scale production use, consider HNSW-based search via `HNSWIndexMaintainer`.
public struct VectorIndexSearcher: IndexSearcher {
    public typealias Query = VectorIndexQuery

    /// Number of dimensions in the vectors
    private let dimensions: Int

    /// Distance metric to use
    private let metric: VectorDistanceMetric

    public init(dimensions: Int, metric: VectorDistanceMetric = .cosine) {
        self.dimensions = dimensions
        self.metric = metric
    }

    /// Search the vector index using flat scan
    ///
    /// - Parameters:
    ///   - query: The search query with query vector and k
    ///   - subspace: The index subspace (resolved via DirectoryLayer)
    ///   - reader: Storage reader for raw KV access
    /// - Returns: Matching index entries sorted by distance (closest first)
    public func search(
        query: VectorIndexQuery,
        in subspace: Subspace,
        using reader: StorageReader
    ) async throws -> [IndexEntry] {
        guard query.queryVector.count == dimensions else {
            throw VectorSearchError.dimensionMismatch(
                expected: dimensions,
                actual: query.queryVector.count
            )
        }

        guard query.k > 0 else {
            throw VectorSearchError.invalidArgument("k must be positive")
        }

        // Use a max-heap to keep track of top k nearest neighbors
        var results: [(entry: IndexEntry, distance: Double)] = []

        for try await (key, value) in reader.scanSubspace(subspace) {
            // Parse primary key from key
            guard let keyTuple = try? subspace.unpack(key) else {
                continue // Skip corrupt entries
            }

            // Parse vector from value
            guard let vector = try? parseVector(from: value) else {
                continue // Skip corrupt entries
            }

            // Calculate distance
            let distance = calculateDistance(query.queryVector, vector)

            // Build entry
            var idElements: [any TupleElement] = []
            for i in 0..<keyTuple.count {
                if let element = keyTuple[i] {
                    idElements.append(element)
                }
            }
            let itemID = Tuple(idElements)

            let entry = IndexEntry(
                itemID: itemID,
                keyValues: Tuple(),
                storedValues: Tuple(),
                score: distance
            )

            // Add to results and maintain heap property
            results.append((entry: entry, distance: distance))
        }

        // Sort by distance and return top k
        results.sort { $0.distance < $1.distance }
        return Array(results.prefix(query.k).map { $0.entry })
    }

    /// Parse a vector from stored bytes
    private func parseVector(from bytes: [UInt8]) throws -> [Float] {
        let elements = try Tuple.unpack(from: bytes)

        var vector: [Float] = []
        vector.reserveCapacity(dimensions)

        for i in 0..<dimensions {
            guard i < elements.count else {
                throw VectorSearchError.invalidVector("Incomplete vector data")
            }

            let element = elements[i]
            if let f = element as? Float {
                vector.append(f)
            } else if let d = element as? Double {
                vector.append(Float(d))
            } else if let i = element as? Int {
                vector.append(Float(i))
            } else if let i64 = element as? Int64 {
                vector.append(Float(i64))
            } else {
                throw VectorSearchError.invalidVector("Cannot convert element to Float")
            }
        }

        return vector
    }

    /// Calculate distance between two vectors
    private func calculateDistance(_ a: [Float], _ b: [Float]) -> Double {
        switch metric {
        case .euclidean:
            return euclideanDistance(a, b)
        case .cosine:
            return cosineDistance(a, b)
        case .dotProduct:
            return 1.0 - dotProduct(a, b)
        }
    }

    private func euclideanDistance(_ a: [Float], _ b: [Float]) -> Double {
        var sum: Float = 0
        for i in 0..<min(a.count, b.count) {
            let diff = a[i] - b[i]
            sum += diff * diff
        }
        return Double(sqrtf(sum))
    }

    private func cosineDistance(_ a: [Float], _ b: [Float]) -> Double {
        var dotProd: Float = 0
        var normA: Float = 0
        var normB: Float = 0

        for i in 0..<min(a.count, b.count) {
            dotProd += a[i] * b[i]
            normA += a[i] * a[i]
            normB += b[i] * b[i]
        }

        let denom = sqrtf(normA) * sqrtf(normB)
        if denom == 0 { return 1.0 }

        let similarity = dotProd / denom
        return Double(1.0 - similarity)
    }

    private func dotProduct(_ a: [Float], _ b: [Float]) -> Double {
        var sum: Float = 0
        for i in 0..<min(a.count, b.count) {
            sum += a[i] * b[i]
        }
        return Double(sum)
    }
}

// MARK: - Spatial Index Searcher

/// Searcher for spatial indexes
///
/// **Index Structure**:
/// ```
/// Key: [subspace][spatialCode][primaryKey]
/// Value: '' (empty)
/// ```
///
/// **Usage**:
/// ```swift
/// // Get subspace via IndexQueryContext (resolves via DirectoryLayer)
/// let indexSubspace = try await queryContext.indexSubspace(for: Location.self)
///     .subspace(indexDescriptor.name)
///
/// let searcher = SpatialIndexSearcher(level: 15)
/// let entries = try await searcher.search(
///     query: SpatialIndexQuery(constraint: .radius(center: (lat, lon), radiusMeters: 1000)),
///     in: indexSubspace,
///     using: reader
/// )
/// ```
public struct SpatialIndexSearcher: IndexSearcher {
    public typealias Query = SpatialIndexQuery

    /// Precision level for Morton encoding
    private let level: Int

    public init(level: Int = 15) {
        self.level = level
    }

    /// Search the spatial index
    ///
    /// - Parameters:
    ///   - query: The search query with spatial constraint
    ///   - subspace: The index subspace (resolved via DirectoryLayer)
    ///   - reader: Storage reader for raw KV access
    /// - Returns: Matching index entries
    public func search(
        query: SpatialIndexQuery,
        in subspace: Subspace,
        using reader: StorageReader
    ) async throws -> [IndexEntry] {
        // Get covering cells for the constraint
        let coveringCells = getCoveringCells(for: query.constraint.type)

        // Collect matching entries from each covering cell
        var seenIDs: Set<[UInt8]> = []
        var results: [IndexEntry] = []

        for cellCode in coveringCells {
            // Create subspace for this cell
            let cellSubspace = subspace.subspace(Int64(bitPattern: cellCode))

            for try await (key, _) in reader.scanSubspace(cellSubspace) {
                // Extract primary key from key
                guard let keyTuple = try? cellSubspace.unpack(key) else {
                    continue
                }

                // Build item ID tuple
                var idElements: [any TupleElement] = []
                for i in 0..<keyTuple.count {
                    if let element = keyTuple[i] {
                        idElements.append(element)
                    }
                }
                let itemID = Tuple(idElements)

                // Deduplicate (same item might appear in multiple covering cells)
                let packedID = itemID.pack()
                if seenIDs.contains(packedID) {
                    continue
                }
                seenIDs.insert(packedID)

                let entry = IndexEntry(
                    itemID: itemID,
                    keyValues: Tuple(),
                    storedValues: Tuple()
                )
                results.append(entry)

                // Apply limit if specified
                if let limit = query.limit, results.count >= limit {
                    return results
                }
            }
        }

        return results
    }

    /// Get covering cells for a spatial constraint (using Morton encoding)
    private func getCoveringCells(for constraintType: SpatialConstraintType) -> [UInt64] {
        switch constraintType {
        case .withinDistance(let center, let radiusMeters):
            // Approximate with bounding box cells
            let earthRadiusMeters = 6_371_000.0
            let latDelta = radiusMeters / earthRadiusMeters * (180.0 / .pi)
            let lonDelta = radiusMeters / (earthRadiusMeters * cos(center.latitude * .pi / 180.0)) * (180.0 / .pi)
            return getCellsForBox(
                minLat: center.latitude - latDelta,
                minLon: center.longitude - lonDelta,
                maxLat: center.latitude + latDelta,
                maxLon: center.longitude + lonDelta
            )
        case .withinBounds(let minLat, let minLon, let maxLat, let maxLon):
            return getCellsForBox(minLat: minLat, minLon: minLon, maxLat: maxLat, maxLon: maxLon)
        case .withinPolygon:
            // For polygon, approximate with bounding box
            return []
        }
    }

    private func getCellsForBox(minLat: Double, minLon: Double, maxLat: Double, maxLon: Double) -> [UInt64] {
        var cells: Set<UInt64> = []
        let step = 180.0 / Double(1 << level)

        var lat = minLat
        while lat <= maxLat {
            var lon = minLon
            while lon <= maxLon {
                // Convert lat/lon to normalized [0,1] coordinates and encode
                let x = (min(max(lon, -180), 180) + 180.0) / 360.0
                let y = (min(max(lat, -90), 90) + 90.0) / 180.0
                let code = encodeMorton(x: x, y: y)
                cells.insert(code)
                lon += step
            }
            lat += step
        }

        return Array(cells)
    }

    /// Simple Morton encoding for 2D coordinates
    private func encodeMorton(x: Double, y: Double) -> UInt64 {
        let maxVal = UInt32(1 << level)
        let xi = UInt32(min(max(x, 0), 1) * Double(maxVal - 1))
        let yi = UInt32(min(max(y, 0), 1) * Double(maxVal - 1))

        var result: UInt64 = 0
        for i in 0..<level {
            result |= UInt64((xi >> i) & 1) << (2 * i)
            result |= UInt64((yi >> i) & 1) << (2 * i + 1)
        }
        return result
    }
}

// MARK: - Index Search Errors

/// Errors during index search operations
public enum IndexSearchError: Error, CustomStringConvertible {
    case invalidKeyStructure(message: String)
    case invalidValueFormat(message: String)
    case indexNotFound(indexName: String)

    public var description: String {
        switch self {
        case .invalidKeyStructure(let message):
            return "Invalid index key structure: \(message)"
        case .invalidValueFormat(let message):
            return "Invalid index value format: \(message)"
        case .indexNotFound(let indexName):
            return "Index not found: \(indexName)"
        }
    }
}

/// Errors during vector index search operations
public enum VectorSearchError: Error, CustomStringConvertible {
    case dimensionMismatch(expected: Int, actual: Int)
    case invalidVector(String)
    case invalidArgument(String)

    public var description: String {
        switch self {
        case .dimensionMismatch(let expected, let actual):
            return "Vector dimension mismatch: expected \(expected), got \(actual)"
        case .invalidVector(let message):
            return "Invalid vector: \(message)"
        case .invalidArgument(let message):
            return "Invalid argument: \(message)"
        }
    }
}
