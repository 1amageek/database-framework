// IndexSearcher.swift
// QueryPlanner - Index search abstraction

import Foundation
import FoundationDB
import Core
import Accelerate

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
    ///
    /// **Optimization**: Uses Subspace.subspace(Tuple) to create a nested subspace
    /// for the prefix, then scans only that range. This converts O(N) full index
    /// scan to O(log N + k) range scan where k = matching entries.
    ///
    /// Reference: FoundationDB's Subspace.range() for efficient prefix scanning
    private func searchWithPrefix(
        subspace: Subspace,
        prefix: [any TupleElement],
        limit: Int?,
        reverse: Bool,
        using reader: StorageReader
    ) async throws -> [IndexEntry] {
        let prefixTuple = Tuple(prefix)

        // Create a subspace for the prefix by directly appending the packed bytes.
        // We cannot use subspace.subspace(prefixTuple) because that treats Tuple as a
        // nested tuple element (with special type code), which doesn't match the actual
        // index key structure where elements are packed directly.
        //
        // Index key structure: [subspace.prefix][value1][value2]...[id]
        // prefixSubspace.prefix = subspace.prefix + Tuple(prefix).pack()
        let prefixSubspace = Subspace(prefix: subspace.prefix + prefixTuple.pack())

        var results: [IndexEntry] = []

        // Scan only the prefix range using the nested subspace
        // nil start/end means "all keys within this subspace"
        for try await (key, value) in reader.scanRange(
            subspace: prefixSubspace,
            start: nil,
            end: nil,
            startInclusive: true,
            endInclusive: false,
            reverse: reverse
        ) {
            // key is the full key (including prefixSubspace.prefix)
            // Since prefixSubspace.prefix starts with subspace.prefix,
            // we can use the original subspace to parse the entry
            let entry = try parseIndexEntry(
                key: key,
                value: value,
                subspace: subspace
            )
            results.append(entry)

            if let limit = limit, results.count >= limit {
                break
            }
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

/// Searcher for full-text indexes with BM25 scoring
///
/// **Index Structure**:
/// ```
/// Key: [subspace]["terms"][term][primaryKey]
/// Value: Tuple(termFreq, positions...) or Tuple(termFreq) or '' (legacy)
///
/// Key: [subspace]["meta"]["docLength"][primaryKey]
/// Value: Tuple(docLength)
///
/// Key: [subspace]["meta"]["totalDocs"]
/// Value: Tuple(count)
///
/// Key: [subspace]["meta"]["avgDocLength"]
/// Value: Tuple(avgLength)
/// ```
///
/// **Optimizations**:
/// - Posting list intersection starts with shortest list
/// - BM25 scoring for relevance ranking
/// - Early termination with limit
/// - Position-based phrase matching
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

    /// BM25 parameters
    private let k1: Double = 1.2  // Term frequency saturation parameter
    private let b: Double = 0.75  // Length normalization parameter

    public init() {}

    /// Search the full-text index with BM25 scoring
    ///
    /// - Parameters:
    ///   - query: The search query with terms and match mode
    ///   - subspace: The index subspace (resolved via DirectoryLayer)
    ///   - reader: Storage reader for raw KV access
    /// - Returns: Matching index entries sorted by BM25 score
    public func search(
        query: FullTextIndexQuery,
        in subspace: Subspace,
        using reader: StorageReader
    ) async throws -> [IndexEntry] {
        let termsSubspace = subspace.subspace("terms")
        let metaSubspace = subspace.subspace("meta")

        guard !query.terms.isEmpty else {
            return []
        }

        // Normalize search terms
        let normalizedTerms = query.terms.map { $0.lowercased() }

        // Load corpus statistics for BM25
        let stats = try await loadCorpusStatistics(from: metaSubspace, using: reader)

        // Find postings for each term with term frequencies
        // PostingEntry: (packedID, termFreq, positions)
        var termPostings: [(term: String, postings: [PostingEntry])] = []

        for term in normalizedTerms {
            let termSubspace = termsSubspace.subspace(term)
            var postings: [PostingEntry] = []

            for try await (key, value) in reader.scanSubspace(termSubspace) {
                guard let keyTuple = try? termSubspace.unpack(key) else {
                    continue
                }

                // Build primary key
                var idElements: [any TupleElement] = []
                for i in 0..<keyTuple.count {
                    if let element = keyTuple[i] {
                        idElements.append(element)
                    }
                }
                let packedID = Tuple(idElements).pack()

                // Parse term frequency and positions from value
                let (termFreq, positions) = parsePostingValue(value)

                postings.append(PostingEntry(
                    packedID: packedID,
                    termFreq: termFreq,
                    positions: positions
                ))
            }

            termPostings.append((term: term, postings: postings))
        }

        // Combine results based on match mode
        let matchingDocs: [ScoredDocument]

        switch query.matchMode {
        case .all:
            matchingDocs = try await intersectPostingsWithScoring(
                termPostings: termPostings,
                stats: stats,
                metaSubspace: metaSubspace,
                reader: reader
            )
        case .any:
            matchingDocs = try await unionPostingsWithScoring(
                termPostings: termPostings,
                stats: stats,
                metaSubspace: metaSubspace,
                reader: reader
            )
        case .phrase:
            matchingDocs = try await phraseMatchWithScoring(
                termPostings: termPostings,
                stats: stats,
                metaSubspace: metaSubspace,
                reader: reader
            )
        }

        // Sort by score descending
        let sortedDocs = matchingDocs.sorted { $0.score > $1.score }

        // Build result entries with limit
        var results: [IndexEntry] = []
        for doc in sortedDocs {
            let idElements = try Tuple.unpack(from: doc.packedID)
            let itemID = Tuple(idElements)

            let entry = IndexEntry(
                itemID: itemID,
                keyValues: Tuple(normalizedTerms.map { $0 as any TupleElement }),
                storedValues: Tuple(),
                score: doc.score
            )
            results.append(entry)

            if let limit = query.limit, results.count >= limit {
                break
            }
        }

        return results
    }

    // MARK: - Posting List Operations

    /// Posting entry with term frequency and positions
    private struct PostingEntry {
        let packedID: [UInt8]
        let termFreq: Int
        let positions: [Int]
    }

    /// Document with BM25 score
    private struct ScoredDocument {
        let packedID: [UInt8]
        var score: Double
    }

    /// Corpus statistics for BM25
    private struct CorpusStatistics {
        var totalDocs: Int = 0
        var avgDocLength: Double = 0
    }

    /// Load corpus statistics from metadata subspace
    private func loadCorpusStatistics(
        from metaSubspace: Subspace,
        using reader: StorageReader
    ) async throws -> CorpusStatistics {
        var stats = CorpusStatistics()

        // Try to load totalDocs
        let totalDocsKey = metaSubspace.pack(Tuple(["totalDocs"]))
        if let value = try await reader.getValue(key: totalDocsKey) {
            let elements = try Tuple.unpack(from: value)
            if let count = elements.first as? Int64 {
                stats.totalDocs = Int(count)
            } else if let count = elements.first as? Int {
                stats.totalDocs = count
            }
        }

        // Try to load avgDocLength
        let avgLengthKey = metaSubspace.pack(Tuple(["avgDocLength"]))
        if let value = try await reader.getValue(key: avgLengthKey) {
            let elements = try Tuple.unpack(from: value)
            if let avg = elements.first as? Double {
                stats.avgDocLength = avg
            } else if let avg = elements.first as? Int64 {
                stats.avgDocLength = Double(avg)
            }
        }

        // Default values if metadata not found
        if stats.totalDocs == 0 { stats.totalDocs = 1 }
        if stats.avgDocLength == 0 { stats.avgDocLength = 100 }

        return stats
    }

    /// Load document length for a specific document
    private func loadDocLength(
        packedID: [UInt8],
        from metaSubspace: Subspace,
        using reader: StorageReader
    ) async throws -> Int {
        let docLengthSubspace = metaSubspace.subspace("docLength")
        let key = docLengthSubspace.prefix + packedID

        if let value = try await reader.getValue(key: key) {
            let elements = try Tuple.unpack(from: value)
            if let length = elements.first as? Int64 {
                return Int(length)
            } else if let length = elements.first as? Int {
                return length
            }
        }

        // Default document length
        return 100
    }

    /// Parse posting value to extract term frequency and positions
    private func parsePostingValue(_ value: [UInt8]) -> (termFreq: Int, positions: [Int]) {
        guard !value.isEmpty else {
            return (1, []) // Legacy: no value means tf=1
        }

        do {
            let elements = try Tuple.unpack(from: value)
            guard !elements.isEmpty else {
                return (1, [])
            }

            // First element is term frequency
            var termFreq = 1
            if let tf = elements.first as? Int64 {
                termFreq = Int(tf)
            } else if let tf = elements.first as? Int {
                termFreq = tf
            }

            // Remaining elements are positions
            var positions: [Int] = []
            for i in 1..<elements.count {
                if let pos = elements[i] as? Int64 {
                    positions.append(Int(pos))
                } else if let pos = elements[i] as? Int {
                    positions.append(pos)
                }
            }

            return (termFreq, positions)
        } catch {
            return (1, [])
        }
    }

    /// Intersect posting lists with BM25 scoring (shortest-first optimization)
    private func intersectPostingsWithScoring(
        termPostings: [(term: String, postings: [PostingEntry])],
        stats: CorpusStatistics,
        metaSubspace: Subspace,
        reader: StorageReader
    ) async throws -> [ScoredDocument] {
        guard !termPostings.isEmpty else { return [] }

        // Sort by posting list length (shortest first)
        let sorted = termPostings.sorted { $0.postings.count < $1.postings.count }

        // Start with shortest posting list
        guard let first = sorted.first else { return [] }

        // Build initial candidate set from shortest list
        var candidates: [ArraySlice<UInt8>: ScoredDocument] = [:]
        let idf = calculateIDF(docFreq: first.postings.count, totalDocs: stats.totalDocs)

        for posting in first.postings {
            let docLength = try await loadDocLength(
                packedID: posting.packedID,
                from: metaSubspace,
                using: reader
            )
            let score = calculateBM25TermScore(
                termFreq: posting.termFreq,
                docLength: docLength,
                avgDocLength: stats.avgDocLength,
                idf: idf
            )
            candidates[posting.packedID[...]] = ScoredDocument(packedID: posting.packedID, score: score)
        }

        // Intersect with remaining lists
        for termPosting in sorted.dropFirst() {
            let postingSet = Set(termPosting.postings.map { $0.packedID[...] })
            let idf = calculateIDF(docFreq: termPosting.postings.count, totalDocs: stats.totalDocs)

            // Remove candidates not in this posting list
            candidates = candidates.filter { postingSet.contains($0.key) }

            // Add scores for matched documents
            for posting in termPosting.postings {
                if var doc = candidates[posting.packedID[...]] {
                    let docLength = try await loadDocLength(
                        packedID: posting.packedID,
                        from: metaSubspace,
                        using: reader
                    )
                    let score = calculateBM25TermScore(
                        termFreq: posting.termFreq,
                        docLength: docLength,
                        avgDocLength: stats.avgDocLength,
                        idf: idf
                    )
                    doc.score += score
                    candidates[posting.packedID[...]] = doc
                }
            }

            // Early termination if no candidates left
            if candidates.isEmpty { break }
        }

        return Array(candidates.values)
    }

    /// Union posting lists with BM25 scoring
    private func unionPostingsWithScoring(
        termPostings: [(term: String, postings: [PostingEntry])],
        stats: CorpusStatistics,
        metaSubspace: Subspace,
        reader: StorageReader
    ) async throws -> [ScoredDocument] {
        var documents: [ArraySlice<UInt8>: ScoredDocument] = [:]

        for termPosting in termPostings {
            let idf = calculateIDF(docFreq: termPosting.postings.count, totalDocs: stats.totalDocs)

            for posting in termPosting.postings {
                let docLength = try await loadDocLength(
                    packedID: posting.packedID,
                    from: metaSubspace,
                    using: reader
                )
                let score = calculateBM25TermScore(
                    termFreq: posting.termFreq,
                    docLength: docLength,
                    avgDocLength: stats.avgDocLength,
                    idf: idf
                )

                if var doc = documents[posting.packedID[...]] {
                    doc.score += score
                    documents[posting.packedID[...]] = doc
                } else {
                    documents[posting.packedID[...]] = ScoredDocument(packedID: posting.packedID, score: score)
                }
            }
        }

        return Array(documents.values)
    }

    /// Phrase match using position information
    private func phraseMatchWithScoring(
        termPostings: [(term: String, postings: [PostingEntry])],
        stats: CorpusStatistics,
        metaSubspace: Subspace,
        reader: StorageReader
    ) async throws -> [ScoredDocument] {
        guard termPostings.count >= 2 else {
            // Single term or empty: fall back to intersection
            return try await intersectPostingsWithScoring(
                termPostings: termPostings,
                stats: stats,
                metaSubspace: metaSubspace,
                reader: reader
            )
        }

        // First, get intersection candidates
        let intersected = try await intersectPostingsWithScoring(
            termPostings: termPostings,
            stats: stats,
            metaSubspace: metaSubspace,
            reader: reader
        )

        // Build position map for phrase checking
        var positionsByDoc: [ArraySlice<UInt8>: [[Int]]] = [:]
        for (termIndex, termPosting) in termPostings.enumerated() {
            for posting in termPosting.postings {
                if positionsByDoc[posting.packedID[...]] == nil {
                    positionsByDoc[posting.packedID[...]] = Array(repeating: [], count: termPostings.count)
                }
                positionsByDoc[posting.packedID[...]]![termIndex] = posting.positions
            }
        }

        // Filter to documents with consecutive positions
        var results: [ScoredDocument] = []
        for doc in intersected {
            guard let positions = positionsByDoc[doc.packedID[...]] else { continue }

            // Check if positions form a phrase (consecutive)
            if hasConsecutivePositions(positions) {
                results.append(doc)
            }
        }

        return results
    }

    /// Check if term positions form a consecutive phrase
    private func hasConsecutivePositions(_ positionLists: [[Int]]) -> Bool {
        guard let firstList = positionLists.first, !firstList.isEmpty else {
            return false
        }

        // For each starting position in first term, check if phrase exists
        for startPos in firstList {
            var matches = true
            for (offset, positions) in positionLists.enumerated() {
                if offset == 0 { continue }
                let expectedPos = startPos + offset
                if !positions.contains(expectedPos) {
                    matches = false
                    break
                }
            }
            if matches { return true }
        }

        return false
    }

    // MARK: - BM25 Scoring

    /// Calculate IDF (Inverse Document Frequency)
    private func calculateIDF(docFreq: Int, totalDocs: Int) -> Double {
        let n = Double(totalDocs)
        let df = Double(max(docFreq, 1))
        return log((n - df + 0.5) / (df + 0.5) + 1)
    }

    /// Calculate BM25 term score
    private func calculateBM25TermScore(
        termFreq: Int,
        docLength: Int,
        avgDocLength: Double,
        idf: Double
    ) -> Double {
        let tf = Double(termFreq)
        let dl = Double(docLength)
        let avgdl = max(avgDocLength, 1.0)

        let numerator = tf * (k1 + 1)
        let denominator = tf + k1 * (1 - b + b * (dl / avgdl))

        return idf * (numerator / denominator)
    }
}

// MARK: - Vector Index Searcher

/// Searcher for vector similarity indexes (flat scan with SIMD optimization)
///
/// **Index Structure** (Flat):
/// ```
/// Key: [subspace]/[primaryKey]
/// Value: Tuple(float1, float2, ..., floatN)  // vector components
/// ```
///
/// **Optimizations**:
/// - SIMD distance calculation using Accelerate/vDSP
/// - Max-heap for top-K maintenance (O(n log k) instead of O(n log n))
/// - Early termination when sufficient candidates found
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
/// **Note**: This is a brute-force implementation with SIMD optimization.
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

    /// Search the vector index using flat scan with SIMD optimization
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

        // Use max-heap for top-K maintenance
        var topK = MaxHeap<HeapEntry>(capacity: query.k)

        for try await (key, value) in reader.scanSubspace(subspace) {
            // Parse primary key from key
            guard let keyTuple = try? subspace.unpack(key) else {
                continue // Skip corrupt entries
            }

            // Parse vector from value
            guard let vector = try? parseVector(from: value) else {
                continue // Skip corrupt entries
            }

            // Calculate distance using SIMD
            let distance = calculateDistanceSIMD(query.queryVector, vector)

            // Early skip: if heap is full and this distance is worse than max, skip
            if topK.isFull, let maxDist = topK.peek()?.distance, distance >= maxDist {
                continue
            }

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

            // Add to max-heap (automatically maintains top-K)
            topK.insert(HeapEntry(entry: entry, distance: distance))
        }

        // Extract results sorted by distance (ascending)
        return topK.extractSorted().map { $0.entry }
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
            if let f = TypeConversion.asFloat(element) {
                vector.append(f)
            } else {
                throw VectorSearchError.invalidVector("Cannot convert element to Float")
            }
        }

        return vector
    }

    /// Calculate distance between two vectors using SIMD (Accelerate framework)
    private func calculateDistanceSIMD(_ a: [Float], _ b: [Float]) -> Double {
        switch metric {
        case .euclidean:
            return euclideanDistanceSIMD(a, b)
        case .cosine:
            return cosineDistanceSIMD(a, b)
        case .dotProduct:
            return 1.0 - dotProductSIMD(a, b)
        }
    }

    /// SIMD-optimized Euclidean distance using vDSP
    private func euclideanDistanceSIMD(_ a: [Float], _ b: [Float]) -> Double {
        let count = vDSP_Length(min(a.count, b.count))
        guard count > 0 else { return 0 }

        // Compute difference: diff = a - b
        var diff = [Float](repeating: 0, count: Int(count))
        vDSP_vsub(b, 1, a, 1, &diff, 1, count)

        // Compute sum of squares
        var sumOfSquares: Float = 0
        vDSP_svesq(diff, 1, &sumOfSquares, count)

        return Double(sqrtf(sumOfSquares))
    }

    /// SIMD-optimized Cosine distance using vDSP
    private func cosineDistanceSIMD(_ a: [Float], _ b: [Float]) -> Double {
        let count = vDSP_Length(min(a.count, b.count))
        guard count > 0 else { return 1.0 }

        // Compute dot product: a · b
        var dotProd: Float = 0
        vDSP_dotpr(a, 1, b, 1, &dotProd, count)

        // Compute squared norms
        var normASquared: Float = 0
        var normBSquared: Float = 0
        vDSP_svesq(a, 1, &normASquared, count)
        vDSP_svesq(b, 1, &normBSquared, count)

        let denom = sqrtf(normASquared) * sqrtf(normBSquared)
        if denom == 0 { return 1.0 }

        let similarity = dotProd / denom
        return Double(1.0 - similarity)
    }

    /// SIMD-optimized dot product using vDSP
    private func dotProductSIMD(_ a: [Float], _ b: [Float]) -> Double {
        let count = vDSP_Length(min(a.count, b.count))
        guard count > 0 else { return 0 }

        var result: Float = 0
        vDSP_dotpr(a, 1, b, 1, &result, count)

        return Double(result)
    }

    /// Entry for max-heap storage
    ///
    /// For a max-heap maintaining top-K smallest distances:
    /// - Root should have the largest distance among K elements
    /// - We replace root when finding a smaller distance
    /// - Standard < comparison: smaller distance is "less than"
    private struct HeapEntry: Comparable {
        let entry: IndexEntry
        let distance: Double

        static func < (lhs: HeapEntry, rhs: HeapEntry) -> Bool {
            // Standard comparison: smaller distance is less
            // Max-heap will put larger distances at root
            lhs.distance < rhs.distance
        }

        static func == (lhs: HeapEntry, rhs: HeapEntry) -> Bool {
            lhs.distance == rhs.distance
        }
    }
}

// MARK: - Max-Heap for Top-K

/// Fixed-capacity max-heap for maintaining top-K smallest elements
///
/// Inserts are O(log k), maintains only k elements at any time.
private struct MaxHeap<Element: Comparable> {
    private var elements: [Element] = []
    private let capacity: Int

    var isFull: Bool { elements.count >= capacity }
    var count: Int { elements.count }

    init(capacity: Int) {
        self.capacity = capacity
        elements.reserveCapacity(capacity + 1)
    }

    /// Peek at the maximum element (worst in top-K)
    func peek() -> Element? {
        elements.first
    }

    /// Insert an element, maintaining capacity
    mutating func insert(_ element: Element) {
        if elements.count < capacity {
            // Heap not full, just add
            elements.append(element)
            siftUp(elements.count - 1)
        } else if element < elements[0] {
            // New element is better than worst, replace
            elements[0] = element
            siftDown(0)
        }
        // Otherwise, skip (new element is worse than all in heap)
    }

    /// Extract all elements sorted (ascending for top-K smallest)
    func extractSorted() -> [Element] {
        elements.sorted()
    }

    private mutating func siftUp(_ index: Int) {
        var child = index
        var parent = (child - 1) / 2

        while child > 0 && elements[child] > elements[parent] {
            elements.swapAt(child, parent)
            child = parent
            parent = (child - 1) / 2
        }
    }

    private mutating func siftDown(_ index: Int) {
        var parent = index
        let count = elements.count

        while true {
            let left = 2 * parent + 1
            let right = 2 * parent + 2
            var largest = parent

            if left < count && elements[left] > elements[largest] {
                largest = left
            }
            if right < count && elements[right] > elements[largest] {
                largest = right
            }

            if largest == parent { break }

            elements.swapAt(parent, largest)
            parent = largest
        }
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
