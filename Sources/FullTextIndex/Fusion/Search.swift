// Search.swift
// FullTextIndex - FullText search query for Fusion
//
// This file is part of FullTextIndex module, not DatabaseEngine.
// DatabaseEngine does not know about FullTextIndexKind.

import Foundation
import Core
import DatabaseEngine
import FoundationDB
import FullText

/// FullText search query for Fusion
///
/// Searches text fields using inverted index with optional BM25 scoring.
///
/// **Usage**:
/// ```swift
/// let results = try await context.fuse(Article.self) {
///     Search(\.content)
///         .terms(["swift", "concurrency"])
///         .mode(.all)
///         .bm25(k1: 1.5, b: 0.75)
/// }
/// .execute()
/// ```
public struct Search<T: Persistable>: FusionQuery, Sendable {
    public typealias Item = T

    private let queryContext: IndexQueryContext
    private let fieldName: String
    private var searchTerms: [String] = []
    private var matchMode: TextMatchMode = .all
    private var k1: Float = 1.2
    private var b: Float = 0.75

    // MARK: - Initialization (FusionContext)

    /// Create a Search query for a text field
    ///
    /// Uses FusionContext.current for context (automatically set by `context.fuse { }`).
    ///
    /// - Parameter keyPath: KeyPath to the String field to search
    ///
    /// **Usage**:
    /// ```swift
    /// context.fuse(Article.self) {
    ///     Search(\.content).terms(["swift", "concurrency"])
    /// }
    /// ```
    public init(_ keyPath: KeyPath<T, String>) {
        guard let context = FusionContext.current else {
            fatalError("Search must be used within context.fuse { } block")
        }
        self.fieldName = T.fieldName(for: keyPath)
        self.queryContext = context
    }

    /// Create a Search query for an optional text field
    ///
    /// Uses FusionContext.current for context (automatically set by `context.fuse { }`).
    ///
    /// - Parameter keyPath: KeyPath to the optional String field to search
    public init(_ keyPath: KeyPath<T, String?>) {
        guard let context = FusionContext.current else {
            fatalError("Search must be used within context.fuse { } block")
        }
        self.fieldName = T.fieldName(for: keyPath)
        self.queryContext = context
    }

    // MARK: - Initialization (Explicit Context)

    /// Create a Search query for a text field with explicit context
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the String field to search
    ///   - context: IndexQueryContext for database access
    public init(_ keyPath: KeyPath<T, String>, context: IndexQueryContext) {
        self.fieldName = T.fieldName(for: keyPath)
        self.queryContext = context
    }

    /// Create a Search query for an optional text field with explicit context
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the optional String field to search
    ///   - context: IndexQueryContext for database access
    public init(_ keyPath: KeyPath<T, String?>, context: IndexQueryContext) {
        self.fieldName = T.fieldName(for: keyPath)
        self.queryContext = context
    }

    /// Create a Search query with a field name string
    ///
    /// - Parameters:
    ///   - fieldName: The field name to search
    ///   - context: IndexQueryContext for database access
    public init(fieldName: String, context: IndexQueryContext) {
        self.fieldName = fieldName
        self.queryContext = context
    }

    // MARK: - Configuration

    /// Set search terms
    ///
    /// - Parameter terms: Array of terms to search for
    /// - Returns: Updated query
    public func terms(_ terms: [String]) -> Self {
        var copy = self
        copy.searchTerms = terms
        return copy
    }

    /// Set search terms with match mode
    ///
    /// - Parameters:
    ///   - terms: Array of terms to search for
    ///   - mode: How to match terms (.all = AND, .any = OR, .phrase)
    /// - Returns: Updated query
    public func terms(_ terms: [String], mode: TextMatchMode) -> Self {
        var copy = self
        copy.searchTerms = terms
        copy.matchMode = mode
        return copy
    }

    /// Set match mode
    ///
    /// - Parameter mode: How to match terms
    /// - Returns: Updated query
    public func mode(_ mode: TextMatchMode) -> Self {
        var copy = self
        copy.matchMode = mode
        return copy
    }

    /// Set BM25 parameters
    ///
    /// - Parameters:
    ///   - k1: Term frequency saturation (default: 1.2)
    ///   - b: Document length normalization (default: 0.75)
    /// - Returns: Updated query
    public func bm25(k1: Float = 1.2, b: Float = 0.75) -> Self {
        var copy = self
        copy.k1 = k1
        copy.b = b
        return copy
    }

    // MARK: - Index Discovery

    /// Find the index descriptor using kindIdentifier and fieldName
    private func findIndexDescriptor() -> IndexDescriptor? {
        T.indexDescriptors.first { descriptor in
            // 1. Filter by kindIdentifier
            guard descriptor.kindIdentifier == FullTextIndexKind<T>.identifier else {
                return false
            }
            // 2. Match by fieldName
            guard let kind = descriptor.kind as? FullTextIndexKind<T> else {
                return false
            }
            return kind.fieldNames.contains(fieldName)
        }
    }

    // MARK: - FusionQuery

    public func execute(candidates: Set<String>?) async throws -> [ScoredResult<T>] {
        guard !searchTerms.isEmpty else { return [] }

        // Find index descriptor
        guard let descriptor = findIndexDescriptor() else {
            throw FusionQueryError.indexNotFound(
                type: T.persistableType,
                field: fieldName,
                kind: "fulltext"
            )
        }

        let indexName = descriptor.name

        // Get index subspace
        let typeSubspace = try await queryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(indexName)

        // Execute full-text search
        let scoredIds: [(id: Tuple, score: Double)] = try await queryContext.withTransaction { transaction in
            try await self.searchFullText(
                terms: self.searchTerms,
                matchMode: self.matchMode,
                indexSubspace: indexSubspace,
                transaction: transaction
            )
        }

        // Fetch items by primary keys
        var items = try await queryContext.fetchItems(ids: scoredIds.map(\.id), type: T.self)

        // Filter to candidates if provided
        if let candidateIds = candidates {
            items = items.filter { candidateIds.contains("\($0.id)") }
        }

        // Match items with scores
        var results: [ScoredResult<T>] = []
        for item in items {
            // Find matching score
            for result in scoredIds {
                if let pkId = result.id[0] as? String, "\(item.id)" == pkId {
                    results.append(ScoredResult(item: item, score: result.score))
                    break
                } else if let pkId = result.id[0] as? Int64, "\(item.id)" == "\(pkId)" {
                    results.append(ScoredResult(item: item, score: result.score))
                    break
                }
            }
        }

        // Sort by score descending
        return results.sorted { $0.score > $1.score }
    }

    // MARK: - FullText Index Reading

    /// Index structure:
    /// - `[indexSubspace]["terms"][term][primaryKey]` → positions or tf
    /// - `[indexSubspace]["docs"][primaryKey]` → (uniqueTermCount, docLength)
    /// - `[indexSubspace]["stats"]["N"]` → total document count
    /// - `[indexSubspace]["stats"]["totalLength"]` → sum of document lengths
    /// - `[indexSubspace]["df"][term]` → document frequency

    /// Search full-text index and return scored results
    private func searchFullText(
        terms: [String],
        matchMode: TextMatchMode,
        indexSubspace: Subspace,
        transaction: any TransactionProtocol
    ) async throws -> [(id: Tuple, score: Double)] {
        let termsSubspace = indexSubspace.subspace("terms")
        let docsSubspace = indexSubspace.subspace("docs")
        let statsSubspace = indexSubspace.subspace("stats")
        let dfSubspace = indexSubspace.subspace("df")

        // Normalize terms (simple lowercase for now)
        let normalizedTerms = terms.map { $0.lowercased().trimmingCharacters(in: .whitespaces) }

        // Get matching document IDs based on match mode
        let matchingIds: [[any TupleElement]]
        switch matchMode {
        case .all:
            matchingIds = try await searchTermsAND(
                normalizedTerms,
                termsSubspace: termsSubspace,
                transaction: transaction
            )
        case .any:
            matchingIds = try await searchTermsOR(
                normalizedTerms,
                termsSubspace: termsSubspace,
                transaction: transaction
            )
        case .phrase:
            // For phrase search, join terms and search as AND first, then verify positions
            matchingIds = try await searchTermsAND(
                normalizedTerms,
                termsSubspace: termsSubspace,
                transaction: transaction
            )
        }

        guard !matchingIds.isEmpty else { return [] }

        // Get BM25 statistics
        let stats = try await getBM25Statistics(
            statsSubspace: statsSubspace,
            transaction: transaction
        )
        guard stats.totalDocuments > 0 else {
            // No statistics, return with equal scores
            return matchingIds.map { (id: Tuple($0), score: 1.0) }
        }

        // Get document frequencies for all terms
        var documentFrequencies: [String: Int64] = [:]
        for term in normalizedTerms {
            documentFrequencies[term] = try await getDocumentFrequency(
                term: term,
                dfSubspace: dfSubspace,
                transaction: transaction
            )
        }

        // Calculate BM25 scores for each document
        var scoredResults: [(id: Tuple, score: Double)] = []

        for docElements in matchingIds {
            let docId = Tuple(docElements)

            // Get document metadata
            guard let metadata = try await getDocumentMetadata(
                id: docId,
                docsSubspace: docsSubspace,
                transaction: transaction
            ) else {
                continue
            }

            // Get term frequencies in this document
            var termFrequencies: [String: Int] = [:]
            for term in normalizedTerms {
                let termSubspace = termsSubspace.subspace(term)
                let termKey = termSubspace.pack(docId)
                if let value = try await transaction.getValue(for: termKey, snapshot: true) {
                    // Try to decode as positions first, then as tf
                    if let tfTuple = try? Tuple.unpack(from: value) {
                        termFrequencies[term] = tfTuple.count > 0 ? Int(tfTuple.count) : 1
                    } else {
                        termFrequencies[term] = 1
                    }
                }
            }

            // Calculate BM25 score
            let score = calculateBM25Score(
                termFrequencies: termFrequencies,
                documentFrequencies: documentFrequencies,
                docLength: Int(metadata.docLength),
                stats: stats
            )

            scoredResults.append((id: docId, score: score))
        }

        // Sort by score descending
        scoredResults.sort { $0.score > $1.score }

        return scoredResults
    }

    /// Search for documents containing all terms (AND query)
    private func searchTermsAND(
        _ terms: [String],
        termsSubspace: Subspace,
        transaction: any TransactionProtocol
    ) async throws -> [[any TupleElement]] {
        guard !terms.isEmpty else { return [] }

        var intersection: Set<String>? = nil
        var idToElements: [String: [any TupleElement]] = [:]

        for term in terms {
            let results = try await searchTerm(
                term,
                termsSubspace: termsSubspace,
                transaction: transaction
            )
            var currentSet: Set<String> = []

            for elements in results {
                let idKey = elementsToStableKey(elements)
                currentSet.insert(idKey)

                if intersection == nil || intersection!.contains(idKey) {
                    idToElements[idKey] = elements
                }
            }

            if let prev = intersection {
                intersection = prev.intersection(currentSet)
                if intersection!.isEmpty {
                    return []
                }
            } else {
                intersection = currentSet
            }
        }

        guard let finalIntersection = intersection else { return [] }
        return finalIntersection.compactMap { idToElements[$0] }
    }

    /// Search for documents containing any term (OR query)
    private func searchTermsOR(
        _ terms: [String],
        termsSubspace: Subspace,
        transaction: any TransactionProtocol
    ) async throws -> [[any TupleElement]] {
        guard !terms.isEmpty else { return [] }

        var idToElements: [String: [any TupleElement]] = [:]

        for term in terms {
            let results = try await searchTerm(
                term,
                termsSubspace: termsSubspace,
                transaction: transaction
            )

            for elements in results {
                let idKey = elementsToStableKey(elements)
                idToElements[idKey] = elements
            }
        }

        return Array(idToElements.values)
    }

    /// Search for documents containing a term
    private func searchTerm(
        _ term: String,
        termsSubspace: Subspace,
        transaction: any TransactionProtocol
    ) async throws -> [[any TupleElement]] {
        let termSubspace = termsSubspace.subspace(term)
        let (begin, end) = termSubspace.range()

        var results: [[any TupleElement]] = []

        let sequence = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(begin),
            endSelector: .firstGreaterOrEqual(end),
            snapshot: true
        )

        for try await (key, _) in sequence {
            guard termSubspace.contains(key) else { break }

            guard let keyTuple = try? termSubspace.unpack(key),
                  let elements = try? Tuple.unpack(from: keyTuple.pack()) else {
                continue
            }
            results.append(elements)
        }

        return results
    }

    // MARK: - BM25 Scoring

    private struct BM25Stats {
        let totalDocuments: Int64
        let totalLength: Int64
        var avgDocLength: Double {
            totalDocuments > 0 ? Double(totalLength) / Double(totalDocuments) : 0
        }
    }

    private func getBM25Statistics(
        statsSubspace: Subspace,
        transaction: any TransactionProtocol
    ) async throws -> BM25Stats {
        let nKey = statsSubspace.pack(Tuple("N"))
        let lengthKey = statsSubspace.pack(Tuple("totalLength"))

        let nValue = try await transaction.getValue(for: nKey, snapshot: true)
        let lengthValue = try await transaction.getValue(for: lengthKey, snapshot: true)

        let n: Int64 = nValue.map { bytesToInt64($0) } ?? 0
        let totalLength: Int64 = lengthValue.map { bytesToInt64($0) } ?? 0

        return BM25Stats(totalDocuments: n, totalLength: totalLength)
    }

    private func getDocumentFrequency(
        term: String,
        dfSubspace: Subspace,
        transaction: any TransactionProtocol
    ) async throws -> Int64 {
        let dfKey = dfSubspace.pack(Tuple(term))
        let value = try await transaction.getValue(for: dfKey, snapshot: true)
        return value.map { bytesToInt64($0) } ?? 0
    }

    private func getDocumentMetadata(
        id: Tuple,
        docsSubspace: Subspace,
        transaction: any TransactionProtocol
    ) async throws -> (uniqueTermCount: Int64, docLength: Int64)? {
        let docKey = docsSubspace.pack(id)
        guard let value = try await transaction.getValue(for: docKey, snapshot: true) else {
            return nil
        }
        let tuple = try Tuple.unpack(from: value)
        guard tuple.count >= 2,
              let termCount = tuple[0] as? Int64,
              let docLength = tuple[1] as? Int64 else {
            if let termCount = tuple[0] as? Int64 {
                return (uniqueTermCount: termCount, docLength: 0)
            }
            return nil
        }
        return (uniqueTermCount: termCount, docLength: docLength)
    }

    /// Calculate BM25 score for a document
    ///
    /// BM25 formula:
    /// score = Σ IDF(t) * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * dl/avgdl))
    private func calculateBM25Score(
        termFrequencies: [String: Int],
        documentFrequencies: [String: Int64],
        docLength: Int,
        stats: BM25Stats
    ) -> Double {
        var score: Double = 0.0

        for (term, tf) in termFrequencies {
            let df = documentFrequencies[term] ?? 0

            // IDF = ln((N - df + 0.5) / (df + 0.5) + 1)
            let idf = log((Double(stats.totalDocuments) - Double(df) + 0.5) / (Double(df) + 0.5) + 1.0)

            // TF component with length normalization
            let tfDouble = Double(tf)
            let k1Double = Double(k1)
            let bDouble = Double(b)
            let dlNorm = stats.avgDocLength > 0 ? Double(docLength) / stats.avgDocLength : 1.0

            let tfNormalized = (tfDouble * (k1Double + 1)) /
                               (tfDouble + k1Double * (1 - bDouble + bDouble * dlNorm))

            score += idf * tfNormalized
        }

        return score
    }

    // MARK: - Helpers

    private func elementsToStableKey(_ elements: [any TupleElement]) -> String {
        let packed = Tuple(elements).pack()
        return Data(packed).base64EncodedString()
    }

    private func bytesToInt64(_ bytes: [UInt8]) -> Int64 {
        guard bytes.count >= 8 else { return 0 }
        return bytes.withUnsafeBytes { $0.load(as: Int64.self) }
    }
}
