// Search.swift
// FullTextIndex - FullText search query for Fusion
//
// This file is part of FullTextIndex module, not DatabaseEngine.
// DatabaseEngine does not know about FullTextIndexKind.

import DatabaseTypes
#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import DatabaseKit
import DatabaseMath
import DatabaseEngine
import StorageKit

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
    private let field: FieldIdentity
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
    public init(_ field: Field<T, String>) {
        guard let context = FusionContext.current else {
            fatalError("Search must be used within context.fuse { } block")
        }
        self.field = field.identity
        self.queryContext = context
    }

    /// Create a Search query for an optional text field
    ///
    /// Uses FusionContext.current for context (automatically set by `context.fuse { }`).
    ///
    /// - Parameter keyPath: KeyPath to the optional String field to search
    public init(_ field: Field<T, String?>) {
        guard let context = FusionContext.current else {
            fatalError("Search must be used within context.fuse { } block")
        }
        self.field = field.identity
        self.queryContext = context
    }

    // MARK: - Initialization (Explicit Context)

    /// Create a Search query for a text field with explicit context
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the String field to search
    ///   - context: IndexQueryContext for database access
    public init(_ field: Field<T, String>, context: IndexQueryContext) {
        self.field = field.identity
        self.queryContext = context
    }

    /// Create a Search query for an optional text field with explicit context
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the optional String field to search
    ///   - context: IndexQueryContext for database access
    public init(_ field: Field<T, String?>, context: IndexQueryContext) {
        self.field = field.identity
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
    private func resolveIndexDescriptor() throws -> IndexDescriptor {
        let matches = try T.indexDescriptors.filter { descriptor in
            descriptor.kindIdentifier == "fulltext"
                && descriptor.kind.fields.contains(where: {
                    $0.identity == field
                })
        }
        guard let descriptor = matches.first else {
            throw FusionQueryError.indexNotFound(
                type: T.persistableType,
                field: field.name,
                kind: "fulltext"
            )
        }
        guard matches.count == 1 else {
            throw FullTextQueryError.ambiguousIndex(
                entity: T.persistableType,
                field: field.name
            )
        }
        return descriptor
    }

    // MARK: - FusionQuery

    public func execute(candidates: Set<T.ID>?) async throws -> [ScoredResult<T>] {
        guard !searchTerms.isEmpty else { return [] }

        // Find index descriptor
        let descriptor = try resolveIndexDescriptor()
        let configuration = try FullTextIndexConfiguration(
            metadata: descriptor.kind
        )

        let indexName = descriptor.name

        // Get index subspace
        let typeSubspace = try await queryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(indexName)

        // Execute full-text search
        let scoredIds: [(id: Tuple, score: Double)] = try await queryContext.withTransaction { transaction in
            try await self.searchFullText(
                terms: self.searchTerms,
                matchMode: self.matchMode,
                configuration: configuration,
                indexSubspace: indexSubspace,
                transaction: transaction
            )
        }

        // Fetch items by primary keys
        var items = try await queryContext.fetchItems(ids: scoredIds.map(\.id), type: T.self)

        // Filter to candidates if provided
        if let candidateIDs = candidates {
            items = items.filter { candidateIDs.contains($0.id) }
        }

        // Match items with scores
        var scoresByIdentifier: [ByteString: Double] = [:]
        scoresByIdentifier.reserveCapacity(scoredIds.count)
        for result in scoredIds {
            scoresByIdentifier[
                FullTextDocumentLookupKey.key(for: result.id)
            ] = result.score
        }
        var results: [ScoredResult<T>] = []
        results.reserveCapacity(items.count)
        for item in items {
            let key = try FullTextDocumentLookupKey.key(for: item)
            if let score = scoresByIdentifier[key] {
                results.append(ScoredResult(item: item, score: score))
            }
        }

        // Sort by score descending
        return results.sorted { $0.score > $1.score }
    }

    // MARK: - FullText Index Reading

    /// Index structure:
    /// - `[indexSubspace]["terms"][term][primaryKey]` → (termFrequency, positions...)
    /// - `[indexSubspace]["docs"][primaryKey]` → (uniqueTermCount, docLength)
    /// - `[indexSubspace]["stats"]["N"]` → total document count
    /// - `[indexSubspace]["stats"]["totalLength"]` → sum of document lengths
    /// - `[indexSubspace]["df"][term]` → document frequency

    /// Search full-text index and return scored results
    private func searchFullText(
        terms: [String],
        matchMode: TextMatchMode,
        configuration: FullTextIndexConfiguration,
        indexSubspace: Subspace,
        transaction: any TransactionAccess
    ) async throws -> [(id: Tuple, score: Double)] {
        let termsSubspace = indexSubspace.subspace("terms")
        let docsSubspace = indexSubspace.subspace("docs")
        let statsSubspace = indexSubspace.subspace("stats")
        let dfSubspace = indexSubspace.subspace("df")

        let termGroups = normalizeQueryTermGroups(
            terms,
            configuration: configuration
        )
        let normalizedTerms = uniqueTerms(termGroups.flatMap { $0 })

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
            var idToElements: [ByteString: [any TupleElement]] = [:]
            for group in termGroups {
                let matches = try await searchTermsAND(
                    group,
                    termsSubspace: termsSubspace,
                    transaction: transaction
                )
                for elements in matches {
                    idToElements[elementsToStableKey(elements)] = elements
                }
            }
            matchingIds = Array(idToElements.values)
        case .phrase:
            // Position-verified phrase search via FullTextIndexMaintainer.searchPhrase()
            matchingIds = try await searchPhraseIds(
                indexSubspace: indexSubspace,
                transaction: transaction
            )
        }

        guard !matchingIds.isEmpty else { return [] }

        // Get BM25 statistics
        let stats = try await getBM25Statistics(
            statsSubspace: statsSubspace,
            transaction: transaction
        )
        guard stats.totalDocuments > 0, stats.totalLength > 0 else {
            throw FullTextStorageError.corruptedCorpusStatistics
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
                throw FullTextStorageError.missingDocumentMetadata
            }

            // Get term frequencies in this document
            var termFrequencies: [String: Int] = [:]
            for term in normalizedTerms {
                let termSubspace = termsSubspace.subspace(term)
                let termKey = termSubspace.pack(docId)
                if let value = try await transaction.getValue(for: termKey, snapshot: true) {
                    let posting = try FullTextStorageDecoder.posting(
                        from: value,
                        positionsStored: configuration.storePositions,
                        term: term
                    )
                    termFrequencies[term] = posting.termFrequency
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
        transaction: any TransactionAccess
    ) async throws -> [[any TupleElement]] {
        guard !terms.isEmpty else { return [] }

        var intersection: Set<ByteString>? = nil
        var idToElements: [ByteString: [any TupleElement]] = [:]

        for term in terms {
            let results = try await searchTerm(
                term,
                termsSubspace: termsSubspace,
                transaction: transaction
            )
            var currentSet: Set<ByteString> = []

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
        transaction: any TransactionAccess
    ) async throws -> [[any TupleElement]] {
        guard !terms.isEmpty else { return [] }

        var idToElements: [ByteString: [any TupleElement]] = [:]

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
        transaction: any TransactionAccess
    ) async throws -> [[any TupleElement]] {
        let termSubspace = termsSubspace.subspace(term)
        let (begin, end) = termSubspace.range()

        var results: [[any TupleElement]] = []

        let sequence = try await transaction.collectRange(
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            snapshot: true
        )

        for (key, _) in sequence {
            guard termSubspace.contains(key) else { break }

            let keyTuple = try termSubspace.unpack(key)
            let elements = try keyTuple.elements()
            results.append(elements)
        }

        return results
    }

    // MARK: - Phrase Search

    /// Search for exact phrase matches using position-verified matching
    ///
    /// Creates a `FullTextIndexMaintainer` and delegates to `searchPhrase()`,
    /// which verifies term positions form a consecutive sequence.
    /// Requires `storePositions=true` on the index — throws
    /// `FullTextIndexError.invalidQuery` otherwise.
    private func searchPhraseIds(
        indexSubspace: Subspace,
        transaction: any TransactionAccess
    ) async throws -> [[any TupleElement]] {
        let descriptor = try resolveIndexDescriptor()
        let configuration = try FullTextIndexConfiguration(
            metadata: descriptor.kind
        )

        let index = Index(
            name: descriptor.name,
            kind: descriptor.kind,
            rootExpression: KeyExpressionFactory.from(keyPaths: descriptor.fieldNames),
            isUnique: descriptor.isUnique,
            storedFieldNames: descriptor.storedFieldNames
        )

        let maintainer = FullTextIndexMaintainer<T>(
            index: index,
            tokenizer: configuration.tokenizer,
            storePositions: configuration.storePositions,
            ngramSize: configuration.ngramSize,
            minTermLength: configuration.minTermLength,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id")
        )

        let phraseString = searchTerms.joined(separator: " ")
        return try await maintainer.searchPhrase(phraseString, transaction: transaction)
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
        transaction: any TransactionAccess
    ) async throws -> BM25Stats {
        let nKey = statsSubspace.pack(Tuple("N"))
        let lengthKey = statsSubspace.pack(Tuple("totalLength"))

        let nValue = try await transaction.getValue(for: nKey, snapshot: true)
        let lengthValue = try await transaction.getValue(for: lengthKey, snapshot: true)

        let n: Int64
        if let nValue {
            n = try bytesToInt64(nValue)
        } else {
            n = 0
        }
        let totalLength: Int64
        if let lengthValue {
            totalLength = try bytesToInt64(lengthValue)
        } else {
            totalLength = 0
        }

        return BM25Stats(totalDocuments: n, totalLength: totalLength)
    }

    private func getDocumentFrequency(
        term: String,
        dfSubspace: Subspace,
        transaction: any TransactionAccess
    ) async throws -> Int64 {
        let dfKey = dfSubspace.pack(Tuple(term))
        let value = try await transaction.getValue(for: dfKey, snapshot: true)
        guard let value else { return 0 }
        return try bytesToInt64(value)
    }

    private func getDocumentMetadata(
        id: Tuple,
        docsSubspace: Subspace,
        transaction: any TransactionAccess
    ) async throws -> (uniqueTermCount: Int64, docLength: Int64)? {
        let docKey = docsSubspace.pack(id)
        guard let value = try await transaction.getValue(for: docKey, snapshot: true) else {
            return nil
        }
        return try FullTextStorageDecoder.documentMetadata(from: value)
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

            // Standard BM25 IDF: log((N - df + 0.5) / (df + 0.5))
            // Reference: Robertson & Zaragoza (2009), "The Probabilistic Relevance Framework: BM25 and Beyond"
            let idf = DatabaseMath.naturalLogarithm(
                (Double(stats.totalDocuments) - Double(df) + 0.5) /
                    (Double(df) + 0.5)
            )

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

    private func elementsToStableKey(
        _ elements: [any TupleElement]
    ) -> ByteString {
        Tuple(elements).pack()
    }

    private func normalizeQueryTermGroups(
        _ terms: [String],
        configuration: FullTextIndexConfiguration
    ) -> [[String]] {
        let normalizer = FullTextTermNormalizer(
            tokenizer: configuration.tokenizer,
            ngramSize: configuration.ngramSize,
            minTermLength: configuration.minTermLength
        )
        return terms.map { term in
            uniqueTerms(normalizer.normalizedTerms(from: term))
        }
        .filter { !$0.isEmpty }
    }

    private func uniqueTerms(_ terms: [String]) -> [String] {
        var seen: Set<String> = []
        var result: [String] = []
        result.reserveCapacity(terms.count)
        for term in terms where !seen.contains(term) {
            seen.insert(term)
            result.append(term)
        }
        return result
    }

    private func bytesToInt64(_ bytes: ByteString) throws -> Int64 {
        try ByteConversion.bytesToInt64(bytes)
    }
}
