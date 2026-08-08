// FullTextIndexMaintainer.swift
// FullTextIndexLayer - Full-text index maintainer
//
// Maintains full-text indexes using inverted index structure.

import DatabaseTypes
import DatabaseKit
import DatabaseEngine
import StorageKit

private struct FullTextPositionResult: Sendable {
    let termIndex: Int
    let positions: [Int]
}

private enum FullTextPositionOutcome: Sendable {
    case success(FullTextPositionResult)
    case failure(any Error)
}

private enum FullTextPositionBatchOutcome: Sendable {
    case success([FullTextPositionResult])
    case failure(any Error)
}

// MARK: - FullText Constants

/// Maximum term length in bytes to prevent key size overflow.
/// FDB key limit is 10KB; we reserve space for subspace prefix and ID.
public let fullTextMaxTermBytes: Int = 8000

/// Maintainer for full-text search indexes
///
/// **Functionality**:
/// - Tokenize text fields
/// - Build and maintain inverted index
/// - Support term and phrase queries
///
/// **Index Structure**:
/// ```
/// // Inverted index (term → documents)
/// Key: [indexSubspace]["terms"][term][primaryKey]
/// Value: Tuple(termFrequency, position1, position2, ...)
/// Positions are omitted when phrase search is disabled.
///
/// // Document metadata (for BM25 ranking)
/// Key: [indexSubspace]["docs"][primaryKey]
/// Value: Tuple(uniqueTermCount, docLength)
///
/// // BM25 corpus statistics
/// Key: [indexSubspace]["stats"]["N"]
/// Value: Int64 (total document count)
///
/// Key: [indexSubspace]["stats"]["totalLength"]
/// Value: Int64 (sum of all document lengths)
///
/// // Document frequency per term (for IDF)
/// Key: [indexSubspace]["df"][term]
/// Value: Int64 (number of documents containing term)
/// ```
///
/// **Usage**:
/// ```swift
/// let maintainer = FullTextIndexMaintainer<Article>(
///     index: titleIndex,
///     kind: FullTextIndexKind(tokenizer: .simple, storePositions: true),
///     subspace: fullTextSubspace,
///     idExpression: FieldKeyExpression(fieldName: "id")
/// )
/// ```
public struct FullTextIndexMaintainer<Item: PersistedEntityValue>: IndexMaintainer {
    public let index: Index
    public let subspace: Subspace
    public let idExpression: KeyExpression

    private let tokenizer: TokenizationStrategy
    private let storePositions: Bool
    private let ngramSize: Int
    private let minTermLength: Int

    // Subspaces
    private let termsSubspace: Subspace
    private let docsSubspace: Subspace
    private let statsSubspace: Subspace
    private let dfSubspace: Subspace

    // BM25 statistics keys
    private let statsNKey: ByteString
    private let statsTotalLengthKey: ByteString

    private var termNormalizer: FullTextTermNormalizer {
        FullTextTermNormalizer(
            tokenizer: tokenizer,
            ngramSize: ngramSize,
            minTermLength: minTermLength
        )
    }

    public init(
        index: Index,
        tokenizer: TokenizationStrategy,
        storePositions: Bool,
        ngramSize: Int,
        minTermLength: Int,
        subspace: Subspace,
        idExpression: KeyExpression
    ) {
        self.index = index
        self.subspace = subspace
        self.idExpression = idExpression
        self.tokenizer = tokenizer
        self.storePositions = storePositions
        self.ngramSize = ngramSize
        self.minTermLength = minTermLength
        self.termsSubspace = subspace.subspace("terms")
        self.docsSubspace = subspace.subspace("docs")
        self.statsSubspace = subspace.subspace("stats")
        self.dfSubspace = subspace.subspace("df")
        self.statsNKey = statsSubspace.pack(Tuple("N"))
        self.statsTotalLengthKey = statsSubspace.pack(Tuple("totalLength"))
    }

    public func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionAccess
    ) async throws {
        // Remove old index entries and update BM25 statistics
        // Sparse index: if text field was nil, the document was never indexed
        if let oldItem = oldItem {
            do {
                let oldId = try DataAccess.extractId(from: oldItem, using: idExpression)
                let oldText = try extractText(from: oldItem)
                let oldTokens = tokenize(oldText)
                let oldDocLength = oldTokens.count

                // Group by truncated term to match how keys were stored
                var oldTermPositions: [String: [Int]] = [:]
                for token in oldTokens {
                    let safeTerm = truncateTerm(token.term)
                    oldTermPositions[safeTerm, default: []].append(token.position)
                }

                // Remove term entries
                for term in oldTermPositions.keys {
                    let termKey = try buildTermKey(term: term, id: oldId)
                    try transaction.clear(key: termKey)

                    // Decrement df for this term (BM25)
                    let dfKey = dfSubspace.pack(Tuple(term))
                    try transaction.atomicOp(key: dfKey, param: ByteConversion.int64ToBytes(-1), mutationType: .add)
                }

                // Remove document metadata
                let docKey = docsSubspace.pack(oldId)
                try transaction.clear(key: docKey)

                // Decrement BM25 corpus statistics
                try transaction.atomicOp(key: statsNKey, param: ByteConversion.int64ToBytes(-1), mutationType: .add)
                try transaction.atomicOp(key: statsTotalLengthKey, param: ByteConversion.int64ToBytes(-Int64(oldDocLength)), mutationType: .add)
            } catch DataAccessError.nilValueCannotBeIndexed {
                // Sparse index: nil text was not indexed, nothing to remove
            }
        }

        // Add new index entries and update BM25 statistics
        // Sparse index: if text field is nil, skip indexing
        if let newItem = newItem {
            do {
                let newId = try DataAccess.extractId(from: newItem, using: idExpression)
                let newText = try extractText(from: newItem)
                let newTokens = tokenize(newText)
                let newDocLength = newTokens.count

                // Group tokens by term to collect positions
                var termPositions: [String: [Int]] = [:]
                for token in newTokens {
                    let safeTerm = truncateTerm(token.term)
                    termPositions[safeTerm, default: []].append(token.position)
                }

                // Add term entries
                for (term, positions) in termPositions {
                    let termKey = try buildTermKey(term: term, id: newId)
                    try transaction.setValue(
                        postingValue(positions: positions),
                        for: termKey
                    )

                    // Increment df for this term (BM25)
                    let dfKey = dfSubspace.pack(Tuple(term))
                    try transaction.atomicOp(key: dfKey, param: ByteConversion.int64ToBytes(1), mutationType: .add)
                }

                // Store document metadata: (uniqueTermCount, docLength)
                let docKey = docsSubspace.pack(newId)
                let uniqueTermCount = Int64(termPositions.count)
                let docValue = Tuple(uniqueTermCount, Int64(newDocLength)).pack()
                try transaction.setValue(docValue, for: docKey)

                // Increment BM25 corpus statistics
                try transaction.atomicOp(key: statsNKey, param: ByteConversion.int64ToBytes(1), mutationType: .add)
                try transaction.atomicOp(key: statsTotalLengthKey, param: ByteConversion.int64ToBytes(Int64(newDocLength)), mutationType: .add)
            } catch DataAccessError.nilValueCannotBeIndexed {
                // Sparse index: nil text is not indexed
            }
        }
    }

    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionAccess
    ) async throws {
        // Sparse index: if text field is nil, skip indexing
        let text: String
        do {
            text = try extractText(from: item)
        } catch DataAccessError.nilValueCannotBeIndexed {
            // Sparse index: nil text is not indexed
            return
        }

        let tokens = tokenize(text)
        let docLength = tokens.count

        // Group tokens by term to collect positions (using truncated terms)
        var termPositions: [String: [Int]] = [:]
        for token in tokens {
            let safeTerm = truncateTerm(token.term)
            termPositions[safeTerm, default: []].append(token.position)
        }

        for (term, positions) in termPositions {
            let termKey = try buildTermKey(term: term, id: id)
            try transaction.setValue(
                postingValue(positions: positions),
                for: termKey
            )

            // Increment df for this term (BM25)
            let dfKey = dfSubspace.pack(Tuple(term))
            try transaction.atomicOp(key: dfKey, param: ByteConversion.int64ToBytes(1), mutationType: .add)
        }

        // Store document metadata: (uniqueTermCount, docLength)
        let docKey = docsSubspace.pack(id)
        let uniqueTermCount = Int64(termPositions.count)
        let docValue = Tuple(uniqueTermCount, Int64(docLength)).pack()
        try transaction.setValue(docValue, for: docKey)

        // Increment BM25 corpus statistics
        try transaction.atomicOp(key: statsNKey, param: ByteConversion.int64ToBytes(1), mutationType: .add)
        try transaction.atomicOp(key: statsTotalLengthKey, param: ByteConversion.int64ToBytes(Int64(docLength)), mutationType: .add)
    }

    /// Compute expected index keys for this item
    ///
    /// **Sparse index behavior**:
    /// If the text field is nil, returns an empty array (no index entries expected).
    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [ByteString] {
        // Sparse index: if text field is nil, no index entries expected
        let text: String
        do {
            text = try extractText(from: item)
        } catch DataAccessError.nilValueCannotBeIndexed {
            // Sparse index: nil text is not indexed
            return []
        }

        let tokens = tokenize(text)

        var keys: [ByteString] = []
        var seenTerms: Set<String> = []

        for token in tokens {
            let safeTerm = truncateTerm(token.term)
            if !seenTerms.contains(safeTerm) {
                let termKey = try buildTermKey(term: token.term, id: id)
                keys.append(termKey)
                seenTerms.insert(safeTerm)
            }
        }

        // Add document metadata key
        let docKey = docsSubspace.pack(id)
        keys.append(docKey)

        return keys
    }

    // MARK: - Search Methods

    /// Search for documents containing a term
    ///
    /// - Parameters:
    ///   - term: Search term
    ///   - transaction: FDB transaction
    /// - Returns: Array of primary keys
    public func searchTerm(
        _ term: String,
        transaction: any TransactionAccess
    ) async throws -> [[any TupleElement]] {
        let normalizedTerms = normalizeQueryTerms([term])
        return try await searchNormalizedTermsAND(normalizedTerms, transaction: transaction)
    }

    /// Search for documents containing all terms (AND query)
    ///
    /// **Optimization**: Uses incremental intersection with early termination.
    /// If the intersection becomes empty during processing, we stop immediately
    /// without loading results for remaining terms.
    ///
    /// - Parameters:
    ///   - terms: Search terms
    ///   - transaction: FDB transaction
    /// - Returns: Array of primary keys that contain all terms
    public func searchTermsAND(
        _ terms: [String],
        transaction: any TransactionAccess
    ) async throws -> [[any TupleElement]] {
        let normalizedTerms = normalizeQueryTerms(terms)
        return try await searchNormalizedTermsAND(normalizedTerms, transaction: transaction)
    }

    /// Search for already-normalized terms using AND semantics.
    private func searchNormalizedTermsAND(
        _ terms: [String],
        transaction: any TransactionAccess
    ) async throws -> [[any TupleElement]] {
        guard !terms.isEmpty else { return [] }

        var intersection: Set<ByteString>? = nil
        var idToElements: [ByteString: [any TupleElement]] = [:]

        for term in terms {
            let results = try await searchNormalizedTerm(term, transaction: transaction)
            var currentSet: Set<ByteString> = []

            for elements in results {
                let idKey = elementsToStableKey(elements)
                currentSet.insert(idKey)

                // Only store elements that might be in final result
                // For first term, store all; for subsequent terms, only store if in intersection
                if intersection == nil || intersection!.contains(idKey) {
                    idToElements[idKey] = elements
                }
            }

            // Update intersection incrementally
            if let prev = intersection {
                intersection = prev.intersection(currentSet)

                // Early termination: if intersection is empty, no need to check remaining terms
                if intersection!.isEmpty {
                    return []
                }
            } else {
                intersection = currentSet
            }
        }

        // Return matching elements
        guard let finalIntersection = intersection else { return [] }
        return finalIntersection.compactMap { idToElements[$0] }
    }

    /// Search for documents containing any term (OR query)
    ///
    /// - Parameters:
    ///   - terms: Search terms
    ///   - transaction: FDB transaction
    /// - Returns: Array of primary keys that contain any of the terms
    public func searchTermsOR(
        _ terms: [String],
        transaction: any TransactionAccess
    ) async throws -> [[any TupleElement]] {
        let termGroups = normalizeQueryTermGroups(terms)
        guard !termGroups.isEmpty else { return [] }

        var idToElements: [ByteString: [any TupleElement]] = [:]

        for normalizedTerms in termGroups {
            let results = try await searchNormalizedTermsAND(normalizedTerms, transaction: transaction)

            for elements in results {
                let idKey = elementsToStableKey(elements)
                idToElements[idKey] = elements
            }
        }

        return Array(idToElements.values)
    }

    /// Search for a phrase (exact sequence of terms)
    ///
    /// **Optimization**: Uses concurrent fetching to reduce O(t) sequential reads
    /// to O(1) parallel batch. All term positions for a document are fetched
    /// concurrently using TaskGroup.
    ///
    /// - Parameters:
    ///   - phrase: Search phrase
    ///   - transaction: FDB transaction
    /// - Returns: Array of primary keys that contain the phrase
    public func searchPhrase(
        _ phrase: String,
        transaction: any TransactionAccess
    ) async throws -> [[any TupleElement]] {
        guard storePositions else {
            throw FullTextIndexError.invalidQuery("Phrase search requires storePositions=true")
        }

        let phraseTokens = tokenize(phrase)
        guard !phraseTokens.isEmpty else { return [] }

        let terms = phraseTokens.map { truncateTerm($0.term) }

        // First find documents containing all terms
        let candidateDocs = try await searchNormalizedTermsAND(terms, transaction: transaction)

        var results: [[any TupleElement]] = []

        // For each candidate, verify the phrase exists
        for docElements in candidateDocs {
            let docId = Tuple(docElements)

            // Build all term keys upfront using same subspace structure as indexing
            let termKeys: [(index: Int, key: ByteString)] = terms.enumerated().map { (index, term) in
                (index, termsSubspace.subspace(term).pack(docId))
            }

            // Fetch all term positions concurrently using TaskGroup
            let batchOutcome = await withTaskGroup(
                of: FullTextPositionOutcome.self
            ) { group in
                for (index, key) in termKeys {
                    group.addTask {
                        do {
                            if let value = try await transaction.getValue(
                                for: key,
                                snapshot: true
                            ) {
                                let posting = try FullTextStorageDecoder.posting(
                                    from: value,
                                    positionsStored: true,
                                    term: terms[index]
                                )
                                return .success(FullTextPositionResult(
                                    termIndex: index,
                                    positions: posting.positions
                                ))
                            }
                            return .success(FullTextPositionResult(
                                termIndex: index,
                                positions: []
                            ))
                        } catch {
                            return .failure(error)
                        }
                    }
                }

                // Collect results and sort by original index
                var collected: [FullTextPositionResult] = []
                for await outcome in group {
                    switch outcome {
                    case .success(let result):
                        collected.append(result)
                    case .failure(let error):
                        group.cancelAll()
                        return FullTextPositionBatchOutcome.failure(error)
                    }
                }
                return .success(collected.sorted {
                    $0.termIndex < $1.termIndex
                })
            }

            let positionResults: [FullTextPositionResult]
            switch batchOutcome {
            case .success(let results):
                positionResults = results
            case .failure(let error):
                throw error
            }

            // Extract position arrays in order
            let termPositionArrays = positionResults.map { $0.positions }

            // Check if positions form a consecutive sequence
            if verifyPhrasePositions(termPositionArrays) {
                results.append(docElements)
            }
        }

        return results
    }

    // MARK: - Private Methods

    /// Extract text from item by evaluating the index expression
    ///
    /// **KeyPath Optimization**:
    /// When `index.keyPaths` is available, uses direct KeyPath subscript access
    /// which is more efficient than string-based `@dynamicMemberLookup`.
    private func extractText(from item: Item) throws -> String {
        // Use optimized DataAccess method - KeyPath when available, falls back to KeyExpression
        let fieldValues = try DataAccess.evaluate(
            item: item,
            expression: index.rootExpression
        )

        var texts: [String] = []
        for value in fieldValues {
            texts.append(try TupleDecoder.decodeString(value))
        }

        return texts.joined(separator: " ")
    }

    /// Tokenize text into terms with positions
    private func tokenize(_ text: String) -> [(term: String, position: Int)] {
        termNormalizer.tokenize(text)
    }

    /// Search for an exact already-normalized term.
    private func searchNormalizedTerm(
        _ term: String,
        transaction: any TransactionAccess
    ) async throws -> [[any TupleElement]] {
        let termSubspace = termsSubspace.subspace(term)
        let (begin, end) = termSubspace.range()

        var results: [[any TupleElement]] = []

        let sequence = try await TransactionRangeCollection.collect(using: transaction,
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            limit: 0,
            reverse: false,
            snapshot: true,
            streamingMode: .wantAll
        )

        for (key, _) in sequence {
            guard termSubspace.contains(key) else { break }

            let keyTuple = try termSubspace.unpack(key)
            let elements = try keyTuple.elements()
            results.append(elements)
        }

        return results
    }

    private func normalizeQueryTerms(_ terms: [String]) -> [String] {
        uniqueTerms(termGroups: normalizeQueryTermGroups(terms).flatMap { $0 })
    }

    private func normalizeQueryTermGroups(_ terms: [String]) -> [[String]] {
        terms.map { term in
            uniqueTerms(termGroups: termNormalizer.normalizedTerms(from: term))
        }
        .filter { !$0.isEmpty }
    }

    private func uniqueTerms(termGroups terms: [String]) -> [String] {
        var seen: Set<String> = []
        var result: [String] = []
        result.reserveCapacity(terms.count)

        for term in terms where !seen.contains(term) {
            seen.insert(term)
            result.append(term)
        }

        return result
    }

    /// Verify that term positions form a consecutive phrase
    private func verifyPhrasePositions(_ positionArrays: [[Int]]) -> Bool {
        guard !positionArrays.isEmpty else { return false }
        guard let firstPositions = positionArrays.first, !firstPositions.isEmpty else { return false }

        // For each starting position of the first term
        for startPos in firstPositions {
            var found = true

            // Check if subsequent terms appear at consecutive positions
            for (i, positions) in positionArrays.enumerated() {
                let expectedPos = startPos + i
                if !positions.contains(expectedPos) {
                    found = false
                    break
                }
            }

            if found {
                return true
            }
        }

        return false
    }

    // MARK: - Key Size Validation

    /// Truncate term to fit within key size limits
    private func truncateTerm(_ term: String) -> String {
        termNormalizer.truncateTerm(term)
    }

    /// Build and validate term key
    ///
    /// Key structure: [termsSubspace][term][id]
    /// Using subspace nesting ensures consistent key format for indexing and search.
    private func buildTermKey(term: String, id: Tuple) throws -> ByteString {
        let safeTerm = truncateTerm(term)
        let termSubspace = termsSubspace.subspace(safeTerm)
        let key = termSubspace.pack(id)
        try validateKeySize(key)
        return key
    }

    /// Encode the canonical posting payload as `(termFrequency, positions...)`.
    ///
    /// Positions are omitted when the index does not support phrase search.
    /// Tuple packing owns the final storage buffer; no intermediate packed
    /// buffers are materialized.
    private func postingValue(positions: [Int]) -> ByteString {
        var elements: [any TupleElement] = []
        elements.reserveCapacity(storePositions ? positions.count + 1 : 1)
        elements.append(Int64(positions.count))
        if storePositions {
            for position in positions {
                elements.append(Int64(position))
            }
        }
        return Tuple(elements).pack()
    }

    /// Preserve tuple type identity while using the packed bytes directly as a
    /// hash key. `ByteString` retains its immutable storage without materializing a
    /// `Data` or Base64 representation.
    private func elementsToStableKey(
        _ elements: [any TupleElement]
    ) -> ByteString {
        Tuple(elements).pack()
    }

    // MARK: - BM25 Statistics

    /// Get BM25 corpus statistics
    ///
    /// - Parameter transaction: FDB transaction
    /// - Returns: BM25 statistics (N, totalLength, avgDL)
    public func getBM25Statistics(
        transaction: any TransactionAccess
    ) async throws -> BM25Statistics {
        // Read N (total document count)
        let nValue = try await transaction.getValue(for: statsNKey, snapshot: true)
        let n: Int64
        if let nValue {
            n = try ByteConversion.bytesToInt64(nValue)
        } else {
            n = 0
        }

        // Read totalLength
        let lengthValue = try await transaction.getValue(for: statsTotalLengthKey, snapshot: true)
        let totalLength: Int64
        if let lengthValue {
            totalLength = try ByteConversion.bytesToInt64(lengthValue)
        } else {
            totalLength = 0
        }

        return BM25Statistics(totalDocuments: n, totalLength: totalLength)
    }

    /// Get document frequency for a term
    ///
    /// Uses the same tokenization pipeline as indexing to ensure consistency.
    /// For example, if stemming is enabled, "running" will be stemmed to "run"
    /// before looking up the document frequency.
    ///
    /// - Parameters:
    ///   - term: The term (raw, will be tokenized)
    ///   - transaction: FDB transaction
    /// - Returns: Number of documents containing the term
    public func getDocumentFrequency(
        term: String,
        transaction: any TransactionAccess
    ) async throws -> Int64 {
        // Tokenize the term using the same pipeline as indexing
        let tokens = tokenize(term)
        guard let firstToken = tokens.first else { return 0 }
        let safeTerm = truncateTerm(firstToken.term)
        return try await getDocumentFrequencyForNormalizedTerm(safeTerm, transaction: transaction)
    }

    /// Get document frequency for an already-normalized term
    ///
    /// Internal helper used when terms have already been processed through the tokenization pipeline.
    ///
    /// - Parameters:
    ///   - normalizedTerm: The normalized/tokenized term
    ///   - transaction: FDB transaction
    /// - Returns: Number of documents containing the term
    private func getDocumentFrequencyForNormalizedTerm(
        _ normalizedTerm: String,
        transaction: any TransactionAccess
    ) async throws -> Int64 {
        let dfKey = dfSubspace.pack(Tuple(normalizedTerm))
        let value = try await transaction.getValue(for: dfKey, snapshot: true)
        guard let value else { return 0 }
        return try ByteConversion.bytesToInt64(value)
    }

    /// Get document metadata (term count and document length)
    ///
    /// - Parameters:
    ///   - id: Document ID
    ///   - transaction: FDB transaction
    /// - Returns: Tuple of (uniqueTermCount, docLength), or nil if not found
    public func getDocumentMetadata(
        id: Tuple,
        transaction: any TransactionAccess
    ) async throws -> (uniqueTermCount: Int64, docLength: Int64)? {
        let docKey = docsSubspace.pack(id)
        guard let value = try await transaction.getValue(for: docKey, snapshot: true) else {
            return nil
        }
        return try FullTextStorageDecoder.documentMetadata(from: value)
    }

    // MARK: - BM25 Scored Search

    /// Search for documents with BM25 scores
    ///
    /// Internal method used by FullTextQueryBuilder.executeWithScores().
    /// External callers should use the query builder API instead.
    ///
    /// - Parameters:
    ///   - terms: Search terms
    ///   - matchMode: AND or OR mode
    ///   - bm25Params: BM25 parameters
    ///   - transaction: FDB transaction
    ///   - limit: Maximum results (nil for unlimited)
    /// - Returns: Array of (id, score) sorted by score descending
    internal func searchWithScores(
        terms: [String],
        matchMode: TextMatchMode = .all,
        bm25Params: BM25Parameters = .default,
        transaction: any TransactionAccess,
        limit: Int? = nil
    ) async throws -> [(id: Tuple, score: Double)] {
        guard !terms.isEmpty else { return [] }

        // Normalize search terms using the same tokenization pipeline as indexing
        // This ensures stemming, n-gram, or other transformations are applied consistently
        let normalizedTerms = normalizeQueryTerms(terms)

        // Get document frequencies for all terms (already normalized, use internal helper)
        var documentFrequencies: [String: Int64] = [:]
        for term in normalizedTerms {
            documentFrequencies[term] = try await getDocumentFrequencyForNormalizedTerm(term, transaction: transaction)
        }

        // Find matching documents
        let matchingDocs: [[any TupleElement]]
        switch matchMode {
        case .all:
            matchingDocs = try await searchNormalizedTermsAND(normalizedTerms, transaction: transaction)
        case .any:
            let groups = normalizeQueryTermGroups(terms)
            var idToElements: [ByteString: [any TupleElement]] = [:]
            for group in groups {
                let matches = try await searchNormalizedTermsAND(group, transaction: transaction)
                for elements in matches {
                    idToElements[elementsToStableKey(elements)] = elements
                }
            }
            matchingDocs = Array(idToElements.values)
        case .phrase:
            matchingDocs = try await searchPhrase(terms.joined(separator: " "), transaction: transaction)
        }

        guard !matchingDocs.isEmpty else {
            return []
        }

        // Postings without their corpus counters are persisted corruption, not
        // an empty search result.
        let stats = try await getBM25Statistics(transaction: transaction)
        guard stats.totalDocuments > 0, stats.totalLength > 0 else {
            throw FullTextStorageError.corruptedCorpusStatistics
        }
        let scorer = BM25Scorer(params: bm25Params, statistics: stats)

        // Calculate BM25 scores for each document
        var scoredResults: [(id: Tuple, score: Double)] = []
        scoredResults.reserveCapacity(matchingDocs.count)

        for docElements in matchingDocs {
            let docId = Tuple(docElements)

            // Get document metadata
            guard let metadata = try await getDocumentMetadata(id: docId, transaction: transaction) else {
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
                        positionsStored: storePositions,
                        term: term
                    )
                    termFrequencies[term] = posting.termFrequency
                }
            }

            // Calculate BM25 score
            let score = scorer.score(
                termFrequencies: termFrequencies,
                documentFrequencies: documentFrequencies,
                docLength: Int(metadata.docLength)
            )

            scoredResults.append((id: docId, score: score))
        }

        // Sort by score descending
        scoredResults.sort { $0.score > $1.score }

        // Apply limit
        if let limit = limit {
            return Array(scoredResults.prefix(limit))
        }

        return scoredResults
    }
}
