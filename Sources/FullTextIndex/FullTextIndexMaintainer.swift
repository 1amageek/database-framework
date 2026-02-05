// FullTextIndexMaintainer.swift
// FullTextIndexLayer - Full-text index maintainer
//
// Maintains full-text indexes using inverted index structure.

import Foundation
import Core
import DatabaseEngine
import FoundationDB

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
/// Value: Tuple(position1, position2, ...) or '' (no positions)
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
public struct FullTextIndexMaintainer<Item: Persistable>: IndexMaintainer {
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
    private let statsNKey: [UInt8]
    private let statsTotalLengthKey: [UInt8]

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
        transaction: any TransactionProtocol
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
                    transaction.clear(key: termKey)

                    // Decrement df for this term (BM25)
                    let dfKey = dfSubspace.pack(Tuple(term))
                    transaction.atomicOp(key: dfKey, param: ByteConversion.int64ToBytes(-1), mutationType: .add)
                }

                // Remove document metadata
                let docKey = docsSubspace.pack(oldId)
                transaction.clear(key: docKey)

                // Decrement BM25 corpus statistics
                transaction.atomicOp(key: statsNKey, param: ByteConversion.int64ToBytes(-1), mutationType: .add)
                transaction.atomicOp(key: statsTotalLengthKey, param: ByteConversion.int64ToBytes(-Int64(oldDocLength)), mutationType: .add)
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

                    if storePositions {
                        // Store positions for phrase search support
                        let positionElements: [any TupleElement] = positions.map { Int64($0) as any TupleElement }
                        let value = Tuple(positionElements).pack()
                        transaction.setValue(value, for: termKey)
                    } else {
                        // Store term frequency (tf) for BM25 scoring
                        // Without this, all terms would be treated as tf=1
                        let tfValue = Tuple(Int64(positions.count)).pack()
                        transaction.setValue(tfValue, for: termKey)
                    }

                    // Increment df for this term (BM25)
                    let dfKey = dfSubspace.pack(Tuple(term))
                    transaction.atomicOp(key: dfKey, param: ByteConversion.int64ToBytes(1), mutationType: .add)
                }

                // Store document metadata: (uniqueTermCount, docLength)
                let docKey = docsSubspace.pack(newId)
                let uniqueTermCount = Int64(termPositions.count)
                let docValue = Tuple(uniqueTermCount, Int64(newDocLength)).pack()
                transaction.setValue(docValue, for: docKey)

                // Increment BM25 corpus statistics
                transaction.atomicOp(key: statsNKey, param: ByteConversion.int64ToBytes(1), mutationType: .add)
                transaction.atomicOp(key: statsTotalLengthKey, param: ByteConversion.int64ToBytes(Int64(newDocLength)), mutationType: .add)
            } catch DataAccessError.nilValueCannotBeIndexed {
                // Sparse index: nil text is not indexed
            }
        }
    }

    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionProtocol
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

            if storePositions {
                // Store positions for phrase search support
                let positionElements: [any TupleElement] = positions.map { Int64($0) as any TupleElement }
                let value = Tuple(positionElements).pack()
                transaction.setValue(value, for: termKey)
            } else {
                // Store term frequency (tf) for BM25 scoring
                // Without this, all terms would be treated as tf=1
                let tfValue = Tuple(Int64(positions.count)).pack()
                transaction.setValue(tfValue, for: termKey)
            }

            // Increment df for this term (BM25)
            let dfKey = dfSubspace.pack(Tuple(term))
            transaction.atomicOp(key: dfKey, param: ByteConversion.int64ToBytes(1), mutationType: .add)
        }

        // Store document metadata: (uniqueTermCount, docLength)
        let docKey = docsSubspace.pack(id)
        let uniqueTermCount = Int64(termPositions.count)
        let docValue = Tuple(uniqueTermCount, Int64(docLength)).pack()
        transaction.setValue(docValue, for: docKey)

        // Increment BM25 corpus statistics
        transaction.atomicOp(key: statsNKey, param: ByteConversion.int64ToBytes(1), mutationType: .add)
        transaction.atomicOp(key: statsTotalLengthKey, param: ByteConversion.int64ToBytes(Int64(docLength)), mutationType: .add)
    }

    /// Compute expected index keys for this item
    ///
    /// **Sparse index behavior**:
    /// If the text field is nil, returns an empty array (no index entries expected).
    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [FDB.Bytes] {
        // Sparse index: if text field is nil, no index entries expected
        let text: String
        do {
            text = try extractText(from: item)
        } catch DataAccessError.nilValueCannotBeIndexed {
            // Sparse index: nil text is not indexed
            return []
        }

        let tokens = tokenize(text)

        var keys: [FDB.Bytes] = []
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
        transaction: any TransactionProtocol
    ) async throws -> [[any TupleElement]] {
        // Apply same normalization and truncation as during indexing
        // to ensure search terms match stored terms
        let normalizedTerm = truncateTerm(normalizeToken(term))
        let termSubspace = termsSubspace.subspace(normalizedTerm)
        let (begin, end) = termSubspace.range()

        var results: [[any TupleElement]] = []

        let sequence = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(begin),
            endSelector: .firstGreaterOrEqual(end),
            snapshot: true
        )

        for try await (key, _) in sequence {
            guard termSubspace.contains(key) else { break }

            // Skip corrupt entries
            guard let keyTuple = try? termSubspace.unpack(key),
                  let elements = try? Tuple.unpack(from: keyTuple.pack()) else {
                continue
            }
            results.append(elements)
        }

        return results
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
        transaction: any TransactionProtocol
    ) async throws -> [[any TupleElement]] {
        guard !terms.isEmpty else { return [] }

        var intersection: Set<String>? = nil
        var idToElements: [String: [any TupleElement]] = [:]

        for term in terms {
            let results = try await searchTerm(term, transaction: transaction)
            var currentSet: Set<String> = []

            for elements in results {
                // Use Tuple.pack() + Base64 for stable, type-safe key generation
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
        transaction: any TransactionProtocol
    ) async throws -> [[any TupleElement]] {
        guard !terms.isEmpty else { return [] }

        var idToElements: [String: [any TupleElement]] = [:]

        for term in terms {
            let results = try await searchTerm(term, transaction: transaction)

            for elements in results {
                // Use Tuple.pack() + Base64 for stable, type-safe key generation
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
        transaction: any TransactionProtocol
    ) async throws -> [[any TupleElement]] {
        guard storePositions else {
            throw FullTextIndexError.invalidQuery("Phrase search requires storePositions=true")
        }

        let phraseTokens = tokenize(phrase)
        guard !phraseTokens.isEmpty else { return [] }

        let terms = phraseTokens.map { truncateTerm($0.term) }

        // First find documents containing all terms
        let candidateDocs = try await searchTermsAND(terms, transaction: transaction)

        var results: [[any TupleElement]] = []

        // For each candidate, verify the phrase exists
        for docElements in candidateDocs {
            let docId = Tuple(docElements)

            // Build all term keys upfront using same subspace structure as indexing
            let termKeys: [(index: Int, key: FDB.Bytes)] = terms.enumerated().map { (index, term) in
                (index, termsSubspace.subspace(term).pack(docId))
            }

            // Fetch all term positions concurrently using TaskGroup
            let positionResults = try await withThrowingTaskGroup(
                of: (index: Int, positions: [Int]).self
            ) { group in
                for (index, key) in termKeys {
                    group.addTask {
                        if let value = try await transaction.getValue(for: key, snapshot: true) {
                            let positionTuple = try Tuple.unpack(from: value)
                            var positions: [Int] = []
                            for i in 0..<positionTuple.count {
                                if let pos = positionTuple[i] as? Int64 {
                                    positions.append(Int(pos))
                                } else if let pos = positionTuple[i] as? Int {
                                    positions.append(pos)
                                }
                            }
                            return (index, positions)
                        } else {
                            return (index, [])
                        }
                    }
                }

                // Collect results and sort by original index
                var collected: [(index: Int, positions: [Int])] = []
                for try await result in group {
                    collected.append(result)
                }
                return collected.sorted { $0.index < $1.index }
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
        let fieldValues = try DataAccess.evaluateIndexFields(
            from: item,
            keyPaths: index.keyPaths,
            expression: index.rootExpression
        )

        var texts: [String] = []
        for value in fieldValues {
            if let s = value as? String {
                texts.append(s)
            }
        }

        return texts.joined(separator: " ")
    }

    /// Tokenize text into terms with positions
    private func tokenize(_ text: String) -> [(term: String, position: Int)] {
        switch tokenizer {
        case .simple:
            return simpleTokenize(text)
        case .stem:
            return stemTokenize(text)
        case .ngram:
            return ngramTokenize(text)
        case .keyword:
            return keywordTokenize(text)
        }
    }

    /// Simple whitespace and punctuation tokenization
    private func simpleTokenize(_ text: String) -> [(term: String, position: Int)] {
        var tokens: [(String, Int)] = []
        var position = 0

        let words = text.lowercased().components(separatedBy: CharacterSet.alphanumerics.inverted)

        for word in words {
            let trimmed = word.trimmingCharacters(in: .whitespaces)
            if trimmed.count >= minTermLength {
                tokens.append((trimmed, position))
                position += 1
            }
        }

        return tokens
    }

    /// Stemming tokenization (simplified - just lowercases and removes common suffixes)
    private func stemTokenize(_ text: String) -> [(term: String, position: Int)] {
        var tokens: [(String, Int)] = []
        var position = 0

        let words = text.lowercased().components(separatedBy: CharacterSet.alphanumerics.inverted)

        for word in words {
            var stemmed = word.trimmingCharacters(in: .whitespaces)

            // Simple Porter stemmer rules (English)
            if stemmed.hasSuffix("ing") && stemmed.count > 5 {
                stemmed = String(stemmed.dropLast(3))
            } else if stemmed.hasSuffix("ed") && stemmed.count > 4 {
                stemmed = String(stemmed.dropLast(2))
            } else if stemmed.hasSuffix("s") && !stemmed.hasSuffix("ss") && stemmed.count > 3 {
                stemmed = String(stemmed.dropLast(1))
            }

            if stemmed.count >= minTermLength {
                tokens.append((stemmed, position))
                position += 1
            }
        }

        return tokens
    }

    /// N-gram tokenization
    private func ngramTokenize(_ text: String) -> [(term: String, position: Int)] {
        var tokens: [(String, Int)] = []
        var position = 0

        let lowered = text.lowercased()
        let characters = Array(lowered)

        for i in 0...(max(0, characters.count - ngramSize)) {
            let ngram = String(characters[i..<min(i + ngramSize, characters.count)])
            if ngram.count >= minTermLength && !ngram.trimmingCharacters(in: .whitespaces).isEmpty {
                tokens.append((ngram, position))
                position += 1
            }
        }

        return tokens
    }

    /// Keyword tokenization (entire value as single token)
    private func keywordTokenize(_ text: String) -> [(term: String, position: Int)] {
        let normalized = text.lowercased().trimmingCharacters(in: .whitespaces)
        if normalized.count >= minTermLength {
            return [(normalized, 0)]
        }
        return []
    }

    /// Normalize a single token for search
    private func normalizeToken(_ token: String) -> String {
        return token.lowercased().trimmingCharacters(in: .whitespaces)
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
        let data = Data(term.utf8)
        if data.count <= fullTextMaxTermBytes {
            return term
        }
        // Truncate to max bytes, ensuring valid UTF-8
        var truncatedData = data.prefix(fullTextMaxTermBytes)
        while !truncatedData.isEmpty {
            if let str = String(data: truncatedData, encoding: .utf8) {
                return str
            }
            truncatedData = truncatedData.dropLast()
        }
        return ""
    }

    /// Build and validate term key
    ///
    /// Key structure: [termsSubspace][term][id]
    /// Using subspace nesting ensures consistent key format for indexing and search.
    private func buildTermKey(term: String, id: Tuple) throws -> FDB.Bytes {
        let safeTerm = truncateTerm(term)
        let termSubspace = termsSubspace.subspace(safeTerm)
        let key = termSubspace.pack(id)
        try validateKeySize(key)
        return key
    }

    /// Convert TupleElements to a stable, type-safe key using Tuple.pack() + Base64
    ///
    /// This ensures consistent key generation regardless of element types.
    /// Using String(describing:) is unstable because different types may have
    /// the same string representation (e.g., Int64(123) vs Int(123)).
    private func elementsToStableKey(_ elements: [any TupleElement]) -> String {
        let packed = Tuple(elements).pack()
        return Data(packed).base64EncodedString()
    }

    // MARK: - BM25 Statistics

    /// Get BM25 corpus statistics
    ///
    /// - Parameter transaction: FDB transaction
    /// - Returns: BM25 statistics (N, totalLength, avgDL)
    public func getBM25Statistics(
        transaction: any TransactionProtocol
    ) async throws -> BM25Statistics {
        // Read N (total document count)
        let nValue = try await transaction.getValue(for: statsNKey, snapshot: true)
        let n: Int64 = nValue.map { ByteConversion.bytesToInt64($0) } ?? 0

        // Read totalLength
        let lengthValue = try await transaction.getValue(for: statsTotalLengthKey, snapshot: true)
        let totalLength: Int64 = lengthValue.map { ByteConversion.bytesToInt64($0) } ?? 0

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
        transaction: any TransactionProtocol
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
        transaction: any TransactionProtocol
    ) async throws -> Int64 {
        let dfKey = dfSubspace.pack(Tuple(normalizedTerm))
        let value = try await transaction.getValue(for: dfKey, snapshot: true)
        return value.map { ByteConversion.bytesToInt64($0) } ?? 0
    }

    /// Get document metadata (term count and document length)
    ///
    /// - Parameters:
    ///   - id: Document ID
    ///   - transaction: FDB transaction
    /// - Returns: Tuple of (uniqueTermCount, docLength), or nil if not found
    public func getDocumentMetadata(
        id: Tuple,
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
            // Legacy format: only termCount
            if let termCount = tuple[0] as? Int64 {
                return (uniqueTermCount: termCount, docLength: 0)
            }
            return nil
        }
        return (uniqueTermCount: termCount, docLength: docLength)
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
        transaction: any TransactionProtocol,
        limit: Int? = nil
    ) async throws -> [(id: Tuple, score: Double)] {
        guard !terms.isEmpty else { return [] }

        // Get corpus statistics
        let stats = try await getBM25Statistics(transaction: transaction)
        guard stats.totalDocuments > 0 else { return [] }

        let scorer = BM25Scorer(params: bm25Params, statistics: stats)

        // Normalize search terms using the same tokenization pipeline as indexing
        // This ensures stemming, n-gram, or other transformations are applied consistently
        let normalizedTerms: [String] = terms.flatMap { term in
            tokenize(term).map { truncateTerm($0.term) }
        }

        // Get document frequencies for all terms (already normalized, use internal helper)
        var documentFrequencies: [String: Int64] = [:]
        for term in normalizedTerms {
            documentFrequencies[term] = try await getDocumentFrequencyForNormalizedTerm(term, transaction: transaction)
        }

        // Find matching documents
        let matchingDocs: [[any TupleElement]]
        switch matchMode {
        case .all:
            matchingDocs = try await searchTermsAND(terms, transaction: transaction)
        case .any:
            matchingDocs = try await searchTermsOR(terms, transaction: transaction)
        case .phrase:
            matchingDocs = try await searchPhrase(terms.joined(separator: " "), transaction: transaction)
        }

        // Calculate BM25 scores for each document
        var scoredResults: [(id: Tuple, score: Double)] = []

        for docElements in matchingDocs {
            let docId = Tuple(docElements)

            // Get document metadata
            guard let metadata = try await getDocumentMetadata(id: docId, transaction: transaction) else {
                continue
            }

            // Get term frequencies in this document
            var termFrequencies: [String: Int] = [:]
            for term in normalizedTerms {
                let termSubspace = termsSubspace.subspace(term)
                let termKey = termSubspace.pack(docId)
                if let value = try await transaction.getValue(for: termKey, snapshot: true) {
                    if storePositions {
                        // Count positions as term frequency
                        let positionTuple = try Tuple.unpack(from: value)
                        termFrequencies[term] = positionTuple.count
                    } else {
                        // Read stored term frequency
                        let tfTuple = try Tuple.unpack(from: value)
                        if let tf = tfTuple.first as? Int64 {
                            termFrequencies[term] = Int(tf)
                        } else {
                            // Fallback for legacy data without tf
                            termFrequencies[term] = 1
                        }
                    }
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
