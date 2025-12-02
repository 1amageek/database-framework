// FullTextIndexMaintainer.swift
// FullTextIndexLayer - Full-text index maintainer
//
// Maintains full-text indexes using inverted index structure.

import Foundation
import Core
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
/// // Document metadata (for ranking)
/// Key: [indexSubspace]["docs"][primaryKey]
/// Value: Tuple(termCount)
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
    }

    public func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionProtocol
    ) async throws {
        // Remove old index entries
        if let oldItem = oldItem {
            let oldId = try DataAccess.extractId(from: oldItem, using: idExpression)
            let oldText = try extractText(from: oldItem)
            let oldTokens = tokenize(oldText)

            // Group by truncated term to match how keys were stored
            var seenTerms: Set<String> = []
            for token in oldTokens {
                let safeTerm = truncateTerm(token.term)
                if !seenTerms.contains(safeTerm) {
                    let termKey = try buildTermKey(term: token.term, id: oldId)
                    transaction.clear(key: termKey)
                    seenTerms.insert(safeTerm)
                }
            }

            // Remove document metadata
            let docKey = docsSubspace.pack(oldId)
            transaction.clear(key: docKey)
        }

        // Add new index entries
        if let newItem = newItem {
            let newId = try DataAccess.extractId(from: newItem, using: idExpression)
            let newText = try extractText(from: newItem)
            let newTokens = tokenize(newText)

            // Group tokens by term to collect positions
            var termPositions: [String: [Int]] = [:]
            for token in newTokens {
                let safeTerm = truncateTerm(token.term)
                termPositions[safeTerm, default: []].append(token.position)
            }

            for (term, positions) in termPositions {
                let termKey = try buildTermKey(term: term, id: newId)

                if storePositions {
                    let positionElements: [any TupleElement] = positions.map { Int64($0) as any TupleElement }
                    let value = Tuple(positionElements).pack()
                    transaction.setValue(value, for: termKey)
                } else {
                    transaction.setValue([], for: termKey)
                }
            }

            // Store document metadata
            let docKey = docsSubspace.pack(newId)
            let termCount = Int64(termPositions.count)
            let docValue = Tuple(termCount).pack()
            transaction.setValue(docValue, for: docKey)
        }
    }

    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        let text = try extractText(from: item)
        let tokens = tokenize(text)

        // Group tokens by term to collect positions (using truncated terms)
        var termPositions: [String: [Int]] = [:]
        for token in tokens {
            let safeTerm = truncateTerm(token.term)
            termPositions[safeTerm, default: []].append(token.position)
        }

        for (term, positions) in termPositions {
            let termKey = try buildTermKey(term: term, id: id)

            if storePositions {
                let positionElements: [any TupleElement] = positions.map { Int64($0) as any TupleElement }
                let value = Tuple(positionElements).pack()
                transaction.setValue(value, for: termKey)
            } else {
                transaction.setValue([], for: termKey)
            }
        }

        // Store document metadata
        let docKey = docsSubspace.pack(id)
        let termCount = Int64(termPositions.count)
        let docValue = Tuple(termCount).pack()
        transaction.setValue(docValue, for: docKey)
    }

    /// Compute expected index keys for this item
    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [FDB.Bytes] {
        let text = try extractText(from: item)
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

            // Build all term keys upfront
            let termKeys: [(index: Int, key: FDB.Bytes)] = terms.enumerated().map { (index, term) in
                (index, termsSubspace.pack(Tuple(term, docId)))
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
    private func buildTermKey(term: String, id: Tuple) throws -> FDB.Bytes {
        let safeTerm = truncateTerm(term)
        let key = termsSubspace.pack(Tuple(safeTerm, id))
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
}
