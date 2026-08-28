// FullTextIndexMaintainer.swift
// FullTextIndex - Full-text index maintainer
//
// Maintains full-text indexes using inverted index structure.

import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit

// MARK: - FullText Constants

/// Maximum term length in bytes to prevent key size overflow.
/// FDB key limit is 10KB; we reserve space for subspace prefix and ID.
public let fullTextMaxTermBytes: Int = 8000

/// Maintainer for full-text search indexes
///
/// **Functionality**:
/// - Tokenize text fields
/// - Build and maintain inverted index
/// - Persist postings and BM25 corpus statistics
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
///     index: resolvedIndex,
///     tokenizer: .simple,
///     storePositions: true,
///     ngramSize: 3,
///     minTermLength: 2,
///     subspace: indexSubspace,
///     idExpression: FieldKeyExpression(fieldName: "id")
/// )
/// try await maintainer.updateIndex(
///     oldItem: nil,
///     newItem: article,
///     transaction: transaction
/// )
/// ```
public struct FullTextIndexMaintainer<Item: PersistedEntityValue>: IndexMaintainer {
    public let index: ResolvedIndex
    public let subspace: Subspace
    public let idExpression: KeyExpression

    private let tokenizer: TokenizationStrategy
    private let storePositions: Bool
    private let ngramSize: Int
    private let minTermLength: Int

    // Subspaces
    private let termsSubspace: Subspace
    private let docsSubspace: Subspace
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
        index: ResolvedIndex,
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
        self.termsSubspace = FullTextStorageLayout.terms(in: subspace)
        self.docsSubspace = FullTextStorageLayout.documents(in: subspace)
        self.dfSubspace = FullTextStorageLayout.documentFrequencies(
            in: subspace
        )
        self.statsNKey = FullTextStorageLayout.documentCountKey(in: subspace)
        self.statsTotalLengthKey = FullTextStorageLayout
            .totalDocumentLengthKey(in: subspace)
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

}
