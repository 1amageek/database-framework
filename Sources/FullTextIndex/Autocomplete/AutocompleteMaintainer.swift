// AutocompleteMaintainer.swift
// FullTextIndex - Prefix Trie-based Autocomplete
//
// Reference: Trie data structure for prefix matching
// Knuth, "The Art of Computer Programming", Vol. 3

import DatabaseEngine
import DatabaseKit
import StorageKit

/// Maintainer for autocomplete/typeahead indexes using prefix-based storage
///
/// **Purpose**: Enables fast prefix-based suggestions for search-as-you-type UX.
///
/// **Storage Layout**:
/// ```
/// [subspace]/suggestions/[field]/[prefix]/[term] = Int64 (frequency/score)
/// [subspace]/terms/[field]/[term] = Int64 (total count across documents)
/// ```
///
/// **Algorithm**:
/// Instead of an in-memory trie, we use FDB's ordered key storage:
/// - Store each term with all its prefixes
/// - Use range scan on prefix to get suggestions
/// - Rank by frequency/score
///
/// **Usage**:
/// ```swift
/// let maintainer = AutocompleteMaintainer<Product>(
///     subspace: indexSubspace,
///     idExpression: FieldKeyExpression(fieldName: "id"),
///     autocompleteFields: ["name", "brand"],
///     minPrefixLength: 2,
///     maxSuggestions: 10
/// )
///
/// // Update when document changes
/// try await maintainer.updateAutocomplete(
///     oldItem: oldProduct,
///     newItem: newProduct,
///     transaction: transaction
/// )
///
/// // Get suggestions
/// let suggestions = try await maintainer.getSuggestions(
///     field: "name",
///     prefix: "lap",
///     limit: 10,
///     transaction: transaction
/// )
/// // ["laptop", "laptop bag", "lap desk"]
/// ```
public struct AutocompleteMaintainer<Item: PersistedEntityValue>: IndexMaintainer {
    public let index: ResolvedIndex
    public let subspace: Subspace
    public let idExpression: KeyExpression

    private let fields: [FieldIdentity]
    private let minPrefixLength: Int
    private let maxPrefixLength: Int

    // Subspaces
    private let suggestionsSubspace: Subspace
    private let termsSubspace: Subspace

    /// Create an autocomplete maintainer
    ///
    /// - Parameters:
    ///   - subspace: FDB subspace for autocomplete data
    ///   - idExpression: Expression for extracting item's unique identifier
    ///   - autocompleteFields: Field names to maintain autocomplete for
    ///   - minPrefixLength: Minimum prefix length to store (default: 1)
    ///   - maxPrefixLength: Maximum prefix length to store (default: 10)
    public init(
        index: ResolvedIndex,
        subspace: Subspace,
        idExpression: KeyExpression,
        fields: [FieldIdentity],
        minPrefixLength: Int,
        maxPrefixLength: Int
    ) {
        self.index = index
        self.subspace = subspace
        self.idExpression = idExpression
        self.fields = fields
        self.minPrefixLength = minPrefixLength
        self.maxPrefixLength = maxPrefixLength
        self.suggestionsSubspace = subspace.subspace("suggestions")
        self.termsSubspace = subspace.subspace("terms")
    }

    /// Update autocomplete indexes when a document changes
    ///
    /// - Parameters:
    ///   - oldItem: Previous item state (nil for new items)
    ///   - newItem: New item state (nil for deletions)
    ///   - transaction: FDB transaction
    public func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionAccess
    ) async throws {
        // Remove old autocomplete entries
        if let oldItem = oldItem {
            try await removeAutocomplete(item: oldItem, transaction: transaction)
        }

        // Add new autocomplete entries
        if let newItem = newItem {
            try await addAutocomplete(item: newItem, transaction: transaction)
        }
    }

    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionAccess
    ) async throws {
        try await addAutocomplete(
            item: item,
            transaction: transaction
        )
    }

    // MARK: - Private Methods

    /// Add autocomplete entries for an item
    private func addAutocomplete(
        item: Item,
        transaction: any TransactionAccess
    ) async throws {
        for field in fields {
            let terms = try extractTerms(from: item, field: field)

            for term in terms {
                // Increment term count
                let termKey = termsSubspace.subspace(field.name).pack(Tuple(term))
                try transaction.atomicOp(key: termKey, param: ByteConversion.int64ToBytes(1), mutationType: .add)

                // Add all prefixes
                let prefixes = generatePrefixes(for: term)
                for prefix in prefixes {
                    let suggestionKey = suggestionsSubspace.subspace(field.name).subspace(prefix).pack(Tuple(term))
                    try transaction.atomicOp(key: suggestionKey, param: ByteConversion.int64ToBytes(1), mutationType: .add)
                }
            }
        }
    }

    /// Remove autocomplete entries for an item
    private func removeAutocomplete(
        item: Item,
        transaction: any TransactionAccess
    ) async throws {
        for field in fields {
            let terms = try extractTerms(from: item, field: field)

            for term in terms {
                // Decrement term count
                let termKey = termsSubspace.subspace(field.name).pack(Tuple(term))
                try transaction.atomicOp(key: termKey, param: ByteConversion.int64ToBytes(-1), mutationType: .add)

                // Remove all prefixes
                let prefixes = generatePrefixes(for: term)
                for prefix in prefixes {
                    let suggestionKey = suggestionsSubspace.subspace(field.name).subspace(prefix).pack(Tuple(term))
                    try transaction.atomicOp(key: suggestionKey, param: ByteConversion.int64ToBytes(-1), mutationType: .add)
                }
            }
        }
    }

    /// Extract terms from an item's field for autocomplete
    private func extractTerms(
        from item: Item,
        field: FieldIdentity
    ) throws -> [String] {
        var terms: [String] = []
        for string in try FullTextFieldValueExtractor.strings(
            from: item,
            field: field
        ) {
            terms.append(contentsOf: tokenize(string))
        }
        return terms
    }

    /// Tokenize text into terms for autocomplete
    ///
    /// Uses simple word-boundary tokenization. For autocomplete, we keep
    /// individual words as separate suggestions.
    private func tokenize(_ text: String) -> [String] {
        let normalized = normalizeText(text)
        var terms: [String] = []
        FullTextTextUtilities.forEachTokenSlice(in: normalized) { slice in
            if slice.count >= minPrefixLength {
                terms.append(String(slice))
            }
        }
        return terms
    }

    /// Normalize text for consistent matching
    private func normalizeText(_ text: String) -> String {
        let lowered = text.lowercased()
        return String(FullTextTextUtilities.trimmingWhitespace(lowered))
    }

    /// Generate all prefixes for a term
    ///
    /// For term "laptop", generates: ["l", "la", "lap", "lapt", "lapto", "laptop"]
    /// (respecting minPrefixLength and maxPrefixLength)
    private func generatePrefixes(for term: String) -> [String] {
        var prefixes: [String] = []
        var length = 0
        var end = term.startIndex
        while end < term.endIndex, length < maxPrefixLength {
            end = term.index(after: end)
            length += 1
            if length >= minPrefixLength {
                prefixes.append(String(term[..<end]))
            }
        }

        return prefixes
    }
}
