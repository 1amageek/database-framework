// AutocompleteMaintainer.swift
// FullTextIndex - Prefix Trie-based Autocomplete
//
// Reference: Trie data structure for prefix matching
// Knuth, "The Art of Computer Programming", Vol. 3

import Foundation
import Core
import DatabaseEngine
import FoundationDB

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
public struct AutocompleteMaintainer<Item: Persistable>: Sendable {
    private let subspace: Subspace
    private let idExpression: KeyExpression
    private let autocompleteFields: [String]
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
        subspace: Subspace,
        idExpression: KeyExpression,
        autocompleteFields: [String],
        minPrefixLength: Int = 1,
        maxPrefixLength: Int = 10
    ) {
        self.subspace = subspace
        self.idExpression = idExpression
        self.autocompleteFields = autocompleteFields
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
    public func updateAutocomplete(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionProtocol
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

    /// Get autocomplete suggestions for a prefix
    ///
    /// - Parameters:
    ///   - field: Field to get suggestions for
    ///   - prefix: The prefix to match
    ///   - limit: Maximum number of suggestions (default: 10)
    ///   - transaction: FDB transaction
    /// - Returns: Array of suggestions sorted by frequency descending
    public func getSuggestions(
        field: String,
        prefix: String,
        limit: Int = 10,
        transaction: any TransactionProtocol
    ) async throws -> [AutocompleteSuggestion] {
        let normalizedPrefix = normalizeText(prefix)
        guard normalizedPrefix.count >= minPrefixLength else {
            return []
        }

        // Scan suggestions for this prefix
        let fieldSubspace = suggestionsSubspace.subspace(field)
        let prefixSubspace = fieldSubspace.subspace(normalizedPrefix)
        let (begin, end) = prefixSubspace.range()

        var suggestions: [(term: String, score: Int64)] = []

        let sequence = transaction.getRange(begin: begin, end: end, snapshot: true)

        for try await (key, value) in sequence {
            guard prefixSubspace.contains(key) else { break }

            guard let keyTuple = try? prefixSubspace.unpack(key),
                  let term = keyTuple[0] as? String else {
                continue
            }

            let score = bytesToInt64(value)
            if score > 0 {
                suggestions.append((term: term, score: score))
            }
        }

        // Sort by score descending and limit
        suggestions.sort { $0.score > $1.score }
        return Array(suggestions.prefix(limit)).map {
            AutocompleteSuggestion(term: $0.term, score: $0.score)
        }
    }

    /// Get all terms with their frequencies for a field
    ///
    /// - Parameters:
    ///   - field: Field to get terms for
    ///   - limit: Maximum number of terms
    ///   - transaction: FDB transaction
    /// - Returns: Array of (term, frequency) sorted by frequency descending
    public func getPopularTerms(
        field: String,
        limit: Int = 100,
        transaction: any TransactionProtocol
    ) async throws -> [AutocompleteSuggestion] {
        let fieldSubspace = termsSubspace.subspace(field)
        let (begin, end) = fieldSubspace.range()

        var terms: [(term: String, score: Int64)] = []

        let sequence = transaction.getRange(begin: begin, end: end, snapshot: true)

        for try await (key, value) in sequence {
            guard fieldSubspace.contains(key) else { break }

            guard let keyTuple = try? fieldSubspace.unpack(key),
                  let term = keyTuple[0] as? String else {
                continue
            }

            let score = bytesToInt64(value)
            if score > 0 {
                terms.append((term: term, score: score))
            }
        }

        // Sort by score descending and limit
        terms.sort { $0.score > $1.score }
        return Array(terms.prefix(limit)).map {
            AutocompleteSuggestion(term: $0.term, score: $0.score)
        }
    }

    // MARK: - Private Methods

    /// Add autocomplete entries for an item
    private func addAutocomplete(
        item: Item,
        transaction: any TransactionProtocol
    ) async throws {
        for field in autocompleteFields {
            let terms = extractTerms(from: item, field: field)

            for term in terms {
                // Increment term count
                let termKey = termsSubspace.subspace(field).pack(Tuple(term))
                transaction.atomicOp(key: termKey, param: int64ToBytes(1), mutationType: .add)

                // Add all prefixes
                let prefixes = generatePrefixes(for: term)
                for prefix in prefixes {
                    let suggestionKey = suggestionsSubspace.subspace(field).subspace(prefix).pack(Tuple(term))
                    transaction.atomicOp(key: suggestionKey, param: int64ToBytes(1), mutationType: .add)
                }
            }
        }
    }

    /// Remove autocomplete entries for an item
    private func removeAutocomplete(
        item: Item,
        transaction: any TransactionProtocol
    ) async throws {
        for field in autocompleteFields {
            let terms = extractTerms(from: item, field: field)

            for term in terms {
                // Decrement term count
                let termKey = termsSubspace.subspace(field).pack(Tuple(term))
                transaction.atomicOp(key: termKey, param: int64ToBytes(-1), mutationType: .add)

                // Remove all prefixes
                let prefixes = generatePrefixes(for: term)
                for prefix in prefixes {
                    let suggestionKey = suggestionsSubspace.subspace(field).subspace(prefix).pack(Tuple(term))
                    transaction.atomicOp(key: suggestionKey, param: int64ToBytes(-1), mutationType: .add)
                }
            }
        }
    }

    /// Extract terms from an item's field for autocomplete
    private func extractTerms(from item: Item, field: String) -> [String] {
        guard let value = item[dynamicMember: field] else {
            return []
        }

        var terms: [String] = []

        // Handle arrays
        if let array = value as? [String] {
            for str in array {
                terms.append(contentsOf: tokenize(str))
            }
        } else if let string = value as? String {
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
        let words = normalized.components(separatedBy: CharacterSet.alphanumerics.inverted)
            .filter { $0.count >= minPrefixLength }
        return words
    }

    /// Normalize text for consistent matching
    private func normalizeText(_ text: String) -> String {
        return text.lowercased().trimmingCharacters(in: .whitespaces)
    }

    /// Generate all prefixes for a term
    ///
    /// For term "laptop", generates: ["l", "la", "lap", "lapt", "lapto", "laptop"]
    /// (respecting minPrefixLength and maxPrefixLength)
    private func generatePrefixes(for term: String) -> [String] {
        let characters = Array(term)
        var prefixes: [String] = []

        let start = minPrefixLength
        let end = min(maxPrefixLength, characters.count)

        for length in start...end {
            let prefix = String(characters.prefix(length))
            prefixes.append(prefix)
        }

        return prefixes
    }

    /// Convert Int64 to little-endian bytes
    private func int64ToBytes(_ value: Int64) -> [UInt8] {
        ByteConversion.int64ToBytes(value)
    }

    /// Convert little-endian bytes to Int64
    private func bytesToInt64(_ bytes: [UInt8]) -> Int64 {
        ByteConversion.bytesToInt64(bytes)
    }
}

// MARK: - Autocomplete Suggestion

/// A single autocomplete suggestion with its score
public struct AutocompleteSuggestion: Sendable, Hashable {
    /// The suggested term
    public let term: String

    /// Score/frequency of this suggestion
    public let score: Int64

    public init(term: String, score: Int64) {
        self.term = term
        self.score = score
    }
}
