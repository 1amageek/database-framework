// AutocompleteQuery.swift
// FullTextIndex - Autocomplete Query API
//
// Provides a fluent API for autocomplete/typeahead queries

import Foundation
import Core
import DatabaseEngine
import FoundationDB

// MARK: - Autocomplete Query Builder

/// Builder for autocomplete queries
///
/// **Usage**:
/// ```swift
/// import FullTextIndex
///
/// let suggestions = try await context.autocomplete(Product.self)
///     .field(\.name)
///     .prefix("lap")
///     .limit(10)
///     .execute()
///
/// for suggestion in suggestions {
///     print("\(suggestion.term) (\(suggestion.score))")
/// }
/// ```
public struct AutocompleteQueryBuilder<T: Persistable>: Sendable {
    private let queryContext: IndexQueryContext
    private var fieldName: String?
    private var searchPrefix: String = ""
    private var fetchLimit: Int = 10
    private var minPrefixLength: Int = 1
    private var maxPrefixLength: Int = 10

    internal init(queryContext: IndexQueryContext) {
        self.queryContext = queryContext
    }

    /// Specify the field to search for autocomplete
    ///
    /// - Parameter keyPath: KeyPath to the String field
    /// - Returns: Updated query builder
    public func field(_ keyPath: KeyPath<T, String>) -> Self {
        var copy = self
        copy.fieldName = T.fieldName(for: keyPath)
        return copy
    }

    /// Specify the optional field to search for autocomplete
    ///
    /// - Parameter keyPath: KeyPath to the optional String field
    /// - Returns: Updated query builder
    public func field(_ keyPath: KeyPath<T, String?>) -> Self {
        var copy = self
        copy.fieldName = T.fieldName(for: keyPath)
        return copy
    }

    /// Specify the field name directly
    ///
    /// - Parameter name: Field name as string
    /// - Returns: Updated query builder
    public func field(_ name: String) -> Self {
        var copy = self
        copy.fieldName = name
        return copy
    }

    /// Set the prefix to match
    ///
    /// - Parameter prefix: The prefix string (what the user has typed)
    /// - Returns: Updated query builder
    public func prefix(_ prefix: String) -> Self {
        var copy = self
        copy.searchPrefix = prefix
        return copy
    }

    /// Limit the number of suggestions
    ///
    /// - Parameter count: Maximum number of suggestions (default: 10)
    /// - Returns: Updated query builder
    public func limit(_ count: Int) -> Self {
        var copy = self
        copy.fetchLimit = count
        return copy
    }

    /// Configure minimum prefix length for suggestions
    ///
    /// - Parameter length: Minimum prefix length (default: 1)
    /// - Returns: Updated query builder
    public func minPrefix(_ length: Int) -> Self {
        var copy = self
        copy.minPrefixLength = length
        return copy
    }

    /// Execute the autocomplete query
    ///
    /// - Returns: Array of autocomplete suggestions sorted by score descending
    /// - Throws: Error if query fails
    public func execute() async throws -> [AutocompleteSuggestion] {
        guard let fieldName = fieldName else {
            throw AutocompleteError.noFieldSpecified
        }

        guard !searchPrefix.isEmpty else {
            return []
        }

        let normalizedPrefix = searchPrefix.lowercased().trimmingCharacters(in: .whitespaces)
        guard normalizedPrefix.count >= minPrefixLength else {
            return []
        }

        let indexName = "\(T.persistableType)_autocomplete"
        let typeSubspace = try await queryContext.indexSubspace(for: T.self)
        let autocompleteSubspace = typeSubspace.subspace(indexName)

        return try await queryContext.withTransaction { transaction in
            let maintainer = AutocompleteMaintainer<T>(
                subspace: autocompleteSubspace,
                idExpression: FieldKeyExpression(fieldName: "id"),
                autocompleteFields: [fieldName],
                minPrefixLength: self.minPrefixLength,
                maxPrefixLength: self.maxPrefixLength
            )

            return try await maintainer.getSuggestions(
                field: fieldName,
                prefix: self.searchPrefix,
                limit: self.fetchLimit,
                transaction: transaction
            )
        }
    }

    /// Get popular terms for the field (regardless of prefix)
    ///
    /// Useful for showing popular/trending terms when the search box is empty.
    ///
    /// - Returns: Array of popular terms sorted by frequency descending
    /// - Throws: Error if query fails
    public func getPopularTerms() async throws -> [AutocompleteSuggestion] {
        guard let fieldName = fieldName else {
            throw AutocompleteError.noFieldSpecified
        }

        let indexName = "\(T.persistableType)_autocomplete"
        let typeSubspace = try await queryContext.indexSubspace(for: T.self)
        let autocompleteSubspace = typeSubspace.subspace(indexName)

        return try await queryContext.withTransaction { transaction in
            let maintainer = AutocompleteMaintainer<T>(
                subspace: autocompleteSubspace,
                idExpression: FieldKeyExpression(fieldName: "id"),
                autocompleteFields: [fieldName],
                minPrefixLength: self.minPrefixLength,
                maxPrefixLength: self.maxPrefixLength
            )

            return try await maintainer.getPopularTerms(
                field: fieldName,
                limit: self.fetchLimit,
                transaction: transaction
            )
        }
    }
}

// MARK: - FDBContext Extension

extension FDBContext {

    /// Start an autocomplete query
    ///
    /// This method is available when you import `FullTextIndex`.
    ///
    /// **Usage**:
    /// ```swift
    /// import FullTextIndex
    ///
    /// let suggestions = try await context.autocomplete(Product.self)
    ///     .field(\.name)
    ///     .prefix("lap")
    ///     .limit(10)
    ///     .execute()
    /// ```
    ///
    /// - Parameter type: The Persistable type to get suggestions for
    /// - Returns: Autocomplete query builder
    public func autocomplete<T: Persistable>(_ type: T.Type) -> AutocompleteQueryBuilder<T> {
        AutocompleteQueryBuilder(queryContext: indexQueryContext)
    }
}

// MARK: - Autocomplete Error

/// Errors for autocomplete operations
public enum AutocompleteError: Error, CustomStringConvertible {
    /// No field specified for autocomplete
    case noFieldSpecified

    /// Index not found
    case indexNotFound(String)

    public var description: String {
        switch self {
        case .noFieldSpecified:
            return "No field specified for autocomplete query"
        case .indexNotFound(let name):
            return "Autocomplete index not found: \(name)"
        }
    }
}
