// FDBContext+QueryIR.swift
// DatabaseEngine - FDBContext extension for executing QueryIR SelectQuery

import Foundation
import Core
import QueryIR

// MARK: - Error Types

/// Errors that occur during QueryIR bridge operations.
public enum QueryBridgeError: Error, Sendable {
    /// The SelectQuery could not be converted to a type-safe Query<T>.
    /// This happens when the query uses features not supported by the bridge:
    /// subqueries, GROUP BY, HAVING, unsupported expression patterns, or
    /// when the source table name does not match the target type.
    case cannotConvertSelectQuery

    /// An expression in the query uses patterns not representable in the
    /// type-safe predicate system (e.g., variables, aggregates, functions).
    case unsupportedExpression

    /// A literal in the query cannot be converted to FieldValue
    /// (e.g., IRI, blank node, typed literal, language-tagged literal).
    case incompatibleLiteralType
}

// MARK: - FDBContext + SelectQuery Execution

extension FDBContext {
    /// Execute a QueryIR SelectQuery and return typed results.
    ///
    /// **Note**: This method is designed to work with pre-rewritten queries.
    /// If the query contains SPARQL() functions, they should be rewritten
    /// at the Database layer before calling this method.
    ///
    /// Converts the SelectQuery to a `Query<T>` and delegates to the
    /// existing `fetch(query:)` path. This preserves all existing
    /// optimizations (query planner, index selection, cache policies).
    ///
    /// - Parameters:
    ///   - selectQuery: The QueryIR SelectQuery to execute.
    ///   - type: The Persistable type to fetch.
    /// - Returns: Array of matching models.
    /// - Throws: `QueryBridgeError.cannotConvertSelectQuery` if the query
    ///   cannot be converted, or any underlying fetch errors.
    public func execute<T: Persistable>(
        _ selectQuery: QueryIR.SelectQuery,
        as type: T.Type
    ) async throws -> [T] {
        guard let query: Query<T> = selectQuery.toQuery(for: type) else {
            throw QueryBridgeError.cannotConvertSelectQuery
        }
        return try await fetch(query)
    }
}

