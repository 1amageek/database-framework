// SPARQLEntryPoint.swift
// GraphIndex - Entry point for SPARQL-like queries
//
// Provides the FDBContext extension and entry point for SPARQL queries.

import Foundation
import Core
import DatabaseEngine
import Graph

// MARK: - SPARQL Entry Point

/// Entry point for SPARQL-like queries on graph indexes
///
/// **Usage**:
/// ```swift
/// // Using explicit index fields
/// let results = try await context.sparql(Statement.self)
///     .index(\.subject, \.predicate, \.object)
///     .where("?person", "knows", "Alice")
///     .execute()
///
/// // Using default index
/// let results = try await context.sparql(Statement.self)
///     .defaultIndex()
///     .where("Alice", "knows", "?friend")
///     .select("?friend")
///     .execute()
/// ```
public struct SPARQLEntryPoint<T: Persistable>: Sendable {

    private let queryContext: IndexQueryContext

    internal init(queryContext: IndexQueryContext) {
        self.queryContext = queryContext
    }

    // MARK: - Index Specification

    /// Specify the graph index fields
    ///
    /// - Parameters:
    ///   - from: KeyPath to the source/subject field
    ///   - edge: KeyPath to the edge/predicate field
    ///   - to: KeyPath to the target/object field
    /// - Returns: SPARQL query builder
    public func index<V1, V2, V3>(
        _ from: KeyPath<T, V1>,
        _ edge: KeyPath<T, V2>,
        _ to: KeyPath<T, V3>
    ) -> SPARQLQueryBuilder<T> {
        let fromField = T.fieldName(for: from)
        let edgeField = T.fieldName(for: edge)
        let toField = T.fieldName(for: to)
        return SPARQLQueryBuilder(
            queryContext: queryContext,
            fromFieldName: fromField,
            edgeFieldName: edgeField,
            toFieldName: toField
        )
    }

    /// Use the default graph index (first GraphIndexKind found)
    ///
    /// - Returns: SPARQL query builder configured with the default index
    public func defaultIndex() -> SPARQLQueryBuilder<T> {
        // Find the first GraphIndexKind for this type
        let descriptor = T.indexDescriptors.first { desc in
            desc.kindIdentifier == GraphIndexKind<T>.identifier
        }

        guard let desc = descriptor,
              let kind = desc.kind as? GraphIndexKind<T> else {
            // Return a builder that will fail on execute
            return SPARQLQueryBuilder(
                queryContext: queryContext,
                fromFieldName: "",
                edgeFieldName: "",
                toFieldName: ""
            )
        }

        return SPARQLQueryBuilder(
            queryContext: queryContext,
            fromFieldName: kind.fromField,
            edgeFieldName: kind.edgeField,
            toFieldName: kind.toField
        )
    }
}

// MARK: - FDBContext Extension

extension FDBContext {

    /// Start a SPARQL-like graph query
    ///
    /// **Usage**:
    /// ```swift
    /// import GraphIndex
    ///
    /// // Find all people Alice knows
    /// let results = try await context.sparql(Statement.self)
    ///     .defaultIndex()
    ///     .where("Alice", "knows", "?friend")
    ///     .select("?friend")
    ///     .execute()
    ///
    /// // Multi-pattern query with join
    /// let results = try await context.sparql(Statement.self)
    ///     .defaultIndex()
    ///     .where("?person", "knows", "Bob")
    ///     .where("?person", "name", "?name")
    ///     .select("?person", "?name")
    ///     .execute()
    ///
    /// // Friends of friends
    /// let results = try await context.sparql(Statement.self)
    ///     .defaultIndex()
    ///     .where("Alice", "knows", "?friend")
    ///     .where("?friend", "knows", "?fof")
    ///     .filter(.variableNotEquals("?fof", "Alice"))
    ///     .select("?fof")
    ///     .distinct()
    ///     .execute()
    /// ```
    ///
    /// - Parameter type: The Persistable type to query
    /// - Returns: Entry point for configuring the SPARQL query
    public func sparql<T: Persistable>(_ type: T.Type) -> SPARQLEntryPoint<T> {
        SPARQLEntryPoint(queryContext: indexQueryContext)
    }
}

// MARK: - SPARQL Query Error

/// Errors for SPARQL query operations
public enum SPARQLQueryError: Error, CustomStringConvertible {
    /// Index not configured
    case indexNotConfigured

    /// Index not found
    case indexNotFound(String)

    /// Invalid pattern
    case invalidPattern(String)

    /// Execution failed
    case executionFailed(String)

    /// Variable conflict in join
    case variableConflict(variable: String, existingValue: String, newValue: String)

    /// No patterns specified
    case noPatterns

    /// Invalid GROUP BY
    case invalidGroupBy(String)

    public var description: String {
        switch self {
        case .indexNotConfigured:
            return "Graph index not configured. Use .index() to specify fields or .defaultIndex()."
        case .indexNotFound(let name):
            return "Graph index not found: \(name)"
        case .invalidPattern(let reason):
            return "Invalid query pattern: \(reason)"
        case .executionFailed(let reason):
            return "Query execution failed: \(reason)"
        case .variableConflict(let variable, let existing, let new):
            return "Variable \(variable) conflict: existing='\(existing)', new='\(new)'"
        case .noPatterns:
            return "No patterns specified in query"
        case .invalidGroupBy(let reason):
            return "Invalid GROUP BY: \(reason)"
        }
    }
}
