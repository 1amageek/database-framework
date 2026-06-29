// FusionQuery.swift
// DatabaseEngine - Protocol for fusion-compatible queries

import Foundation
import Core

/// Protocol for queries that can participate in fusion operations
///
/// Conforming types can be combined using `FusionBuilder` to create
/// hybrid search queries that merge results from multiple sources.
///
/// **Design Principle**:
/// Each Index module (VectorIndex, FullTextIndex, etc.) provides its own
/// FusionQuery implementation. DatabaseEngine does not know about specific
/// index types - it only knows this protocol.
///
/// **Implementing a FusionQuery** (in Index module):
/// ```swift
/// // In VectorIndex/Fusion/Similar.swift
/// public struct Similar<T: Persistable>: FusionQuery {
///     private let queryContext: IndexQueryContext
///     // ... other properties
///
///     public func execute(candidates: Set<String>?) async throws -> [ScoredResult<T>] {
///         // Use IndexDescriptor to find index name
///         guard let descriptor = findIndexDescriptor() else { throw ... }
///         // Execute query using queryContext
///     }
/// }
/// ```
///
/// **Usage in Fusion**:
/// ```swift
/// let results = try await context.fuse(Product.self) {
///     Search(\.description).terms(["coffee"])
///     Similar(\.embedding, dimensions: 384).query(vector, k: 100)
/// }
/// .execute()
/// ```
public protocol FusionQuery<Item>: Sendable {
    /// The item type this query returns
    associatedtype Item: Persistable

    /// Execute the query and return scored results
    ///
    /// - Parameter candidates: Optional set of candidate IDs to restrict results to.
    ///                         When provided, the query should only return items whose
    ///                         ID (as string) is in this set. This enables pipeline
    ///                         optimization where later stages only search within
    ///                         candidates from earlier stages.
    /// - Returns: Array of scored results, sorted by score descending.
    ///            Scores should be normalized to [0, 1] where higher is better.
    func execute(candidates: Set<String>?) async throws -> [ScoredResult<Item>]
}

/// Error type for FusionQuery implementations
public enum FusionQueryError: Error, CustomStringConvertible {
    /// Index not found for the specified field
    case indexNotFound(type: String, field: String, kind: String)

    /// Query not properly configured
    case invalidConfiguration(String)

    public var description: String {
        switch self {
        case .indexNotFound(let type, let field, let kind):
            return "No \(kind) index found for field '\(field)' on type '\(type)'"
        case .invalidConfiguration(let reason):
            return "Invalid query configuration: \(reason)"
        }
    }
}
