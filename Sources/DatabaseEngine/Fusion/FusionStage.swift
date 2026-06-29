// FusionStage.swift
// DatabaseEngine - Execution stages for fusion queries

import Foundation
import Core

/// Protocol for execution stages in a fusion pipeline
///
/// A stage represents one or more queries that execute together.
/// Stages execute sequentially in a pipeline, with each stage
/// potentially restricting candidates for subsequent stages.
public protocol FusionStage<Item>: Sendable {
    /// The item type this stage returns
    associatedtype Item: Persistable

    /// Execute the stage and return results from all queries
    ///
    /// - Parameter candidates: Optional candidate IDs from previous stages
    /// - Returns: Array of result arrays (one per query in the stage)
    func execute(candidates: Set<String>?) async throws -> [[ScoredResult<Item>]]
}

// MARK: - SingleStage

/// A stage containing a single query
///
/// Used internally by FusionBuilder when a single query is added.
public struct SingleStage<T: Persistable>: FusionStage {
    public typealias Item = T

    let query: any FusionQuery<T>

    public init(query: any FusionQuery<T>) {
        self.query = query
    }

    public func execute(candidates: Set<String>?) async throws -> [[ScoredResult<T>]] {
        let results = try await query.execute(candidates: candidates)
        return [results]
    }
}

// MARK: - Parallel

/// A stage containing multiple queries that execute in parallel
///
/// All queries in a Parallel stage run concurrently using TaskGroup.
/// Their results are collected and returned as separate arrays.
///
/// **Usage**:
/// ```swift
/// let results = try await context.fuse(Product.self) {
///     Search(\.title).terms(["coffee"])
///
///     Parallel {
///         Similar(\.embedding, dimensions: 384).query(vector, k: 100)
///         Nearby(\.location).within(radiusKm: 5, of: here)
///     }
/// }
/// .execute()
/// ```
public struct Parallel<T: Persistable>: FusionStage {
    public typealias Item = T

    let queries: [any FusionQuery<T>]

    /// Create a parallel stage using ResultBuilder
    ///
    /// - Parameter content: Builder closure containing queries
    public init(@FusionQueryBuilder<T> _ content: () -> [any FusionQuery<T>]) {
        self.queries = content()
    }

    /// Create a parallel stage from an array of queries
    ///
    /// - Parameter queries: Array of queries to execute in parallel
    public init(queries: [any FusionQuery<T>]) {
        self.queries = queries
    }

    public func execute(candidates: Set<String>?) async throws -> [[ScoredResult<T>]] {
        guard !queries.isEmpty else { return [] }

        return try await withThrowingTaskGroup(of: (Int, [ScoredResult<T>]).self) { group in
            // Launch all queries in parallel with index tracking
            for (index, query) in queries.enumerated() {
                group.addTask {
                    let results = try await query.execute(candidates: candidates)
                    return (index, results)
                }
            }

            // Collect results maintaining order
            var indexedResults: [(Int, [ScoredResult<T>])] = []
            for try await result in group {
                indexedResults.append(result)
            }

            // Sort by original index and return results only
            return indexedResults
                .sorted { $0.0 < $1.0 }
                .map { $0.1 }
        }
    }
}

// MARK: - FusionQueryBuilder

/// ResultBuilder for constructing query arrays in Parallel stages
@resultBuilder
public struct FusionQueryBuilder<T: Persistable> {

    public static func buildBlock(_ queries: (any FusionQuery<T>)...) -> [any FusionQuery<T>] {
        queries
    }

    public static func buildExpression(_ query: some FusionQuery<T>) -> any FusionQuery<T> {
        query
    }

    public static func buildArray(_ components: [[any FusionQuery<T>]]) -> [any FusionQuery<T>] {
        components.flatMap { $0 }
    }

    public static func buildOptional(_ component: [any FusionQuery<T>]?) -> [any FusionQuery<T>] {
        component ?? []
    }

    public static func buildEither(first component: [any FusionQuery<T>]) -> [any FusionQuery<T>] {
        component
    }

    public static func buildEither(second component: [any FusionQuery<T>]) -> [any FusionQuery<T>] {
        component
    }
}
