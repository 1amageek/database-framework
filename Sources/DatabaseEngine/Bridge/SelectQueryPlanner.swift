// SelectQueryPlanner.swift
// DatabaseEngine - QueryPlanner extension for SelectQuery entry point

import Foundation
import Core
import QueryIR

// MARK: - QueryPlanner + SelectQuery

extension QueryPlanner {
    /// Plan a SelectQuery and return the optimal execution plan.
    ///
    /// Converts the SelectQuery to a `Query<T>` and delegates to the existing
    /// `plan(query:)` method. Returns `nil` if the SelectQuery cannot be
    /// represented as a `Query<T>` (e.g., contains subqueries, GROUP BY,
    /// HAVING, or unsupported expression patterns).
    ///
    /// - Parameter selectQuery: The QueryIR SelectQuery to plan.
    /// - Returns: The optimal QueryPlan, or `nil` if conversion fails.
    /// - Throws: Planning errors from the underlying query planner.
    public func plan(selectQuery: QueryIR.SelectQuery) throws -> QueryPlan<T>? {
        guard let query: Query<T> = selectQuery.toQuery(for: T.self) else {
            return nil
        }
        return try plan(query: query)
    }

    /// Plan a SelectQuery with hints.
    ///
    /// - Parameters:
    ///   - selectQuery: The QueryIR SelectQuery to plan.
    ///   - hints: Query hints to influence planning.
    /// - Returns: The optimal QueryPlan, or `nil` if conversion fails.
    /// - Throws: Planning errors from the underlying query planner.
    public func plan(selectQuery: QueryIR.SelectQuery, hints: QueryHints) throws -> QueryPlan<T>? {
        guard let query: Query<T> = selectQuery.toQuery(for: T.self) else {
            return nil
        }
        return try plan(query: query, hints: hints)
    }
}
