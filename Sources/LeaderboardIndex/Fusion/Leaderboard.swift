// Leaderboard.swift
// LeaderboardIndex - Leaderboard ranking query for Fusion
//
// This file is part of LeaderboardIndex module, not DatabaseEngine.
// DatabaseEngine does not know about TimeWindowLeaderboardIndexKind.

import Foundation
import Core
import DatabaseEngine
import FoundationDB

/// Leaderboard ranking query for Fusion
///
/// Returns top K items from the leaderboard, scored by their ranking position.
/// Higher ranked items (better scores) get higher fusion scores.
///
/// **Usage**:
/// ```swift
/// let results = try await context.fuse(GameScore.self) {
///     // Get top 100 from leaderboard
///     Leaderboard(\.score).topK(100)
///
///     // Combine with user preferences
///     Similar(\.playerProfile, dimensions: 128).query(userVector, k: 50)
/// }
/// .algorithm(.rrf())
/// .execute()
///
/// // With grouping (e.g., by region)
/// let results = try await context.fuse(GameScore.self) {
///     Leaderboard(\.score, groupBy: \.region).topK(50).group("asia")
/// }
/// .execute()
/// ```
public struct Leaderboard<T: Persistable>: FusionQuery, Sendable {
    public typealias Item = T

    private let queryContext: IndexQueryContext
    private let scoreFieldName: String
    private let groupByFieldName: String?
    private var k: Int = 100
    private var groupValue: (any Sendable & TupleElement)?
    private var windowId: Int64?

    // MARK: - Initialization (FusionContext)

    /// Create a Leaderboard query for a score field
    ///
    /// Uses FusionContext.current for context (automatically set by `context.fuse { }`).
    ///
    /// **Usage**:
    /// ```swift
    /// context.fuse(GameScore.self) {
    ///     Leaderboard(\.score).topK(100)
    /// }
    /// ```
    public init(_ scoreKeyPath: KeyPath<T, Int>) {
        guard let context = FusionContext.current else {
            fatalError("Leaderboard must be used within context.fuse { } block")
        }
        self.scoreFieldName = T.fieldName(for: scoreKeyPath)
        self.groupByFieldName = nil
        self.queryContext = context
    }

    /// Create a Leaderboard query for an Int64 score field
    public init(_ scoreKeyPath: KeyPath<T, Int64>) {
        guard let context = FusionContext.current else {
            fatalError("Leaderboard must be used within context.fuse { } block")
        }
        self.scoreFieldName = T.fieldName(for: scoreKeyPath)
        self.groupByFieldName = nil
        self.queryContext = context
    }

    /// Create a Leaderboard query for a Double score field
    public init(_ scoreKeyPath: KeyPath<T, Double>) {
        guard let context = FusionContext.current else {
            fatalError("Leaderboard must be used within context.fuse { } block")
        }
        self.scoreFieldName = T.fieldName(for: scoreKeyPath)
        self.groupByFieldName = nil
        self.queryContext = context
    }

    /// Create a Leaderboard query with grouping
    ///
    /// **Usage**:
    /// ```swift
    /// Leaderboard(\.score, groupBy: \.region).topK(50).group("asia")
    /// ```
    public init<G: Sendable & Hashable>(
        _ scoreKeyPath: KeyPath<T, Int64>,
        groupBy groupKeyPath: KeyPath<T, G>
    ) {
        guard let context = FusionContext.current else {
            fatalError("Leaderboard must be used within context.fuse { } block")
        }
        self.scoreFieldName = T.fieldName(for: scoreKeyPath)
        self.groupByFieldName = T.fieldName(for: groupKeyPath)
        self.queryContext = context
    }

    // MARK: - Initialization (Explicit Context)

    /// Create a Leaderboard query with explicit context
    public init(_ scoreKeyPath: KeyPath<T, Int>, context: IndexQueryContext) {
        self.scoreFieldName = T.fieldName(for: scoreKeyPath)
        self.groupByFieldName = nil
        self.queryContext = context
    }

    /// Create a Leaderboard query for Int64 with explicit context
    public init(_ scoreKeyPath: KeyPath<T, Int64>, context: IndexQueryContext) {
        self.scoreFieldName = T.fieldName(for: scoreKeyPath)
        self.groupByFieldName = nil
        self.queryContext = context
    }

    /// Create a Leaderboard query for Double with explicit context
    public init(_ scoreKeyPath: KeyPath<T, Double>, context: IndexQueryContext) {
        self.scoreFieldName = T.fieldName(for: scoreKeyPath)
        self.groupByFieldName = nil
        self.queryContext = context
    }

    /// Create a Leaderboard query with grouping and explicit context
    public init<G: Sendable & Hashable>(
        _ scoreKeyPath: KeyPath<T, Int64>,
        groupBy groupKeyPath: KeyPath<T, G>,
        context: IndexQueryContext
    ) {
        self.scoreFieldName = T.fieldName(for: scoreKeyPath)
        self.groupByFieldName = T.fieldName(for: groupKeyPath)
        self.queryContext = context
    }

    // MARK: - Configuration

    /// Set the number of top entries to retrieve
    ///
    /// - Parameter count: Number of top entries (default: 100)
    /// - Returns: Updated query
    public func topK(_ count: Int) -> Self {
        var copy = self
        copy.k = count
        return copy
    }

    /// Filter by group value
    ///
    /// - Parameter value: Group value to filter by
    /// - Returns: Updated query
    public func group<V: Sendable & TupleElement>(_ value: V) -> Self {
        var copy = self
        copy.groupValue = value
        return copy
    }

    /// Query a specific historical window
    ///
    /// - Parameter windowId: Window ID to query
    /// - Returns: Updated query
    public func window(_ windowId: Int64) -> Self {
        var copy = self
        copy.windowId = windowId
        return copy
    }

    // MARK: - Index Discovery

    /// Find the index descriptor for leaderboard
    private func findIndexDescriptor() -> IndexDescriptor? {
        T.indexDescriptors.first { descriptor in
            guard descriptor.kindIdentifier == "time_window_leaderboard" else {
                return false
            }
            // Check if score field matches
            return descriptor.fieldNames.contains(scoreFieldName)
        }
    }

    // MARK: - FusionQuery

    public func execute(candidates: Set<String>?) async throws -> [ScoredResult<T>] {
        guard let descriptor = findIndexDescriptor() else {
            throw FusionQueryError.indexNotFound(
                type: T.persistableType,
                field: scoreFieldName,
                kind: "leaderboard"
            )
        }

        let indexName = descriptor.name

        // Build grouping array
        var grouping: [any TupleElement]? = nil
        if let gv = groupValue {
            grouping = [gv]
        }

        // Execute leaderboard query
        let leaderboardResults = try await queryContext.executeLeaderboardSearch(
            type: T.self,
            indexName: indexName,
            k: k,
            grouping: grouping,
            windowId: windowId
        )

        // Filter to candidates if provided
        var filteredResults = leaderboardResults
        if let candidateIds = candidates {
            filteredResults = leaderboardResults.filter { result in
                candidateIds.contains("\(result.item.id)")
            }
        }

        // Convert leaderboard rank to score
        // Rank 1 (top) gets highest score, lower ranks get lower scores
        let count = Double(filteredResults.count)
        return filteredResults.enumerated().map { index, result in
            // Higher rank (lower index) = higher score
            let score = count > 1 ? 1.0 - Double(index) / (count - 1) : 1.0
            return ScoredResult(item: result.item, score: score)
        }
    }
}
