// LeaderboardQuery.swift
// LeaderboardIndex - Query extension for time-windowed leaderboard indexes
//
// Provides FDBContext extension and query builder for leaderboard operations.

import Foundation
import Core
import DatabaseEngine
import FoundationDB

// MARK: - Leaderboard Entry Point

/// Entry point for leaderboard queries
///
/// **Usage**:
/// ```swift
/// import LeaderboardIndex
///
/// // Get top 10 scores
/// let topPlayers = try await context.leaderboard(GameScore.self)
///     .index(\.score)
///     .top(10)
///     .execute()
///
/// // Get player's rank
/// let rank = try await context.leaderboard(GameScore.self)
///     .index(\.score)
///     .rank(for: playerId)
///
/// // Get top scores for a specific group
/// let groupTop = try await context.leaderboard(GameScore.self)
///     .index(\.score)
///     .group(by: ["region", "weekly"])
///     .top(10)
///     .execute()
/// ```
public struct LeaderboardEntryPoint<T: Persistable>: Sendable {
    private let queryContext: IndexQueryContext

    internal init(queryContext: IndexQueryContext) {
        self.queryContext = queryContext
    }

    /// Specify the leaderboard index by score field
    ///
    /// - Parameter keyPath: KeyPath to the score field
    /// - Returns: Leaderboard query builder
    public func index<Score: Comparable & Numeric & Codable & Sendable>(
        _ keyPath: KeyPath<T, Score>
    ) -> LeaderboardQueryBuilder<T, Score> {
        LeaderboardQueryBuilder(
            queryContext: queryContext,
            scoreFieldName: T.fieldName(for: keyPath)
        )
    }
}

// MARK: - Leaderboard Query Builder

/// Builder for leaderboard index queries
///
/// Supports time-windowed ranking queries with grouping.
public struct LeaderboardQueryBuilder<T: Persistable, Score: Comparable & Numeric & Codable & Sendable>: Sendable {
    // MARK: - Properties

    private let queryContext: IndexQueryContext
    private let scoreFieldName: String
    private var groupingValues: [any TupleElement & Sendable]?
    private var windowId: Int64?
    private var topK: Int = 10

    // MARK: - Initialization

    internal init(queryContext: IndexQueryContext, scoreFieldName: String) {
        self.queryContext = queryContext
        self.scoreFieldName = scoreFieldName
    }

    // MARK: - Configuration Methods

    /// Set grouping filter
    ///
    /// - Parameter values: Grouping values to filter by
    /// - Returns: Updated query builder
    public func group(by values: [any TupleElement & Sendable]) -> Self {
        var copy = self
        copy.groupingValues = values
        return copy
    }

    /// Query a specific historical window
    ///
    /// - Parameter windowId: Window ID to query
    /// - Returns: Updated query builder
    public func window(_ windowId: Int64) -> Self {
        var copy = self
        copy.windowId = windowId
        return copy
    }

    /// Set number of top entries to return
    ///
    /// - Parameter k: Number of entries
    /// - Returns: Updated query builder
    public func top(_ k: Int) -> Self {
        var copy = self
        copy.topK = k
        return copy
    }

    // MARK: - Execution

    /// Execute the query and return top K entries with scores
    ///
    /// - Returns: Array of (item, score) tuples sorted by score descending
    public func execute() async throws -> [(item: T, score: Int64)] {
        let indexName = buildIndexName()
        let typeSubspace = try await queryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(indexName)

        let results: [(pk: Tuple, score: Int64)] = try await queryContext.withTransaction { transaction in
            let maintainer = self.createMaintainer(indexSubspace: indexSubspace)

            let grouping = self.groupingValues?.map { $0 as any TupleElement }

            if let wid = self.windowId {
                return try await maintainer.getTopK(
                    k: self.topK,
                    windowId: wid,
                    grouping: grouping,
                    transaction: transaction
                )
            } else {
                return try await maintainer.getTopK(
                    k: self.topK,
                    grouping: grouping,
                    transaction: transaction
                )
            }
        }

        // Fetch items
        let ids = results.map { $0.pk }
        let items = try await queryContext.fetchItems(ids: ids, type: T.self)

        // Match items with scores
        var finalResults: [(item: T, score: Int64)] = []
        for result in results {
            let pkBytes = result.pk.pack()
            for item in items {
                if let itemId = item.id as? any TupleElement {
                    let itemPkBytes = Tuple(itemId).pack()
                    if pkBytes == itemPkBytes {
                        finalResults.append((item: item, score: result.score))
                        break
                    }
                }
            }
        }

        return finalResults
    }

    /// Get rank for a specific item
    ///
    /// - Parameter id: The item's ID
    /// - Returns: Rank (1-based) or nil if not found
    public func rank<ID: TupleElement>(for id: ID) async throws -> Int? {
        let indexName = buildIndexName()
        let typeSubspace = try await queryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(indexName)

        return try await queryContext.withTransaction { transaction in
            let maintainer = self.createMaintainer(indexSubspace: indexSubspace)
            let grouping = self.groupingValues?.map { $0 as any TupleElement }

            return try await maintainer.getRank(
                pk: Tuple(id),
                grouping: grouping,
                transaction: transaction
            )
        }
    }

    /// Get available window IDs
    ///
    /// - Returns: Array of window IDs (newest first)
    public func availableWindows() async throws -> [Int64] {
        let indexName = buildIndexName()
        let typeSubspace = try await queryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(indexName)

        return try await queryContext.withTransaction { transaction in
            let maintainer = self.createMaintainer(indexSubspace: indexSubspace)
            return try await maintainer.getAvailableWindows(transaction: transaction)
        }
    }

    // MARK: - Private Methods

    private func buildIndexName() -> String {
        "\(T.persistableType)_leaderboard_\(scoreFieldName)"
    }

    private func createMaintainer(indexSubspace: Subspace) -> TimeWindowLeaderboardIndexMaintainer<T, Int64> {
        // Get index descriptor to retrieve window configuration
        let indexName = buildIndexName()

        // Default window configuration
        let window: LeaderboardWindowType = .daily
        let windowCount: Int = 7

        return TimeWindowLeaderboardIndexMaintainer<T, Int64>(
            index: Index(
                name: indexName,
                kind: TimeWindowLeaderboardIndexKind<T, Int64>(
                    scoreFieldName: scoreFieldName,
                    scoreTypeName: "Int64",
                    groupByFieldNames: [],
                    window: window,
                    windowCount: windowCount
                ),
                rootExpression: FieldKeyExpression(fieldName: scoreFieldName),
                keyPaths: []
            ),
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            window: window,
            windowCount: windowCount
        )
    }
}

// MARK: - FDBContext Extension

extension FDBContext {
    /// Start a leaderboard query
    ///
    /// This method is available when you import `LeaderboardIndex`.
    ///
    /// **Usage**:
    /// ```swift
    /// import LeaderboardIndex
    ///
    /// // Get top 10 players
    /// let top = try await context.leaderboard(GameScore.self)
    ///     .index(\.score)
    ///     .top(10)
    ///     .execute()
    ///
    /// // Get player's rank
    /// let rank = try await context.leaderboard(GameScore.self)
    ///     .index(\.score)
    ///     .rank(for: playerId)
    /// ```
    ///
    /// - Parameter type: The Persistable type to query
    /// - Returns: Entry point for configuring the leaderboard query
    public func leaderboard<T: Persistable>(_ type: T.Type) -> LeaderboardEntryPoint<T> {
        LeaderboardEntryPoint(queryContext: indexQueryContext)
    }
}

// MARK: - Leaderboard Query Error

/// Errors for leaderboard query operations
public enum LeaderboardQueryError: Error, CustomStringConvertible {
    /// Index not found
    case indexNotFound(String)

    /// Invalid configuration
    case invalidConfiguration(String)

    public var description: String {
        switch self {
        case .indexNotFound(let name):
            return "Leaderboard index not found: \(name)"
        case .invalidConfiguration(let reason):
            return "Invalid leaderboard configuration: \(reason)"
        }
    }
}
