// LeaderboardQuery.swift
// LeaderboardIndex - Query extension for time-windowed leaderboard indexes
//
// Provides DatabaseContext extension and query builder for leaderboard operations.

import DatabaseKit
import DatabaseTypes
import DatabaseEngine
import StorageKit

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
    /// - Parameter field: Compiled score field identity
    /// - Returns: Leaderboard query builder
    public func index(
        _ field: Field<T, Int64>
    ) -> LeaderboardQueryBuilder<T> {
        LeaderboardQueryBuilder(
            queryContext: queryContext,
            scoreFieldName: field.name
        )
    }
}

// MARK: - Leaderboard Query Builder

/// Builder for leaderboard index queries
///
/// Supports time-windowed ranking queries with grouping.
public struct LeaderboardQueryBuilder<T: Persistable>: Sendable {
    // MARK: - Properties

    private let queryContext: IndexQueryContext
    private let scoreFieldName: String
    private var groupingValues: [FieldValue]?
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
    public func group(by values: [FieldValue]) -> Self {
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

    /// Set number of bottom entries to return
    ///
    /// - Parameter k: Number of entries
    /// - Returns: Updated query builder
    public func bottom(_ k: Int) -> Self {
        var copy = self
        copy.topK = k
        return copy
    }

    // MARK: - Execution

    /// Execute the query and return top K entries with scores
    ///
    /// - Returns: Array of (item, score) tuples sorted by score descending
    public func execute() async throws -> [(item: T, score: Int64)] {
        let (descriptor, indexKind) = try resolveIndexDescriptorAndKind()
        let typeSubspace = try await queryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(descriptor.name)

        let results: [(pk: Tuple, score: Int64)] = try await queryContext.withTransaction { transaction in
            let maintainer = self.createMaintainer(
                indexSubspace: indexSubspace,
                descriptor: descriptor,
                indexKind: indexKind
            )

            let grouping = try self.groupingTupleElements()

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
                let itemPKBytes = try item.persistableIdentifierTuple().pack()
                if pkBytes == itemPKBytes {
                    finalResults.append((item: item, score: result.score))
                    break
                }
            }
        }

        return finalResults
    }

    /// Execute the query and return bottom K entries with scores
    ///
    /// - Returns: Array of (item, score) tuples sorted by score ascending
    public func executeBottom() async throws -> [(item: T, score: Int64)] {
        let (descriptor, indexKind) = try resolveIndexDescriptorAndKind()
        let typeSubspace = try await queryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(descriptor.name)

        let results: [(pk: Tuple, score: Int64)] = try await queryContext.withTransaction { transaction in
            let maintainer = self.createMaintainer(
                indexSubspace: indexSubspace,
                descriptor: descriptor,
                indexKind: indexKind
            )

            let grouping = try self.groupingTupleElements()

            if let wid = self.windowId {
                return try await maintainer.getBottomK(
                    k: self.topK,
                    windowId: wid,
                    grouping: grouping,
                    transaction: transaction
                )
            } else {
                return try await maintainer.getBottomK(
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
                let itemPKBytes = try item.persistableIdentifierTuple().pack()
                if pkBytes == itemPKBytes {
                    finalResults.append((item: item, score: result.score))
                    break
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
        let (descriptor, indexKind) = try resolveIndexDescriptorAndKind()
        let typeSubspace = try await queryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(descriptor.name)

        return try await queryContext.withTransaction { transaction in
            let maintainer = self.createMaintainer(
                indexSubspace: indexSubspace,
                descriptor: descriptor,
                indexKind: indexKind
            )
            let grouping = try self.groupingTupleElements()

            return try await maintainer.getRank(
                pk: Tuple(id),
                grouping: grouping,
                transaction: transaction
            )
        }
    }

    /// Get dense rank for a specific item
    ///
    /// Dense ranking counts unique scores higher than the target.
    /// Ties receive the same rank, but the next rank is incremented by 1.
    ///
    /// **Example**:
    /// Scores: [100, 90, 90, 80, 70]
    /// - Score 100: Dense rank = 1
    /// - Score 90: Dense rank = 2 (both players with 90 share this rank)
    /// - Score 80: Dense rank = 3 (not 4)
    /// - Score 70: Dense rank = 4
    ///
    /// - Parameter id: The item's ID
    /// - Returns: Dense rank (1-based) or nil if not found
    public func denseRank<ID: TupleElement>(for id: ID) async throws -> Int? {
        let (descriptor, indexKind) = try resolveIndexDescriptorAndKind()
        let typeSubspace = try await queryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(descriptor.name)

        return try await queryContext.withTransaction { transaction in
            let maintainer = self.createMaintainer(
                indexSubspace: indexSubspace,
                descriptor: descriptor,
                indexKind: indexKind
            )
            let grouping = try self.groupingTupleElements()

            return try await maintainer.getRankDense(
                pk: Tuple(id),
                grouping: grouping,
                transaction: transaction
            )
        }
    }

    /// Get score at a given percentile
    ///
    /// **Time Complexity**: O(n) where n is the total number of entries
    ///
    /// **Percentile Calculation**: Uses the "exclusive" method where
    /// percentile p returns the score where approximately p% of scores are below it.
    ///
    /// - Parameter percentile: Percentile value between 0.0 and 1.0 (e.g., 0.5 for median)
    /// - Returns: Score at the given percentile, or nil if no entries
    public func percentile(_ percentile: Double) async throws -> Int64? {
        let (descriptor, indexKind) = try resolveIndexDescriptorAndKind()
        let typeSubspace = try await queryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(descriptor.name)

        return try await queryContext.withTransaction { transaction in
            let maintainer = self.createMaintainer(
                indexSubspace: indexSubspace,
                descriptor: descriptor,
                indexKind: indexKind
            )
            let grouping = try self.groupingTupleElements()

            if let wid = self.windowId {
                return try await maintainer.getPercentile(
                    percentile,
                    windowId: wid,
                    grouping: grouping,
                    transaction: transaction
                )
            } else {
                return try await maintainer.getPercentile(
                    percentile,
                    grouping: grouping,
                    transaction: transaction
                )
            }
        }
    }

    /// Get available window IDs
    ///
    /// - Returns: Array of window IDs (newest first)
    public func availableWindows() async throws -> [Int64] {
        let (descriptor, indexKind) = try resolveIndexDescriptorAndKind()
        let typeSubspace = try await queryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(descriptor.name)

        return try await queryContext.withTransaction { transaction in
            let maintainer = self.createMaintainer(
                indexSubspace: indexSubspace,
                descriptor: descriptor,
                indexKind: indexKind
            )
            return try await maintainer.getAvailableWindows(transaction: transaction)
        }
    }

    // MARK: - Private Methods

    private func resolveIndexDescriptorAndKind() throws -> (
        descriptor: IndexDescriptor,
        configuration: TimeWindowLeaderboardConfiguration
    ) {
        guard let result = try findIndexDescriptorAndKind() else {
            throw LeaderboardQueryError.indexNotFound("\(T.persistableType).\(scoreFieldName)")
        }
        return result
    }

    private func findIndexDescriptorAndKind() throws -> (
        descriptor: IndexDescriptor,
        configuration: TimeWindowLeaderboardConfiguration
    )? {
        for descriptor in try T.indexDescriptors {
            guard descriptor.kind.identifier == "time_window_leaderboard" else {
                continue
            }
            let configuration = try TimeWindowLeaderboardConfiguration(
                metadata: descriptor.kind
            )
            if configuration.scoreFieldName == scoreFieldName {
                return (descriptor, configuration)
            }
        }
        return nil
    }

    private func createMaintainer(
        indexSubspace: Subspace,
        descriptor: IndexDescriptor,
        indexKind: TimeWindowLeaderboardConfiguration
    ) -> TimeWindowLeaderboardIndexMaintainer<T> {
        return TimeWindowLeaderboardIndexMaintainer<T>(
            index: Index(
                name: descriptor.name,
                kind: descriptor.kind,
                rootExpression: FieldKeyExpression(fieldName: scoreFieldName),
                subspaceKey: descriptor.name
            ),
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            window: indexKind.window,
            windowCount: indexKind.windowCount,
            wallClock: queryContext.context.container.wallClock
        )
    }

    private func groupingTupleElements() throws -> [any TupleElement]? {
        guard let groupingValues else {
            return nil
        }
        return try FieldValue.toTupleElements(groupingValues)
    }
}

// MARK: - DatabaseContext Extension

extension DatabaseContext {
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
