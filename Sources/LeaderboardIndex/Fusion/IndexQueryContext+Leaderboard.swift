// IndexQueryContext+Leaderboard.swift
// LeaderboardIndex - IndexQueryContext extension for Leaderboard queries

import Foundation
import Core
import DatabaseEngine
import FoundationDB

extension IndexQueryContext {
    /// Create a Leaderboard query for ranking
    ///
    /// **Usage**:
    /// ```swift
    /// let results = try await context.fuse(GameScore.self) {
    ///     context.indexQueryContext.leaderboard(GameScore.self, \.score).topK(100)
    /// }
    /// .execute()
    /// ```
    public func leaderboard<T: Persistable>(
        _ type: T.Type,
        _ scoreKeyPath: KeyPath<T, Int64>
    ) -> Leaderboard<T> {
        Leaderboard(scoreKeyPath, context: self)
    }

    /// Create a Leaderboard query with grouping
    public func leaderboard<T: Persistable, G: Sendable & Hashable>(
        _ type: T.Type,
        _ scoreKeyPath: KeyPath<T, Int64>,
        groupBy groupKeyPath: KeyPath<T, G>
    ) -> Leaderboard<T> {
        Leaderboard(scoreKeyPath, groupBy: groupKeyPath, context: self)
    }

    /// Execute leaderboard search using the index
    ///
    /// - Parameters:
    ///   - type: The Persistable type
    ///   - indexName: Name of the leaderboard index
    ///   - k: Number of top entries to return
    ///   - grouping: Optional grouping filter
    ///   - windowId: Optional specific window ID (nil = current window)
    /// - Returns: Array of items with their leaderboard scores
    public func executeLeaderboardSearch<T: Persistable>(
        type: T.Type,
        indexName: String,
        k: Int,
        grouping: [any TupleElement]?,
        windowId: Int64?
    ) async throws -> [(item: T, score: Int64)] {
        guard let index = schema.index(named: indexName) else {
            throw FusionQueryError.indexNotFound(
                type: T.persistableType,
                field: indexName,
                kind: "leaderboard"
            )
        }

        // Get the maintainer - we need to handle the generic Score type
        // For simplicity, we use Int64 as the common score type since that's what
        // TimeWindowLeaderboardIndexMaintainer stores internally
        guard let maintainer = try await indexMaintainerProvider.maintainer(
            for: index,
            type: T.self
        ) as? TimeWindowLeaderboardIndexMaintainer<T, Int64> else {
            throw FusionQueryError.invalidConfiguration(
                "Could not create TimeWindowLeaderboardIndexMaintainer for \(indexName)"
            )
        }

        return try await database.withTransaction { transaction in
            // Get top K from leaderboard
            let topK: [(pk: Tuple, score: Int64)]

            if let wid = windowId {
                topK = try await maintainer.getTopK(
                    k: k,
                    windowId: wid,
                    grouping: grouping,
                    transaction: transaction
                )
            } else {
                topK = try await maintainer.getTopK(
                    k: k,
                    grouping: grouping,
                    transaction: transaction
                )
            }

            // Fetch items by primary keys
            var results: [(item: T, score: Int64)] = []
            for (pk, score) in topK {
                if let item: T = try await self.fetchItemByPK(pk: pk, type: T.self, transaction: transaction) {
                    results.append((item: item, score: score))
                }
            }

            return results
        }
    }

    /// Fetch a single item by primary key tuple
    private func fetchItemByPK<T: Persistable>(
        pk: Tuple,
        type: T.Type,
        transaction: any TransactionProtocol
    ) async throws -> T? {
        let recordSubspace = subspace
            .subspace(SubspaceKey.items.rawValue)
            .subspace(T.persistableType)
        let recordKey = recordSubspace.pack(pk)

        guard let data = try await transaction.getValue(for: recordKey) else {
            return nil
        }

        return try serializer.deserialize(data, as: T.self)
    }
}
