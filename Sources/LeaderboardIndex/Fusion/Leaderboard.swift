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
///     Leaderboard(\.score).top(100)
///
///     // Combine with user preferences
///     Similar(\.playerProfile, dimensions: 128).nearest(to: userVector, k: 50)
/// }
/// .algorithm(.rrf())
/// .execute()
///
/// // With grouping (e.g., by region)
/// let results = try await context.fuse(GameScore.self) {
///     Leaderboard(\.score, groupBy: \.region).top(50).group("asia")
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
    ///     Leaderboard(\.score).top(100)
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
    /// Leaderboard(\.score, groupBy: \.region).top(50).group("asia")
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
    public func top(_ count: Int) -> Self {
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
            // Use type-safe identifier from TimeWindowLeaderboardIndexKind
            guard descriptor.kindIdentifier == TimeWindowLeaderboardIndexKind<T, Int64>.identifier else {
                return false
            }
            // Check if score field matches via kind's fieldNames
            guard let kind = descriptor.kind as? TimeWindowLeaderboardIndexKind<T, Int64> else {
                return false
            }
            return kind.fieldNames.contains(scoreFieldName)
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

        // Get index subspace using public API
        let typeSubspace = try await queryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(indexName)

        // Execute leaderboard query within transaction
        let topKResults: [(pk: Tuple, score: Int64)] = try await queryContext.withTransaction { transaction in
            try await self.readTopK(
                indexSubspace: indexSubspace,
                k: self.k,
                grouping: self.groupValue.map { [$0] },
                windowId: self.windowId,
                transaction: transaction
            )
        }

        // Fetch items by primary keys
        var items = try await queryContext.fetchItems(ids: topKResults.map(\.pk), type: T.self)

        // Filter to candidates if provided
        if let candidateIds = candidates {
            items = items.filter { candidateIds.contains("\($0.id)") }
        }

        // Match items with their leaderboard scores
        var leaderboardResults: [(item: T, score: Int64)] = []
        for item in items {
            // Find matching pk in topKResults
            for result in topKResults {
                if let pkId = result.pk[0] as? String, "\(item.id)" == pkId {
                    leaderboardResults.append((item: item, score: result.score))
                    break
                } else if let pkId = result.pk[0] as? Int64, "\(item.id)" == "\(pkId)" {
                    leaderboardResults.append((item: item, score: result.score))
                    break
                }
            }
        }

        // Sort by leaderboard score descending
        leaderboardResults.sort { $0.score > $1.score }

        // Convert leaderboard rank to fusion score
        // Rank 1 (top) gets highest score, lower ranks get lower scores
        let count = Double(leaderboardResults.count)
        return leaderboardResults.enumerated().map { index, result in
            // Higher rank (lower index) = higher score
            let score = count > 1 ? 1.0 - Double(index) / (count - 1) : 1.0
            return ScoredResult(item: result.item, score: score)
        }
    }

    // MARK: - Leaderboard Index Reading

    /// Read top K entries from leaderboard index
    ///
    /// Index structure:
    /// - `[indexSubspace]["window"][windowId][groupKey...][invertedScore][primaryKey]` -> empty
    ///
    /// Score inversion: `invertedScore = Int64.max - score` for descending order
    private func readTopK(
        indexSubspace: Subspace,
        k: Int,
        grouping: [any TupleElement]?,
        windowId: Int64?,
        transaction: any TransactionProtocol
    ) async throws -> [(pk: Tuple, score: Int64)] {
        // Calculate current window ID if not specified
        let effectiveWindowId = windowId ?? {
            // Default window duration for daily (most common)
            // Actual window type would be in the index kind, but we use a reasonable default
            let now = Date()
            let timestamp = Int64(now.timeIntervalSince1970)
            return timestamp / 86400  // Daily window (24 * 60 * 60)
        }()

        let windowSubspace = indexSubspace.subspace("window")

        // Build range prefix
        var prefixElements: [any TupleElement] = [effectiveWindowId]
        if let g = grouping {
            prefixElements.append(contentsOf: g)
        }

        let rangeStart = windowSubspace.pack(Tuple(prefixElements))
        let rangeEnd = windowSubspace.pack(Tuple(prefixElements + [Int64.max]))

        let sequence = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(rangeStart),
            endSelector: .firstGreaterOrEqual(rangeEnd),
            snapshot: true
        )

        var results: [(pk: Tuple, score: Int64)] = []
        var count = 0
        let groupingCount = grouping?.count ?? 0

        for try await (key, _) in sequence {
            guard windowSubspace.contains(key), count < k else { break }

            let keyTuple = try windowSubspace.unpack(key)

            // Extract inverted score and primary key
            // Key structure: windowId, [grouping...], invertedScore, [pk...]
            let invertedScoreIndex = 1 + groupingCount

            guard let invertedScore = keyTuple[invertedScoreIndex] as? Int64 else {
                continue
            }

            // Reverse the inversion (same formula is self-inverse)
            let unsigned = UInt64(bitPattern: invertedScore)
            let score = Int64(bitPattern: UInt64.max - unsigned)

            // Extract primary key (remaining elements)
            var pkElements: [any TupleElement] = []
            for i in (invertedScoreIndex + 1)..<keyTuple.count {
                if let elem = keyTuple[i] {
                    pkElements.append(elem)
                }
            }

            results.append((pk: Tuple(pkElements), score: score))
            count += 1
        }

        return results
    }
}
