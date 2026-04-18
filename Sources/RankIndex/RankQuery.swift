// RankQuery.swift
// RankIndex - Query extension for ranking operations
//
// Follows GraphIndex pattern: execute() uses the actual index, not in-memory processing.

import Foundation
import DatabaseEngine
import Core
import QueryIR
import DatabaseClientProtocol
import StorageKit
import Rank

private enum RankQueryRuntime {
    static let registration: Void = {
        RankReadBridge.registerReadExecutors()
    }()

    static func ensureRegistered() {
        _ = registration
    }
}

// MARK: - Rank Query Builder

/// Builder for ranking queries (leaderboards, top-N, percentiles)
///
/// Uses the RankIndex for efficient O(k) or O(log n) queries. If no RankIndex exists
/// for the specified field, falls back to in-memory sorting (O(n log n)).
///
/// **Usage**:
/// ```swift
/// import RankIndex
///
/// let leaderboard = try await context.rank(Player.self)
///     .by(\.score)
///     .top(100)
///     .execute()
/// // Returns: [(item: Player, rank: Int)]
///
/// let median = try await context.rank(Player.self)
///     .by(\.score)
///     .percentile(0.5)
///     .executeOne()
/// // Returns: Player?
/// ```
public struct RankQueryBuilder<T: Persistable>: Sendable {
    private let queryContext: IndexQueryContext
    private let fieldName: String
    private var queryMode: RankQueryMode = .top(10)

    /// Query mode for ranking
    internal enum RankQueryMode: Sendable {
        case top(Int)
        case bottom(Int)
        case range(from: Int, to: Int)
        case percentile(Double)
    }

    internal init(queryContext: IndexQueryContext, fieldName: String) {
        RankQueryRuntime.ensureRegistered()
        self.queryContext = queryContext
        self.fieldName = fieldName
    }

    /// Get top N items (highest values).
    ///
    /// Invalid `n` (≤ 0) is stored as-is; validation is deferred to execute
    /// time so callers receive an error rather than a silently swapped-out mode.
    ///
    /// - Parameter n: Number of items to return (must be > 0 at execute time)
    /// - Returns: Updated query builder
    public func top(_ n: Int) -> Self {
        var copy = self
        copy.queryMode = .top(n)
        return copy
    }

    /// Get bottom N items (lowest values).
    ///
    /// Validation is deferred to execute time; see `top(_:)`.
    ///
    /// - Parameter n: Number of items to return (must be > 0 at execute time)
    /// - Returns: Updated query builder
    public func bottom(_ n: Int) -> Self {
        var copy = self
        copy.queryMode = .bottom(n)
        return copy
    }

    /// Get items in a specific rank range.
    ///
    /// Validation is deferred to execute time; see `top(_:)`.
    ///
    /// - Parameters:
    ///   - from: Start rank (0-based, inclusive; must be ≥ 0 at execute time)
    ///   - to: End rank (exclusive; must be > `from` at execute time)
    /// - Returns: Updated query builder
    public func range(from: Int, to: Int) -> Self {
        var copy = self
        copy.queryMode = .range(from: from, to: to)
        return copy
    }

    /// Get items at a specific percentile.
    ///
    /// Validation is deferred to execute time; see `top(_:)`.
    ///
    /// - Parameter p: Percentile value (must be in [0.0, 1.0] at execute time)
    /// - Returns: Updated query builder
    public func percentile(_ p: Double) -> Self {
        var copy = self
        copy.queryMode = .percentile(p)
        return copy
    }

    /// Validate the current query mode. Called at every execute entry point so
    /// invalid arguments surface as typed errors rather than silently reverting
    /// to the default mode.
    private func validateMode() throws {
        switch queryMode {
        case .top(let n), .bottom(let n):
            guard n > 0 else { throw RankQueryError.invalidCount(n) }
        case .range(let from, let to):
            guard from >= 0, to > from else {
                throw RankQueryError.invalidRange(from: from, to: to)
            }
        case .percentile(let p):
            guard p >= 0.0, p <= 1.0 else { throw RankQueryError.invalidPercentile(p) }
        }
    }

    /// Execute the ranking query using the index
    ///
    /// Uses RankIndex for efficient queries:
    /// - top(k): O(K) native FDB reverse range scan
    /// - bottom(k): O(K) native FDB forward range scan
    /// - range: O(to) reverse scan then drop first `from`
    /// - percentile: O(1) atomic count + O(targetRank) reverse scan
    ///
    /// Falls back to in-memory sorting if no RankIndex exists for the field.
    ///
    /// - Returns: Array of (item, rank) tuples sorted by rank
    /// - Throws: Error if execution fails
    public func execute() async throws -> [(item: T, rank: Int)] {
        try validateMode()

        let response = try await queryContext.context.query(
            toSelectQuery(),
            as: T.self,
            options: .default
        )

        return try response.rows.map { row in
            let item = try QueryRowCodec.decode(row, as: T.self)
            guard let rank = row.annotations["rank"]?.int64Value else {
                throw RankQueryError.invalidResponse("Missing rank annotation")
            }
            return (item: item, rank: Int(rank))
        }
    }

    internal func executeDirect(
        configuration: TransactionConfiguration = .default,
        cachePolicy: CachePolicy = .server
    ) async throws -> [(item: T, rank: Int)] {
        try validateMode()

        // Build index name: {TypeName}_rank_{field}
        let indexName = "\(T.persistableType)_rank_\(fieldName)"

        // Check if index exists
        guard let _ = queryContext.schema.indexDescriptor(named: indexName) else {
            // No index - fall back to in-memory processing
            return try await executeInMemory(cachePolicy: cachePolicy)
        }

        // Get index subspace
        let typeSubspace = try await queryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(indexName)

        // Execute query using index
        return try await queryContext.withTransaction(configuration: configuration) { transaction in
            try await self.executeWithIndex(
                indexSubspace: indexSubspace,
                transaction: transaction,
                cachePolicy: cachePolicy
            )
        }
    }

    /// Execute query using the rank index
    private func executeWithIndex(
        indexSubspace: Subspace,
        transaction: any Transaction,
        cachePolicy: CachePolicy
    ) async throws -> [(item: T, rank: Int)] {
        let scoresSubspace = indexSubspace.subspace("scores")
        let scanner = RankScanner(scoresSubspace: scoresSubspace, transaction: transaction)

        switch queryMode {
        case .top(let k):
            return try await scanTop(
                scanner: scanner,
                k: k,
                cachePolicy: cachePolicy
            )

        case .bottom(let k):
            return try await scanBottom(
                scanner: scanner,
                k: k,
                cachePolicy: cachePolicy
            )

        case .range(let from, let to):
            return try await scanRange(
                scanner: scanner,
                from: from,
                to: to,
                cachePolicy: cachePolicy
            )

        case .percentile(let p):
            return try await scanPercentile(
                scanner: scanner,
                indexSubspace: indexSubspace,
                p: p,
                transaction: transaction,
                cachePolicy: cachePolicy
            )
        }
    }

    /// Scan top K items (highest scores) using FDB reverse range scan.
    ///
    /// **Algorithm**: `collectRange(reverse: true, limit: k)` reads the K highest
    /// entries directly in O(K). No heap, no truncation, no later sort.
    private func scanTop(
        scanner: RankScanner,
        k: Int,
        cachePolicy: CachePolicy
    ) async throws -> [(item: T, rank: Int)] {
        let entries = try await scanner.top(k: k)
        return try await fetchItemsWithRank(entries: entries, startRank: 0, cachePolicy: cachePolicy)
    }

    /// Scan bottom K items (lowest scores) using FDB forward range scan.
    private func scanBottom(
        scanner: RankScanner,
        k: Int,
        cachePolicy: CachePolicy
    ) async throws -> [(item: T, rank: Int)] {
        let entries = try await scanner.bottom(k: k)
        return try await fetchItemsWithRank(entries: entries, startRank: 0, cachePolicy: cachePolicy)
    }

    /// Scan items in a rank range [from, to) using reverse scan of `to` entries.
    private func scanRange(
        scanner: RankScanner,
        from: Int,
        to: Int,
        cachePolicy: CachePolicy
    ) async throws -> [(item: T, rank: Int)] {
        let entries = try await scanner.rangeDescending(from: from, to: to)
        return try await fetchItemsWithRank(entries: entries, startRank: from, cachePolicy: cachePolicy)
    }

    /// Scan item at a specific percentile.
    ///
    /// Uses the atomic count key (`_count`) for O(1) total count, then reads the
    /// Nth-from-top entry directly.
    private func scanPercentile(
        scanner: RankScanner,
        indexSubspace: Subspace,
        p: Double,
        transaction: any Transaction,
        cachePolicy: CachePolicy
    ) async throws -> [(item: T, rank: Int)] {
        let countKey = indexSubspace.pack(Tuple("_count"))
        let countBytes = try await transaction.getValue(for: countKey, snapshot: true)
        let totalCount = countBytes.map { Int(ByteConversion.bytesToInt64($0)) } ?? 0
        guard totalCount > 0 else { return [] }

        // percentile 0.5 (median) = middle rank; 1.0 = highest; 0.0 = lowest.
        let targetRank = Int(Double(totalCount) * (1.0 - p))
        let safeTargetRank = max(0, min(targetRank, totalCount - 1))

        guard let entry = try await scanner.nthFromTop(safeTargetRank) else { return [] }
        return try await fetchItemsWithRank(
            entries: [entry],
            startRank: safeTargetRank,
            cachePolicy: cachePolicy
        )
    }

    /// Fetch items by primary key and pair each with its scan-position rank.
    ///
    /// Uses `fetchItemsPreservingOrder` so that items deleted between scan and
    /// fetch do not shift rank numbers for the remaining items. For top(10) where
    /// entries[3] is missing, results still report ranks 0, 1, 2, 4, 5, ... — not
    /// 0, 1, 2, 3, 4, ... (which would mis-label the 4th-highest as rank 3).
    private func fetchItemsWithRank(
        entries: [RankScanEntry],
        startRank: Int,
        cachePolicy: CachePolicy
    ) async throws -> [(item: T, rank: Int)] {
        let ids = entries.map { $0.primaryKey }
        let items = try await queryContext.fetchItemsPreservingOrder(
            ids: ids,
            type: T.self,
            cachePolicy: cachePolicy
        )
        var results: [(item: T, rank: Int)] = []
        results.reserveCapacity(items.count)
        for (offset, maybeItem) in items.enumerated() {
            guard let item = maybeItem else { continue }
            results.append((item: item, rank: startRank + offset))
        }
        return results
    }

    /// Execute using in-memory calculation (fallback when no index exists)
    private func executeInMemory(cachePolicy: CachePolicy) async throws -> [(item: T, rank: Int)] {
        let items = try await queryContext.context.fetch(T.self)
            .cachePolicy(cachePolicy)
            .execute()

        // Extract values and sort
        let itemsWithValues: [(item: T, value: Double)] = items.compactMap { item in
            guard let rawValue = item[dynamicMember: fieldName],
                  let numValue = TypeConversion.asDouble(rawValue) else { return nil }
            return (item: item, value: numValue)
        }

        switch queryMode {
        case .top(let n):
            let sorted = itemsWithValues.sorted { $0.value > $1.value }
            let limited = Array(sorted.prefix(n))
            return limited.enumerated().map { (item: $0.element.item, rank: $0.offset) }

        case .bottom(let n):
            let sorted = itemsWithValues.sorted { $0.value < $1.value }
            let limited = Array(sorted.prefix(n))
            return limited.enumerated().map { (item: $0.element.item, rank: $0.offset) }

        case .range(let from, let to):
            let sorted = itemsWithValues.sorted { $0.value > $1.value }
            let rangeItems = Array(sorted.dropFirst(from).prefix(to - from))
            return rangeItems.enumerated().map { (item: $0.element.item, rank: from + $0.offset) }

        case .percentile(let p):
            guard !itemsWithValues.isEmpty else { return [] }
            let sorted = itemsWithValues.sorted { $0.value > $1.value }
            let targetRank = Int(Double(sorted.count) * (1.0 - p))
            if targetRank < sorted.count {
                let item = sorted[targetRank]
                return [(item: item.item, rank: targetRank)]
            }
            return []
        }
    }

    internal func toSelectQuery() -> SelectQuery {
        var parameters: [String: QueryParameterValue] = [
            RankReadParameter.fieldName: .string(fieldName)
        ]

        let limit: Int?
        switch queryMode {
        case .top(let count):
            parameters[RankReadParameter.mode] = .string(RankReadParameter.topMode)
            parameters[RankReadParameter.count] = .int(Int64(count))
            limit = count
        case .bottom(let count):
            parameters[RankReadParameter.mode] = .string(RankReadParameter.bottomMode)
            parameters[RankReadParameter.count] = .int(Int64(count))
            limit = count
        case .range(let from, let to):
            parameters[RankReadParameter.mode] = .string(RankReadParameter.rangeMode)
            parameters[RankReadParameter.from] = .int(Int64(from))
            parameters[RankReadParameter.to] = .int(Int64(to))
            limit = max(to - from, 0)
        case .percentile(let percentile):
            parameters[RankReadParameter.mode] = .string(RankReadParameter.percentileMode)
            parameters[RankReadParameter.percentile] = .double(percentile)
            limit = 1
        }

        return SelectQuery(
            projection: .all,
            source: .table(TableRef(table: T.persistableType)),
            accessPath: .index(
                IndexScanSource(
                    indexName: "\(T.persistableType)_rank_\(fieldName)",
                    kindIdentifier: RankIndexKind<T, Int64>.identifier,
                    parameters: parameters
                )
            ),
            limit: limit
        )
    }

    /// Execute and return a single item (useful for percentile queries)
    ///
    /// - Returns: The item at the requested position, or nil if not found
    /// - Throws: Error if execution fails
    public func executeOne() async throws -> T? {
        let results = try await execute()
        return results.first?.item
    }

}

// MARK: - Rank Entry Point

/// Entry point for ranking queries
public struct RankEntryPoint<T: Persistable>: Sendable {
    private let queryContext: IndexQueryContext

    internal init(queryContext: IndexQueryContext) {
        self.queryContext = queryContext
    }

    /// Specify the field to rank by
    ///
    /// - Parameter keyPath: KeyPath to the comparable field
    /// - Returns: Rank query builder
    public func by<V: Comparable>(_ keyPath: KeyPath<T, V>) -> RankQueryBuilder<T> {
        RankQueryBuilder(
            queryContext: queryContext,
            fieldName: T.fieldName(for: keyPath)
        )
    }
}

// MARK: - FDBContext Extension

extension FDBContext {

    /// Start a ranking query
    ///
    /// This method is available when you import `RankIndex`.
    ///
    /// **Usage**:
    /// ```swift
    /// import RankIndex
    ///
    /// let leaderboard = try await context.rank(Player.self)
    ///     .by(\.score)
    ///     .top(100)
    ///     .execute()
    /// // Returns: [(item: Player, rank: Int)]
    ///
    /// let median = try await context.rank(Player.self)
    ///     .by(\.score)
    ///     .percentile(0.5)
    ///     .executeOne()
    /// // Returns: Player?
    /// ```
    ///
    /// - Parameter type: The Persistable type to rank
    /// - Returns: Entry point for configuring the ranking
    public func rank<T: Persistable>(_ type: T.Type) -> RankEntryPoint<T> {
        RankEntryPoint(queryContext: indexQueryContext)
    }
}

// MARK: - Rank Query Error

/// Errors for ranking query operations
public enum RankQueryError: Error, CustomStringConvertible {
    /// No ranking field specified
    case noRankingField

    /// Invalid count for top/bottom (must be > 0)
    case invalidCount(Int)

    /// Invalid rank range (must satisfy `from >= 0 && to > from`)
    case invalidRange(from: Int, to: Int)

    /// Invalid percentile value
    case invalidPercentile(Double)

    /// Index not found
    case indexNotFound(String)

    /// Canonical query response is missing required metadata
    case invalidResponse(String)

    public var description: String {
        switch self {
        case .noRankingField:
            return "No ranking field specified for rank query"
        case .invalidCount(let n):
            return "Invalid count: \(n). top/bottom require a positive count"
        case .invalidRange(let from, let to):
            return "Invalid rank range: from=\(from), to=\(to). Require from >= 0 and to > from"
        case .invalidPercentile(let p):
            return "Invalid percentile value: \(p). Must be between 0.0 and 1.0"
        case .indexNotFound(let name):
            return "Rank index not found: \(name)"
        case .invalidResponse(let reason):
            return "Invalid rank query response: \(reason)"
        }
    }
}
