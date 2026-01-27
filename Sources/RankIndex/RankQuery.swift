// RankQuery.swift
// RankIndex - Query extension for ranking operations
//
// Follows GraphIndex pattern: execute() uses the actual index, not in-memory processing.

import Foundation
import DatabaseEngine
import Core
import FoundationDB
import Rank

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
    /// Maximum number of keys to scan for safety (prevents DoS on large indexes)
    private static var maxScanKeys: Int { 100_000 }

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
        self.queryContext = queryContext
        self.fieldName = fieldName
    }

    /// Get top N items (highest values)
    ///
    /// **Note**: Values ≤ 0 are ignored.
    ///
    /// - Parameter n: Number of items to return (must be > 0)
    /// - Returns: Updated query builder
    public func top(_ n: Int) -> Self {
        guard n > 0 else { return self }
        var copy = self
        copy.queryMode = .top(n)
        return copy
    }

    /// Get bottom N items (lowest values)
    ///
    /// **Note**: Values ≤ 0 are ignored.
    ///
    /// - Parameter n: Number of items to return (must be > 0)
    /// - Returns: Updated query builder
    public func bottom(_ n: Int) -> Self {
        guard n > 0 else { return self }
        var copy = self
        copy.queryMode = .bottom(n)
        return copy
    }

    /// Get items in a specific rank range
    ///
    /// **Note**: Invalid ranges (from < 0 or to ≤ from) are ignored.
    ///
    /// - Parameters:
    ///   - from: Start rank (0-based, inclusive)
    ///   - to: End rank (exclusive)
    /// - Returns: Updated query builder
    public func range(from: Int, to: Int) -> Self {
        guard from >= 0 && to > from else { return self }
        var copy = self
        copy.queryMode = .range(from: from, to: to)
        return copy
    }

    /// Get items at a specific percentile
    ///
    /// **Note**: Values outside 0.0-1.0 range are ignored.
    ///
    /// - Parameter p: Percentile value (0.0 to 1.0, e.g., 0.5 for median)
    /// - Returns: Updated query builder
    public func percentile(_ p: Double) -> Self {
        guard p >= 0.0 && p <= 1.0 else { return self }
        var copy = self
        copy.queryMode = .percentile(p)
        return copy
    }

    /// Execute the ranking query using the index
    ///
    /// Uses RankIndex for efficient queries:
    /// - top(k): O(n log k) with TopKHeap
    /// - bottom(k): O(n log k) with TopKHeap
    /// - range: O(n) scan with skip
    /// - percentile: O(n log k) where k = targetRank
    ///
    /// Falls back to in-memory sorting if no RankIndex exists for the field.
    ///
    /// - Returns: Array of (item, rank) tuples sorted by rank
    /// - Throws: Error if execution fails
    public func execute() async throws -> [(item: T, rank: Int)] {
        // Build index name: {TypeName}_rank_{field}
        let indexName = "\(T.persistableType)_rank_\(fieldName)"

        // Check if index exists
        guard let _ = queryContext.schema.indexDescriptor(named: indexName) else {
            // No index - fall back to in-memory processing
            return try await executeInMemory()
        }

        // Get index subspace
        let typeSubspace = try await queryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(indexName)
        let scoresSubspace = indexSubspace.subspace("scores")

        // Execute query using index
        return try await queryContext.withTransaction { transaction in
            try await self.executeWithIndex(
                scoresSubspace: scoresSubspace,
                transaction: transaction
            )
        }
    }

    /// Execute query using the rank index
    private func executeWithIndex(
        scoresSubspace: Subspace,
        transaction: any TransactionProtocol
    ) async throws -> [(item: T, rank: Int)] {
        switch queryMode {
        case .top(let k):
            return try await scanTopK(k: k, scoresSubspace: scoresSubspace, transaction: transaction)

        case .bottom(let k):
            return try await scanBottomK(k: k, scoresSubspace: scoresSubspace, transaction: transaction)

        case .range(let from, let to):
            return try await scanRange(from: from, to: to, scoresSubspace: scoresSubspace, transaction: transaction)

        case .percentile(let p):
            return try await scanPercentile(p: p, scoresSubspace: scoresSubspace, transaction: transaction)
        }
    }

    /// Scan top K items (highest scores)
    ///
    /// Index stores: [score][primaryKey] in ascending order.
    /// To get top K, we use a min-heap while scanning all entries,
    /// then sort the results in descending order.
    ///
    /// **Resource Limit**: Scans at most 100,000 keys to prevent DoS attacks.
    private func scanTopK(
        k: Int,
        scoresSubspace: Subspace,
        transaction: any TransactionProtocol
    ) async throws -> [(item: T, rank: Int)] {
        let range = scoresSubspace.range()

        // Use min-heap to track top-k highest scores
        var topKHeap = TopKHeap<(score: Double, primaryKey: Tuple)>(
            k: k,
            comparator: { $0.score < $1.score }  // Min-heap: smallest score at top
        )

        let sequence = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(range.begin),
            endSelector: .firstGreaterOrEqual(range.end),
            snapshot: true
        )

        var scannedKeys = 0
        for try await (key, _) in sequence {
            guard scoresSubspace.contains(key) else { break }

            // Resource limit
            scannedKeys += 1
            if scannedKeys >= Self.maxScanKeys { break }

            if let (score, primaryKey) = try parseIndexKey(key, scoresSubspace: scoresSubspace) {
                topKHeap.insert((score: score, primaryKey: primaryKey))
            }
        }

        // Get sorted results (highest first)
        let sortedResults = topKHeap.toSortedArrayDescending()

        // Fetch items by primary key
        return try await fetchItemsWithRank(results: sortedResults)
    }

    /// Scan bottom K items (lowest scores)
    ///
    /// Index stores scores in ascending order, so we can simply take the first K.
    /// Naturally limited to k items (no additional resource limit needed).
    private func scanBottomK(
        k: Int,
        scoresSubspace: Subspace,
        transaction: any TransactionProtocol
    ) async throws -> [(item: T, rank: Int)] {
        let range = scoresSubspace.range()

        var results: [(score: Double, primaryKey: Tuple)] = []
        results.reserveCapacity(k)

        let sequence = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(range.begin),
            endSelector: .firstGreaterOrEqual(range.end),
            snapshot: true
        )

        for try await (key, _) in sequence {
            guard scoresSubspace.contains(key) else { break }

            if let (score, primaryKey) = try parseIndexKey(key, scoresSubspace: scoresSubspace) {
                results.append((score: score, primaryKey: primaryKey))
                if results.count >= k {
                    break
                }
            }
        }

        return try await fetchItemsWithRank(results: results)
    }

    /// Scan items in a rank range
    ///
    /// **Resource Limit**: Scans at most 100,000 keys to prevent DoS attacks.
    private func scanRange(
        from: Int,
        to: Int,
        scoresSubspace: Subspace,
        transaction: any TransactionProtocol
    ) async throws -> [(item: T, rank: Int)] {
        // First, get all items to determine ranks
        // This is similar to top(to) but we skip the first `from` items
        let range = scoresSubspace.range()

        var allItems: [(score: Double, primaryKey: Tuple)] = []

        let sequence = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(range.begin),
            endSelector: .firstGreaterOrEqual(range.end),
            snapshot: true
        )

        var scannedKeys = 0
        for try await (key, _) in sequence {
            guard scoresSubspace.contains(key) else { break }

            // Resource limit
            scannedKeys += 1
            if scannedKeys >= Self.maxScanKeys { break }

            if let (score, primaryKey) = try parseIndexKey(key, scoresSubspace: scoresSubspace) {
                allItems.append((score: score, primaryKey: primaryKey))
            }
        }

        // Sort by score descending (highest first = rank 0)
        allItems.sort { $0.score > $1.score }

        // Extract range
        let rangeItems = Array(allItems.dropFirst(from).prefix(to - from))

        return try await fetchItemsWithRank(results: rangeItems, startRank: from)
    }

    /// Scan items at a specific percentile
    ///
    /// **Resource Limit**: Count limited to 100,000 keys.
    private func scanPercentile(
        p: Double,
        scoresSubspace: Subspace,
        transaction: any TransactionProtocol
    ) async throws -> [(item: T, rank: Int)] {
        // First, count total items
        let range = scoresSubspace.range()
        var totalCount = 0

        let countSequence = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(range.begin),
            endSelector: .firstGreaterOrEqual(range.end),
            snapshot: true
        )

        for try await (key, _) in countSequence {
            guard scoresSubspace.contains(key) else { break }
            totalCount += 1
            // Resource limit for count operation
            if totalCount >= Self.maxScanKeys { break }
        }

        guard totalCount > 0 else { return [] }

        // Calculate target rank
        // percentile 0.5 (median) = rank at position totalCount * 0.5
        // percentile 1.0 (100th) = rank 0 (highest score)
        // percentile 0.0 (0th) = rank totalCount - 1 (lowest score)
        let targetRank = Int(Double(totalCount) * (1.0 - p))
        let safeTargetRank = max(0, min(targetRank, totalCount - 1))

        // Get the item at targetRank using range query
        let results = try await scanRange(
            from: safeTargetRank,
            to: safeTargetRank + 1,
            scoresSubspace: scoresSubspace,
            transaction: transaction
        )

        return results
    }

    /// Parse index key to extract score and primary key
    ///
    /// Key format: [scoresSubspace][score][primaryKey...]
    private func parseIndexKey(_ key: FDB.Bytes, scoresSubspace: Subspace) throws -> (score: Double, primaryKey: Tuple)? {
        let tuple = try scoresSubspace.unpack(key)

        guard tuple.count >= 2 else {
            return nil
        }

        // First element is score
        guard let firstElement = tuple[0] else { return nil }

        guard let score = try? TypeConversion.double(from: firstElement) else {
            return nil
        }

        // Remaining elements are primary key
        var primaryKeyElements: [any TupleElement] = []
        for i in 1..<tuple.count {
            if let element = tuple[i] {
                primaryKeyElements.append(element)
            }
        }

        return (score: score, primaryKey: Tuple(primaryKeyElements))
    }

    /// Fetch items by primary key and add rank
    private func fetchItemsWithRank(
        results: [(score: Double, primaryKey: Tuple)],
        startRank: Int = 0
    ) async throws -> [(item: T, rank: Int)] {
        let ids = results.map { $0.primaryKey }
        let items = try await queryContext.fetchItems(ids: ids, type: T.self)

        // Combine items with ranks
        return items.enumerated().map { (item: $0.element, rank: startRank + $0.offset) }
    }

    /// Execute using in-memory calculation (fallback when no index exists)
    private func executeInMemory() async throws -> [(item: T, rank: Int)] {
        let items = try await queryContext.context.fetch(T.self).execute()

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

    /// Invalid percentile value
    case invalidPercentile(Double)

    /// Index not found
    case indexNotFound(String)

    public var description: String {
        switch self {
        case .noRankingField:
            return "No ranking field specified for rank query"
        case .invalidPercentile(let p):
            return "Invalid percentile value: \(p). Must be between 0.0 and 1.0"
        case .indexNotFound(let name):
            return "Rank index not found: \(name)"
        }
    }
}
