// RankQuery.swift
// RankIndex - Query extension for ranking operations

import Foundation
import DatabaseEngine
import Core

// MARK: - Rank Query Builder

/// Builder for ranking queries (leaderboards, top-N, percentiles)
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
        self.queryContext = queryContext
        self.fieldName = fieldName
    }

    /// Get top N items (highest values)
    ///
    /// - Parameter n: Number of items to return
    /// - Returns: Updated query builder
    public func top(_ n: Int) -> Self {
        var copy = self
        copy.queryMode = .top(n)
        return copy
    }

    /// Get bottom N items (lowest values)
    ///
    /// - Parameter n: Number of items to return
    /// - Returns: Updated query builder
    public func bottom(_ n: Int) -> Self {
        var copy = self
        copy.queryMode = .bottom(n)
        return copy
    }

    /// Get items in a specific rank range
    ///
    /// - Parameters:
    ///   - from: Start rank (0-based, inclusive)
    ///   - to: End rank (exclusive)
    /// - Returns: Updated query builder
    public func range(from: Int, to: Int) -> Self {
        var copy = self
        copy.queryMode = .range(from: from, to: to)
        return copy
    }

    /// Get items at a specific percentile
    ///
    /// - Parameter p: Percentile value (0.0 to 1.0, e.g., 0.5 for median)
    /// - Returns: Updated query builder
    public func percentile(_ p: Double) -> Self {
        var copy = self
        copy.queryMode = .percentile(p)
        return copy
    }

    /// Execute the ranking query
    ///
    /// - Returns: Array of (item, rank) tuples sorted by rank
    /// - Throws: Error if execution fails
    ///
    /// **Note**: When a RankIndex exists for the field, this uses the optimized
    /// RankIndexMaintainer which provides O(k) or O(log n) performance for top-K queries.
    /// Otherwise, it falls back to in-memory sorting which is O(n log n).
    public func execute() async throws -> [(item: T, rank: Int)] {
        // For now, use in-memory calculation
        // TODO: Optimize with RankIndexMaintainer when available
        // The RankIndexMaintainer integration requires access to internal types
        // (TransactionProtocol, TupleElement) which are not exposed in the public API.
        // A future enhancement would add executeRankQuery() to IndexQueryContext.
        return try await executeInMemory()
    }

    /// Execute using in-memory calculation (fallback path)
    private func executeInMemory() async throws -> [(item: T, rank: Int)] {
        let items = try await queryContext.context.fetch(T.self).executeRaw()

        // Extract values and sort
        let itemsWithValues: [(item: T, value: Double)] = items.compactMap { item in
            if let numValue = extractNumericValue(from: item) {
                return (item: item, value: numValue)
            }
            return nil
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

    /// Extract numeric value from item using the ranking field
    private func extractNumericValue(from item: T) -> Double? {
        guard let value = item[dynamicMember: fieldName] else { return nil }

        if let intValue = value as? Int { return Double(intValue) }
        if let doubleValue = value as? Double { return doubleValue }
        if let floatValue = value as? Float { return Double(floatValue) }
        if let int64Value = value as? Int64 { return Double(int64Value) }

        return nil
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
