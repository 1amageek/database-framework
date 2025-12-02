// RankQuery.swift
// Query DSL - Rank-based queries (top-N, percentile, etc.)

import Core

// MARK: - Rank Query

/// A query for rank-based operations
///
/// **Usage**:
/// ```swift
/// // Get top 10 players by score
/// let topPlayers = try await context.fetch(Player.self)
///     .where(\.isActive == true)
///     .top(10, by: \.score)
///     .execute()
///
/// // Get bottom 5 by revenue
/// let lowest = try await context.fetch(Company.self)
///     .bottom(5, by: \.revenue)
///     .execute()
///
/// // Get players ranked 11-20
/// let nextPage = try await context.fetch(Player.self)
///     .ranked(from: 11, to: 20, by: \.score)
///     .execute()
/// ```
public struct RankQuery<T: Persistable>: @unchecked Sendable {
    /// Base query with filters
    public let baseQuery: Query<T>

    /// Field to rank by (AnyKeyPath is immutable and thread-safe)
    public let rankField: AnyKeyPath

    /// Field name for the rank field
    public let rankFieldName: String

    /// Ranking direction
    public let direction: RankDirection

    /// Number of results to return
    public let count: Int

    /// Offset (for pagination)
    public let offset: Int

    /// Ranking direction
    public enum RankDirection: Sendable {
        /// Highest values first (descending)
        case descending

        /// Lowest values first (ascending)
        case ascending
    }

    internal init(
        baseQuery: Query<T>,
        rankField: AnyKeyPath,
        rankFieldName: String,
        direction: RankDirection,
        count: Int,
        offset: Int = 0
    ) {
        self.baseQuery = baseQuery
        self.rankField = rankField
        self.rankFieldName = rankFieldName
        self.direction = direction
        self.count = count
        self.offset = offset
    }

    /// Convert to a standard Query with appropriate sort and limit
    public func toQuery() -> Query<T> {
        var query = baseQuery

        // Add sort descriptor
        let sortOrder: SortOrder = direction == .descending ? .descending : .ascending
        query.sortDescriptors.append(SortDescriptor(
            keyPath: rankField,
            order: sortOrder
        ))

        // Add limit and offset
        query.fetchLimit = count
        if offset > 0 {
            query.fetchOffset = offset
        }

        return query
    }
}

// MARK: - SortDescriptor Extension

extension SortDescriptor {
    /// Create from AnyKeyPath (internal use)
    internal init(keyPath: AnyKeyPath, order: SortOrder) {
        self.keyPath = keyPath
        self.order = order
    }
}

// MARK: - Query Extension

extension Query {
    /// Get top N records by a field (highest values first)
    ///
    /// **Usage**:
    /// ```swift
    /// let topScorers = try await context.fetch(Player.self)
    ///     .top(10, by: \.score)
    ///     .execute()
    /// ```
    public func top<V: Comparable & Sendable>(_ n: Int, by keyPath: KeyPath<T, V>) -> RankQuery<T> {
        RankQuery(
            baseQuery: self,
            rankField: keyPath,
            rankFieldName: T.fieldName(for: keyPath),
            direction: .descending,
            count: n
        )
    }

    /// Get bottom N records by a field (lowest values first)
    ///
    /// **Usage**:
    /// ```swift
    /// let lowestPrices = try await context.fetch(Product.self)
    ///     .bottom(10, by: \.price)
    ///     .execute()
    /// ```
    public func bottom<V: Comparable & Sendable>(_ n: Int, by keyPath: KeyPath<T, V>) -> RankQuery<T> {
        RankQuery(
            baseQuery: self,
            rankField: keyPath,
            rankFieldName: T.fieldName(for: keyPath),
            direction: .ascending,
            count: n
        )
    }

    /// Get records ranked from position X to Y (1-indexed)
    ///
    /// **Usage**:
    /// ```swift
    /// // Get players ranked 11-20 (second page of top 10)
    /// let page2 = try await context.fetch(Player.self)
    ///     .ranked(from: 11, to: 20, by: \.score)
    ///     .execute()
    /// ```
    public func ranked<V: Comparable & Sendable>(
        from startRank: Int,
        to endRank: Int,
        by keyPath: KeyPath<T, V>,
        direction: RankQuery<T>.RankDirection = .descending
    ) -> RankQuery<T> {
        let count = max(1, endRank - startRank + 1)
        let offset = max(0, startRank - 1)

        return RankQuery(
            baseQuery: self,
            rankField: keyPath,
            rankFieldName: T.fieldName(for: keyPath),
            direction: direction,
            count: count,
            offset: offset
        )
    }
}

// MARK: - Percentile Query

/// A query for percentile-based operations
public struct PercentileQuery<T: Persistable>: @unchecked Sendable {
    /// Base query with filters
    public let baseQuery: Query<T>

    /// Field to compute percentile on (AnyKeyPath is immutable and thread-safe)
    public let field: AnyKeyPath

    /// Field name
    public let fieldName: String

    /// Percentile value (0.0 - 1.0)
    public let percentile: Double

    internal init(
        baseQuery: Query<T>,
        field: AnyKeyPath,
        fieldName: String,
        percentile: Double
    ) {
        self.baseQuery = baseQuery
        self.field = field
        self.fieldName = fieldName
        self.percentile = min(1.0, max(0.0, percentile))
    }
}

extension Query {
    /// Get the value at a specific percentile
    ///
    /// **Usage**:
    /// ```swift
    /// // Get the median (50th percentile) salary
    /// let medianSalary = try await context.fetch(Employee.self)
    ///     .percentile(0.5, of: \.salary)
    ///     .execute()
    ///
    /// // Get the 90th percentile response time
    /// let p90 = try await context.fetch(Request.self)
    ///     .percentile(0.9, of: \.responseTime)
    ///     .execute()
    /// ```
    public func percentile<V: Comparable & Sendable>(
        _ p: Double,
        of keyPath: KeyPath<T, V>
    ) -> PercentileQuery<T> {
        PercentileQuery(
            baseQuery: self,
            field: keyPath,
            fieldName: T.fieldName(for: keyPath),
            percentile: p
        )
    }
}
