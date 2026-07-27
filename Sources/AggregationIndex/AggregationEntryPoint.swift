// AggregationEntryPoint.swift
// AggregationIndex - Entry point for aggregation queries
//
// Follows the EntryPoint → QueryBuilder pattern used by other index types
// (VectorEntryPoint, FullTextEntryPoint, SpatialEntryPoint)

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import DatabaseEngine
import DatabaseKit

// MARK: - Aggregation Entry Point

/// Entry point for aggregation queries
///
/// This type follows the EntryPoint → QueryBuilder pattern used by other index types.
/// It serves as the initial configuration point before building the actual query.
///
/// **Usage**:
/// ```swift
/// import AggregationIndex
///
/// // Grouped aggregation
/// let stats = try await context.aggregate(Order.self)
///     .groupBy(Order.fields.region)
///     .count(as: "orderCount")
///     .sum(Order.fields.amount, as: "totalSales")
///     .execute()
///
/// // Global aggregation (no grouping)
/// let total = try await context.aggregate(Order.self)
///     .count()
///     .sum(Order.fields.amount)
///     .execute()
///
/// // Force a specific index
/// let stats = try await context.aggregate(Order.self)
///     .using(index: "Order_count_region")
///     .execute()
/// ```
public struct AggregationEntryPoint<T: Persistable>: Sendable {
    private let queryContext: IndexQueryContext

    internal init(queryContext: IndexQueryContext) {
        self.queryContext = queryContext
    }

    // MARK: - Group By

    /// Start a grouped aggregation
    ///
    /// - Parameter field: Typed field to group by
    /// - Returns: AggregationQueryBuilder for adding aggregations
    public func groupBy<V>(_ field: Field<T, V>) -> AggregationQueryBuilder<T> {
        AggregationQueryBuilder(queryContext: queryContext)
            .groupBy(field)
    }

    // MARK: - Global Aggregation (No Grouping)

    /// Add a COUNT aggregation (global - no grouping)
    ///
    /// - Parameter name: Name for the aggregation result
    /// - Returns: AggregationQueryBuilder for chaining
    public func count(as name: String = "count") -> AggregationQueryBuilder<T> {
        AggregationQueryBuilder(queryContext: queryContext)
            .count(as: name)
    }

    /// Add a SUM aggregation (global - no grouping)
    ///
    /// - Parameters:
    ///   - field: Typed numeric field to sum
    ///   - name: Name for the aggregation result
    /// - Returns: AggregationQueryBuilder for chaining
    public func sum<V: IndexNumericValue>(
        _ field: Field<T, V>,
        as name: String? = nil
    ) -> AggregationQueryBuilder<T> {
        AggregationQueryBuilder(queryContext: queryContext)
            .sum(field, as: name)
    }

    public func sum<V: IndexNumericValue>(
        _ field: Field<T, V?>,
        as name: String? = nil
    ) -> AggregationQueryBuilder<T> {
        AggregationQueryBuilder(queryContext: queryContext)
            .sum(field, as: name)
    }

    /// Add an AVG aggregation (global - no grouping)
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the numeric field to average
    ///   - name: Name for the aggregation result
    /// - Returns: AggregationQueryBuilder for chaining
    public func avg<V: IndexNumericValue>(
        _ field: Field<T, V>,
        as name: String? = nil
    ) -> AggregationQueryBuilder<T> {
        AggregationQueryBuilder(queryContext: queryContext)
            .avg(field, as: name)
    }

    public func avg<V: IndexNumericValue>(
        _ field: Field<T, V?>,
        as name: String? = nil
    ) -> AggregationQueryBuilder<T> {
        AggregationQueryBuilder(queryContext: queryContext)
            .avg(field, as: name)
    }

    /// Add a MIN aggregation (global - no grouping)
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the comparable field
    ///   - name: Name for the aggregation result
    /// - Returns: AggregationQueryBuilder for chaining
    public func min<V: IndexComparableValue>(
        _ field: Field<T, V>,
        as name: String? = nil
    ) -> AggregationQueryBuilder<T> {
        AggregationQueryBuilder(queryContext: queryContext)
            .min(field, as: name)
    }

    public func min<V: IndexComparableValue>(
        _ field: Field<T, V?>,
        as name: String? = nil
    ) -> AggregationQueryBuilder<T> {
        AggregationQueryBuilder(queryContext: queryContext)
            .min(field, as: name)
    }

    /// Add a MAX aggregation (global - no grouping)
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the comparable field
    ///   - name: Name for the aggregation result
    /// - Returns: AggregationQueryBuilder for chaining
    public func max<V: IndexComparableValue>(
        _ field: Field<T, V>,
        as name: String? = nil
    ) -> AggregationQueryBuilder<T> {
        AggregationQueryBuilder(queryContext: queryContext)
            .max(field, as: name)
    }

    public func max<V: IndexComparableValue>(
        _ field: Field<T, V?>,
        as name: String? = nil
    ) -> AggregationQueryBuilder<T> {
        AggregationQueryBuilder(queryContext: queryContext)
            .max(field, as: name)
    }

    /// Add a DISTINCT aggregation (global - no grouping)
    ///
    /// - Parameters:
    ///   - field: Typed field whose distinct values are counted
    ///   - name: Name for the aggregation result
    /// - Returns: AggregationQueryBuilder for chaining
    public func distinct<V>(
        _ field: Field<T, V>,
        as name: String? = nil
    ) -> AggregationQueryBuilder<T> {
        AggregationQueryBuilder(queryContext: queryContext)
            .distinct(field, as: name)
    }

    /// Add a PERCENTILE aggregation (global - no grouping)
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the numeric field
    ///   - percentile: Quantile between zero and one
    ///   - name: Name for the aggregation result
    /// - Returns: AggregationQueryBuilder for chaining
    public func percentile<V: IndexNumericValue>(
        _ field: Field<T, V>,
        p percentile: Double,
        as name: String? = nil
    ) -> AggregationQueryBuilder<T> {
        AggregationQueryBuilder(queryContext: queryContext)
            .percentile(field, p: percentile, as: name)
    }

    public func percentile<V: IndexNumericValue>(
        _ field: Field<T, V?>,
        p percentile: Double,
        as name: String? = nil
    ) -> AggregationQueryBuilder<T> {
        AggregationQueryBuilder(queryContext: queryContext)
            .percentile(field, p: percentile, as: name)
    }

    // MARK: - Index Selection

    /// Force use of a specific index
    ///
    /// By default, the system automatically selects the best index based on
    /// the query's groupBy fields and aggregation types. Use this method to
    /// force use of a specific index.
    ///
    /// - Parameter indexName: Name of the index to use
    /// - Returns: AggregationQueryBuilder with forced index
    public func using(index indexName: String) -> AggregationQueryBuilder<T> {
        var builder = AggregationQueryBuilder<T>(queryContext: queryContext)
        builder.forcedIndexName = indexName
        return builder
    }
}

// MARK: - DatabaseContext Extension (Updated)

extension DatabaseContext {

    /// Start an aggregation query
    ///
    /// This method is available when you import `AggregationIndex`.
    /// Returns an `AggregationEntryPoint` that provides entry into the aggregation query builder.
    ///
    /// **Usage**:
    /// ```swift
    /// import AggregationIndex
    ///
    /// // Grouped aggregation
    /// let stats = try await context.aggregate(Order.self)
    ///     .groupBy(Order.fields.region)
    ///     .count(as: "orderCount")
    ///     .sum(Order.fields.amount, as: "totalSales")
    ///     .having { $0.aggregateInt64("orderCount") ?? 0 > 10 }
    ///     .execute()
    /// // Returns: [AggregateResult<Order>]
    ///
    /// // Global aggregation (no grouping)
    /// let total = try await context.aggregate(Order.self)
    ///     .count()
    ///     .sum(Order.fields.amount)
    ///     .execute()
    /// ```
    ///
    /// **Automatic Index Selection**:
    /// When a matching precomputed index exists (e.g., `CountIndexKind`, `SumIndexKind`),
    /// the system automatically uses its bounded group scan. Otherwise, it falls back
    /// to O(n) in-memory computation.
    ///
    /// - Parameter type: The Persistable type to aggregate
    /// - Returns: Entry point for configuring the aggregation
    public func aggregate<T: Persistable>(_ type: T.Type) -> AggregationEntryPoint<T> {
        AggregationEntryPoint(queryContext: indexQueryContext)
    }
}
