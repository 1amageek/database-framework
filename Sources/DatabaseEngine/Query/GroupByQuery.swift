// GroupByQuery.swift
// Query DSL - GROUP BY aggregation queries

import Foundation
import Core

// MARK: - Aggregation Types

/// Represents an aggregation function
public enum Aggregation<T: Persistable, V: Sendable> {
    /// Count of records
    case count

    /// Sum of field values
    case sum(KeyPath<T, V>)

    /// Average of field values
    case avg(KeyPath<T, V>)

    /// Minimum field value
    case min(KeyPath<T, V>)

    /// Maximum field value
    case max(KeyPath<T, V>)
}

// KeyPath is immutable and thread-safe, but not formally Sendable
extension Aggregation: @unchecked Sendable {}

/// Type-erased aggregation for storage
public struct AnyAggregation<T: Persistable>: Sendable {
    internal let name: String
    internal let type: AggregationType
    internal let fieldName: String?

    internal init<V: Sendable>(name: String, aggregation: Aggregation<T, V>) {
        self.name = name
        switch aggregation {
        case .count:
            self.type = .count
            self.fieldName = nil
        case .sum(let keyPath):
            self.type = .sum(field: T.fieldName(for: keyPath))
            self.fieldName = T.fieldName(for: keyPath)
        case .avg(let keyPath):
            self.type = .avg(field: T.fieldName(for: keyPath))
            self.fieldName = T.fieldName(for: keyPath)
        case .min(let keyPath):
            self.type = .min(field: T.fieldName(for: keyPath))
            self.fieldName = T.fieldName(for: keyPath)
        case .max(let keyPath):
            self.type = .max(field: T.fieldName(for: keyPath))
            self.fieldName = T.fieldName(for: keyPath)
        }
    }
}

// MARK: - Aggregate Result

/// Result of a GROUP BY query
public struct AggregateResult<T: Persistable>: Sendable {
    /// Group key values (field name -> value)
    public let groupKey: [String: AnySendable]

    /// Aggregation results (aggregation name -> value)
    public let aggregates: [String: AnySendable]

    /// Number of records in this group
    public let count: Int

    public init(
        groupKey: [String: AnySendable],
        aggregates: [String: AnySendable],
        count: Int
    ) {
        self.groupKey = groupKey
        self.aggregates = aggregates
        self.count = count
    }
}

// MARK: - GROUP BY Query

/// A query with GROUP BY aggregation
public struct GroupByQuery<T: Persistable>: @unchecked Sendable {
    /// Base query with filters
    public let baseQuery: Query<T>

    /// Fields to group by (AnyKeyPath is immutable and thread-safe)
    public let groupByFields: [AnyKeyPath]

    /// Aggregations to compute
    public let aggregations: [AnyAggregation<T>]

    /// HAVING clause (filter on aggregated results)
    public let havingPredicate: Predicate<T>?

    /// Create a GROUP BY query
    internal init(
        baseQuery: Query<T>,
        groupByFields: [AnyKeyPath],
        aggregations: [AnyAggregation<T>],
        havingPredicate: Predicate<T>? = nil
    ) {
        self.baseQuery = baseQuery
        self.groupByFields = groupByFields
        self.aggregations = aggregations
        self.havingPredicate = havingPredicate
    }

    /// Add a HAVING clause to filter aggregated results
    public func having(_ predicate: Predicate<T>) -> GroupByQuery<T> {
        GroupByQuery(
            baseQuery: baseQuery,
            groupByFields: groupByFields,
            aggregations: aggregations,
            havingPredicate: predicate
        )
    }
}

// MARK: - GROUP BY Builder

/// Builder for GROUP BY queries
public struct GroupByBuilder<T: Persistable>: @unchecked Sendable {
    private let baseQuery: Query<T>
    private var groupByFields: [AnyKeyPath] = []
    private var aggregations: [AnyAggregation<T>] = []

    internal init(baseQuery: Query<T>) {
        self.baseQuery = baseQuery
    }

    /// Add a field to GROUP BY
    public func by<V: Sendable>(_ keyPath: KeyPath<T, V>) -> GroupByBuilder<T> {
        var copy = self
        copy.groupByFields.append(keyPath)
        return copy
    }

    /// Add a COUNT aggregation
    public func count(as name: String = "count") -> GroupByBuilder<T> {
        var copy = self
        copy.aggregations.append(AnyAggregation(name: name, aggregation: Aggregation<T, Int>.count))
        return copy
    }

    /// Add a SUM aggregation
    public func sum<V: Numeric & Sendable>(_ keyPath: KeyPath<T, V>, as name: String? = nil) -> GroupByBuilder<T> {
        var copy = self
        let fieldName = T.fieldName(for: keyPath)
        let aggName = name ?? "sum_\(fieldName)"
        copy.aggregations.append(AnyAggregation(name: aggName, aggregation: .sum(keyPath)))
        return copy
    }

    /// Add an AVG aggregation
    public func avg<V: Numeric & Sendable>(_ keyPath: KeyPath<T, V>, as name: String? = nil) -> GroupByBuilder<T> {
        var copy = self
        let fieldName = T.fieldName(for: keyPath)
        let aggName = name ?? "avg_\(fieldName)"
        copy.aggregations.append(AnyAggregation(name: aggName, aggregation: .avg(keyPath)))
        return copy
    }

    /// Add a MIN aggregation
    public func min<V: Comparable & Sendable>(_ keyPath: KeyPath<T, V>, as name: String? = nil) -> GroupByBuilder<T> {
        var copy = self
        let fieldName = T.fieldName(for: keyPath)
        let aggName = name ?? "min_\(fieldName)"
        copy.aggregations.append(AnyAggregation(name: aggName, aggregation: .min(keyPath)))
        return copy
    }

    /// Add a MAX aggregation
    public func max<V: Comparable & Sendable>(_ keyPath: KeyPath<T, V>, as name: String? = nil) -> GroupByBuilder<T> {
        var copy = self
        let fieldName = T.fieldName(for: keyPath)
        let aggName = name ?? "max_\(fieldName)"
        copy.aggregations.append(AnyAggregation(name: aggName, aggregation: .max(keyPath)))
        return copy
    }

    /// Build the GROUP BY query
    public func build() -> GroupByQuery<T> {
        GroupByQuery(
            baseQuery: baseQuery,
            groupByFields: groupByFields,
            aggregations: aggregations,
            havingPredicate: nil
        )
    }
}

// MARK: - Query Extension

extension Query {
    /// Start building a GROUP BY query
    ///
    /// **Usage**:
    /// ```swift
    /// let results = try await context.fetch(Order.self)
    ///     .where(\.status == "completed")
    ///     .groupBy()
    ///     .by(\.customerId)
    ///     .count()
    ///     .sum(\.amount)
    ///     .build()
    ///     .having(\.count > 5)
    ///     .execute()
    /// ```
    public func groupBy() -> GroupByBuilder<T> {
        GroupByBuilder(baseQuery: self)
    }

    /// Create a GROUP BY query with a single grouping field
    ///
    /// **Usage**:
    /// ```swift
    /// let query = Query<Order>()
    ///     .where(\.status == "completed")
    ///     .groupBy(\.department)
    /// ```
    public func groupBy<V: Sendable>(_ keyPath: KeyPath<T, V>) -> GroupByBuilder<T> {
        GroupByBuilder(baseQuery: self).by(keyPath)
    }
}

// MARK: - Aggregation Accumulator (Internal)

/// Accumulator for computing aggregations during query execution
internal struct AggregationAccumulator<T: Persistable>: Sendable {
    private var counts: [String: Int] = [:]
    private var sums: [String: Double] = [:]
    private var mins: [String: AnySendable] = [:]
    private var maxs: [String: AnySendable] = [:]

    internal init() {}

    internal mutating func accumulate(_ item: T, aggregations: [AnyAggregation<T>]) {
        for agg in aggregations {
            switch agg.type {
            case .count:
                counts[agg.name, default: 0] += 1

            case .sum(let field):
                if let value = extractNumericValue(from: item, field: field) {
                    sums[agg.name, default: 0] += value
                }

            case .avg(let field):
                counts["\(agg.name)_count", default: 0] += 1
                if let value = extractNumericValue(from: item, field: field) {
                    sums["\(agg.name)_sum", default: 0] += value
                }

            case .min(let field):
                if let current = mins[agg.name] {
                    if let newValue = item[dynamicMember: field],
                       compareValues(newValue, current.value) == .orderedAscending {
                        mins[agg.name] = AnySendable(newValue)
                    }
                } else if let value = item[dynamicMember: field] {
                    mins[agg.name] = AnySendable(value)
                }

            case .max(let field):
                if let current = maxs[agg.name] {
                    if let newValue = item[dynamicMember: field],
                       compareValues(newValue, current.value) == .orderedDescending {
                        maxs[agg.name] = AnySendable(newValue)
                    }
                } else if let value = item[dynamicMember: field] {
                    maxs[agg.name] = AnySendable(value)
                }
            }
        }
    }

    internal func results(aggregations: [AnyAggregation<T>]) -> [String: AnySendable] {
        var result: [String: AnySendable] = [:]

        for agg in aggregations {
            switch agg.type {
            case .count:
                result[agg.name] = AnySendable(counts[agg.name] ?? 0)

            case .sum:
                result[agg.name] = AnySendable(sums[agg.name] ?? 0.0)

            case .avg:
                let count = counts["\(agg.name)_count"] ?? 0
                let sum = sums["\(agg.name)_sum"] ?? 0.0
                let avg = count > 0 ? sum / Double(count) : 0.0
                result[agg.name] = AnySendable(avg)

            case .min:
                if let min = mins[agg.name] {
                    result[agg.name] = min
                }

            case .max:
                if let max = maxs[agg.name] {
                    result[agg.name] = max
                }
            }
        }

        return result
    }

    internal var totalCount: Int {
        counts.values.first ?? 0
    }

    private func extractNumericValue(from item: T, field: String) -> Double? {
        guard let value = item[dynamicMember: field] else { return nil }

        if let intValue = value as? Int { return Double(intValue) }
        if let doubleValue = value as? Double { return doubleValue }
        if let floatValue = value as? Float { return Double(floatValue) }
        if let int64Value = value as? Int64 { return Double(int64Value) }

        return nil
    }

    private func compareValues(_ lhs: Any, _ rhs: Any) -> ComparisonResult {
        if let l = lhs as? Int, let r = rhs as? Int {
            return l < r ? .orderedAscending : (l > r ? .orderedDescending : .orderedSame)
        }
        if let l = lhs as? Double, let r = rhs as? Double {
            return l < r ? .orderedAscending : (l > r ? .orderedDescending : .orderedSame)
        }
        if let l = lhs as? String, let r = rhs as? String {
            return l.compare(r)
        }
        return .orderedSame
    }
}
