// AggregationQuery.swift
// AggregationIndex - Query extension for aggregation operations

import Foundation
import DatabaseEngine
import Core

// MARK: - Aggregation Query Builder

/// Builder for aggregation queries
///
/// **Usage**:
/// ```swift
/// import AggregationIndex
///
/// let stats = try await context.aggregate(Order.self)
///     .groupBy(\.region)
///     .count(as: "orderCount")
///     .sum(\.amount, as: "totalSales")
///     .having { $0.aggregates["orderCount"]?.value as? Int ?? 0 > 10 }
///     .execute()
/// // Returns: [AggregateResult<Order>]
/// ```
public struct AggregationQueryBuilder<T: Persistable>: @unchecked Sendable {
    private let queryContext: IndexQueryContext
    private var groupByFieldNames: [String] = []
    private var aggregations: [AggregationSpec] = []
    private var havingPredicate: ((AggregateResult<T>) -> Bool)?

    /// Specification for an aggregation
    internal struct AggregationSpec: Sendable {
        let name: String
        let type: AggregationType
    }

    internal init(queryContext: IndexQueryContext) {
        self.queryContext = queryContext
    }

    /// Add a field to GROUP BY
    ///
    /// - Parameter keyPath: KeyPath to the field to group by
    /// - Returns: Updated query builder
    public func groupBy<V>(_ keyPath: KeyPath<T, V>) -> Self {
        var copy = self
        copy.groupByFieldNames.append(T.fieldName(for: keyPath))
        return copy
    }

    /// Add a COUNT aggregation
    ///
    /// - Parameter name: Name for the aggregation result
    /// - Returns: Updated query builder
    public func count(as name: String = "count") -> Self {
        var copy = self
        copy.aggregations.append(AggregationSpec(name: name, type: .count))
        return copy
    }

    /// Add a SUM aggregation
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the numeric field to sum
    ///   - name: Name for the aggregation result (defaults to "sum_fieldName")
    /// - Returns: Updated query builder
    public func sum<V: Numeric>(_ keyPath: KeyPath<T, V>, as name: String? = nil) -> Self {
        var copy = self
        let fieldName = T.fieldName(for: keyPath)
        let aggName = name ?? "sum_\(fieldName)"
        copy.aggregations.append(AggregationSpec(name: aggName, type: .sum(field: fieldName)))
        return copy
    }

    /// Add an AVG aggregation
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the numeric field to average
    ///   - name: Name for the aggregation result (defaults to "avg_fieldName")
    /// - Returns: Updated query builder
    public func avg<V: Numeric>(_ keyPath: KeyPath<T, V>, as name: String? = nil) -> Self {
        var copy = self
        let fieldName = T.fieldName(for: keyPath)
        let aggName = name ?? "avg_\(fieldName)"
        copy.aggregations.append(AggregationSpec(name: aggName, type: .avg(field: fieldName)))
        return copy
    }

    /// Add a MIN aggregation
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the comparable field
    ///   - name: Name for the aggregation result (defaults to "min_fieldName")
    /// - Returns: Updated query builder
    public func min<V: Comparable>(_ keyPath: KeyPath<T, V>, as name: String? = nil) -> Self {
        var copy = self
        let fieldName = T.fieldName(for: keyPath)
        let aggName = name ?? "min_\(fieldName)"
        copy.aggregations.append(AggregationSpec(name: aggName, type: .min(field: fieldName)))
        return copy
    }

    /// Add a MAX aggregation
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the comparable field
    ///   - name: Name for the aggregation result (defaults to "max_fieldName")
    /// - Returns: Updated query builder
    public func max<V: Comparable>(_ keyPath: KeyPath<T, V>, as name: String? = nil) -> Self {
        var copy = self
        let fieldName = T.fieldName(for: keyPath)
        let aggName = name ?? "max_\(fieldName)"
        copy.aggregations.append(AggregationSpec(name: aggName, type: .max(field: fieldName)))
        return copy
    }

    /// Add a HAVING clause to filter aggregated results
    ///
    /// - Parameter predicate: Predicate to filter results
    /// - Returns: Updated query builder
    public func having(_ predicate: @escaping (AggregateResult<T>) -> Bool) -> Self {
        var copy = self
        copy.havingPredicate = predicate
        return copy
    }

    /// Execute the aggregation query
    ///
    /// - Returns: Array of aggregate results
    /// - Throws: Error if execution fails
    ///
    /// **Performance Notes**:
    /// - When no GROUP BY is specified and precomputed aggregation indexes exist
    ///   (CountIndex, SumIndex, etc.), this could use O(1) lookups.
    /// - With GROUP BY or without indexes, falls back to O(n) in-memory aggregation.
    ///
    /// **TODO**: Future optimization to use precomputed aggregation indexes:
    /// - CountIndexMaintainer.getCount() for count queries
    /// - SumIndexMaintainer.getSum() for sum queries
    /// - This requires exposing aggregation query methods in IndexQueryContext.
    public func execute() async throws -> [AggregateResult<T>] {
        guard !aggregations.isEmpty else {
            throw AggregationQueryError.noAggregations
        }

        // Fetch all items and compute aggregates in memory
        // TODO: Use precomputed aggregation indexes when available (CountIndex, SumIndex, etc.)
        // For GROUP BY queries, in-memory calculation is required.
        // For simple aggregates without GROUP BY, precomputed indexes could provide O(1) lookups.
        let items = try await queryContext.context.fetch(T.self).execute()

        // Group items
        var groups: [String: [T]] = [:]
        for item in items {
            let groupKeyParts = groupByFieldNames.map { fieldName -> String in
                if let value = item[dynamicMember: fieldName] {
                    return String(describing: value)
                }
                return "null"
            }
            let groupKey = groupKeyParts.joined(separator: "|")
            groups[groupKey, default: []].append(item)
        }

        // Compute aggregates for each group
        var results: [AggregateResult<T>] = []
        for (groupKeyString, groupItems) in groups {
            // Build group key dictionary
            var groupKeyDict: [String: any Sendable] = [:]
            let keyParts = groupKeyString.split(separator: "|")
            for (index, fieldName) in groupByFieldNames.enumerated() {
                if index < keyParts.count {
                    groupKeyDict[fieldName] = String(keyParts[index])
                }
            }

            // Compute aggregates
            var aggregateDict: [String: any Sendable] = [:]
            for agg in aggregations {
                let value = computeAggregate(items: groupItems, aggregation: agg)
                aggregateDict[agg.name] = value
            }

            let result = AggregateResult<T>(
                groupKey: groupKeyDict,
                aggregates: aggregateDict,
                count: groupItems.count
            )

            // Apply HAVING filter
            if let havingPredicate = havingPredicate {
                if havingPredicate(result) {
                    results.append(result)
                }
            } else {
                results.append(result)
            }
        }

        return results
    }

    /// Compute a single aggregate value
    private func computeAggregate(items: [T], aggregation: AggregationSpec) -> any Sendable {
        switch aggregation.type {
        case .count:
            return items.count

        case .sum(let field):
            var sum: Double = 0
            for item in items {
                if let value = extractNumericValue(from: item, field: field) {
                    sum += value
                }
            }
            return sum

        case .avg(let field):
            var sum: Double = 0
            var count = 0
            for item in items {
                if let value = extractNumericValue(from: item, field: field) {
                    sum += value
                    count += 1
                }
            }
            let avg = count > 0 ? sum / Double(count) : 0.0
            return avg

        case .min(let field):
            var minDouble: Double?
            var minInt: Int?
            var minString: String?

            for item in items {
                if let value = item[dynamicMember: field] {
                    if let numVal = value as? Double {
                        if minDouble == nil || numVal < minDouble! {
                            minDouble = numVal
                        }
                    } else if let intVal = value as? Int {
                        if minInt == nil || intVal < minInt! {
                            minInt = intVal
                        }
                    } else if let strVal = value as? String {
                        if minString == nil || strVal < minString! {
                            minString = strVal
                        }
                    }
                }
            }

            if let d = minDouble { return d }
            if let i = minInt { return i }
            if let s = minString { return s }
            return 0

        case .max(let field):
            var maxDouble: Double?
            var maxInt: Int?
            var maxString: String?

            for item in items {
                if let value = item[dynamicMember: field] {
                    if let numVal = value as? Double {
                        if maxDouble == nil || numVal > maxDouble! {
                            maxDouble = numVal
                        }
                    } else if let intVal = value as? Int {
                        if maxInt == nil || intVal > maxInt! {
                            maxInt = intVal
                        }
                    } else if let strVal = value as? String {
                        if maxString == nil || strVal > maxString! {
                            maxString = strVal
                        }
                    }
                }
            }

            if let d = maxDouble { return d }
            if let i = maxInt { return i }
            if let s = maxString { return s }
            return 0
        }
    }

    /// Extract numeric value from item field
    private func extractNumericValue(from item: T, field: String) -> Double? {
        guard let value = item[dynamicMember: field] else { return nil }

        if let intValue = value as? Int { return Double(intValue) }
        if let doubleValue = value as? Double { return doubleValue }
        if let floatValue = value as? Float { return Double(floatValue) }
        if let int64Value = value as? Int64 { return Double(int64Value) }

        return nil
    }
}

// MARK: - FDBContext Extension

extension FDBContext {

    /// Start an aggregation query
    ///
    /// This method is available when you import `AggregationIndex`.
    ///
    /// **Usage**:
    /// ```swift
    /// import AggregationIndex
    ///
    /// let stats = try await context.aggregate(Order.self)
    ///     .groupBy(\.region)
    ///     .count(as: "orderCount")
    ///     .sum(\.amount, as: "totalSales")
    ///     .having { $0.count > 10 }
    ///     .execute()
    /// // Returns: [AggregateResult<Order>]
    /// ```
    ///
    /// - Parameter type: The Persistable type to aggregate
    /// - Returns: Entry point for configuring the aggregation
    public func aggregate<T: Persistable>(_ type: T.Type) -> AggregationQueryBuilder<T> {
        AggregationQueryBuilder(queryContext: indexQueryContext)
    }
}

// MARK: - Aggregation Query Error

/// Errors for aggregation query operations
public enum AggregationQueryError: Error, CustomStringConvertible {
    /// No aggregations specified
    case noAggregations

    /// Invalid field for aggregation
    case invalidField(String)

    /// Index not found
    case indexNotFound(String)

    public var description: String {
        switch self {
        case .noAggregations:
            return "No aggregations specified for aggregation query"
        case .invalidField(let field):
            return "Invalid field for aggregation: \(field)"
        case .indexNotFound(let name):
            return "Aggregation index not found: \(name)"
        }
    }
}
