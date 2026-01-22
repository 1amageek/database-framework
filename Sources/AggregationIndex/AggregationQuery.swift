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
///     .having { $0.aggregateInt64("orderCount") ?? 0 > 10 }
///     .execute()
/// // Returns: [AggregateResult<Order>]
/// ```
///
/// **Type Preservation**:
/// - Group keys retain original types via `FieldValue` (int64, double, string, bool, data)
/// - Aggregates return typed results:
///   - count: `FieldValue.int64`
///   - sum/avg: `FieldValue.double`
///   - min/max: `FieldValue?` (original type, nil for empty groups)
///
/// **Grouping Behavior**:
/// - Empty `groupByFieldNames`: All items grouped into single group (global aggregation)
/// - Null field values: Treated as `FieldValue.null` and grouped together
///
/// **Numeric Type Support** (via FieldValue):
/// - Integers: Int, Int8, Int16, Int32, Int64, UInt, UInt8, UInt16, UInt32, UInt64
/// - Floating-point: Float, Double
public struct AggregationQueryBuilder<T: Persistable>: Sendable {
    private let queryContext: IndexQueryContext
    private var groupByFieldNames: [String] = []
    private var aggregations: [AggregationSpec] = []
    private var havingPredicate: (@Sendable (AggregateResult<T>) -> Bool)?

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
    public func having(_ predicate: @escaping @Sendable (AggregateResult<T>) -> Bool) -> Self {
        var copy = self
        copy.havingPredicate = predicate
        return copy
    }

    /// Execute the aggregation query
    ///
    /// - Returns: Array of aggregate results
    /// - Throws: Error if execution fails
    ///
    /// **Current Implementation**:
    /// Fetches all items and computes aggregates in memory (O(n)).
    ///
    /// **Why Not Using Precomputed Indexes**:
    /// AggregationQuery allows combining multiple aggregation types (count + sum + avg)
    /// in a single query. Using individual precomputed indexes would require:
    /// 1. Multiple index lookups for combined queries
    /// 2. Consistent grouping semantics across different index types
    /// 3. Fallback logic when some indexes exist but others don't
    ///
    /// **Direct Index Access** (for O(1) lookups):
    /// For performance-critical single aggregations, use the maintainers directly:
    /// ```swift
    /// // O(1) count lookup
    /// let count = try await countMaintainer.getCount(groupingValues: [region], transaction: tx)
    ///
    /// // O(1) sum lookup
    /// let sum = try await sumMaintainer.getSum(groupingValues: [region], transaction: tx)
    /// ```
    ///
    /// **Precomputed Indexes Available**:
    /// - `CountIndexMaintainer.getCount()` / `getAllCounts()`
    /// - `SumIndexMaintainer.getSum()` / `getAllSums()`
    /// - `AverageIndexMaintainer.getAverage()` / `getAllAverages()`
    /// - `MinIndexMaintainer.getMin()`
    /// - `MaxIndexMaintainer.getMax()`
    public func execute() async throws -> [AggregateResult<T>] {
        guard !aggregations.isEmpty else {
            throw AggregationQueryError.noAggregations
        }

        // Fetch all items and compute aggregates in memory
        let items = try await queryContext.context.fetch(T.self).execute()

        // Group items using FieldValue for type preservation
        // Key: stable hash of field values, Value: (typed field values, items)
        var groups: [UInt64: (fieldValues: [FieldValue], items: [T])] = [:]
        for item in items {
            // Extract field values as FieldValue (type-preserving)
            let groupFieldValues: [FieldValue] = groupByFieldNames.map { fieldName in
                if let value = item[dynamicMember: fieldName] {
                    return FieldValue(value) ?? .null
                }
                return .null
            }

            // Compute stable hash for grouping (FNV-1a algorithm via FieldValue.stableHash)
            // XOR combine hashes with position to preserve order
            var groupKey: UInt64 = 0
            for (index, fieldValue) in groupFieldValues.enumerated() {
                let positionedHash = fieldValue.stableHash() &+ UInt64(index)
                groupKey ^= positionedHash
            }

            if var existing = groups[groupKey] {
                existing.items.append(item)
                groups[groupKey] = existing
            } else {
                groups[groupKey] = (fieldValues: groupFieldValues, items: [item])
            }
        }

        // Compute aggregates for each group
        var results: [AggregateResult<T>] = []
        for (_, groupData) in groups {
            let groupItems = groupData.items

            // Build group key dictionary from stored FieldValue (type-preserving)
            var groupKeyDict: [String: FieldValue] = [:]
            for (index, fieldName) in groupByFieldNames.enumerated() {
                if index < groupData.fieldValues.count {
                    groupKeyDict[fieldName] = groupData.fieldValues[index]
                }
            }

            // Compute aggregates
            var aggregateDict: [String: FieldValue?] = [:]
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
    ///
    /// **Return Values**:
    /// - count: `FieldValue.int64(count)`
    /// - sum: `FieldValue.double(sum)`
    /// - avg: `FieldValue.double(avg)`
    /// - min/max: `FieldValue?` (nil for empty groups)
    private func computeAggregate(items: [T], aggregation: AggregationSpec) -> FieldValue? {
        switch aggregation.type {
        case .count:
            return .int64(Int64(items.count))

        case .sum(let field):
            var sum: Double = 0
            for item in items {
                if let value = extractNumericValue(from: item, field: field) {
                    sum += value
                }
            }
            return .double(sum)

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
            return .double(avg)

        case .min(let field):
            var minValue: FieldValue?

            for item in items {
                if let value = item[dynamicMember: field],
                   let fieldValue = FieldValue(value) {
                    if let current = minValue {
                        // FieldValue is Comparable - use standard comparison
                        if fieldValue < current {
                            minValue = fieldValue
                        }
                    } else {
                        minValue = fieldValue
                    }
                }
            }

            // Return nil for empty groups (not zero)
            return minValue

        case .max(let field):
            var maxValue: FieldValue?

            for item in items {
                if let value = item[dynamicMember: field],
                   let fieldValue = FieldValue(value) {
                    if let current = maxValue {
                        // FieldValue is Comparable - use standard comparison
                        if fieldValue > current {
                            maxValue = fieldValue
                        }
                    } else {
                        maxValue = fieldValue
                    }
                }
            }

            // Return nil for empty groups (not zero)
            return maxValue
        }
    }

    /// Extract numeric value from item field using FieldValue
    ///
    /// **Supported Types** (via FieldValue):
    /// - Int, Int8, Int16, Int32, Int64
    /// - UInt, UInt8, UInt16, UInt32, UInt64
    /// - Float, Double
    private func extractNumericValue(from item: T, field: String) -> Double? {
        guard let value = item[dynamicMember: field] else { return nil }
        // FieldValue handles all numeric type conversions
        guard let fieldValue = FieldValue(value) else { return nil }
        return fieldValue.asDouble
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
