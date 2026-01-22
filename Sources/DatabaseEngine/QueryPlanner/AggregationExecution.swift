// AggregationExecution.swift
// DatabaseEngine - Aggregation query execution with precomputed index support
//
// Reference: FDB Record Layer FDBRecordStore.executeAggregateFunction
// Executes aggregation queries using precomputed indexes when available.

import Foundation
import Core
import FoundationDB

// MARK: - NullValue

/// Represents a SQL NULL value for aggregation results
///
/// Used when MIN/MAX/AVG operates on an empty set, which should return NULL
/// rather than a default value like 0.
///
/// **Example**:
/// ```swift
/// let result = executor.min(field: "amount")
/// if result.isNull {
///     print("No records to aggregate")
/// }
/// ```
public struct NullValue: Sendable, Equatable, CustomStringConvertible {
    public static let instance = NullValue()

    private init() {}

    public var description: String { "NULL" }
}

// MARK: - AggregationResult

/// Result of an aggregation query
///
/// Contains the computed aggregate value along with optional group key
/// for GROUP BY queries.
///
/// **Usage**:
/// ```swift
/// let results = try await executor.executeAggregation(
///     type: Order.self,
///     aggregation: .sum(field: "amount"),
///     groupBy: ["region"]
/// )
///
/// for result in results {
///     print("Region: \(result.groupKey), Total: \(result.value)")
/// }
/// ```
public struct AggregationResult: @unchecked Sendable, CustomStringConvertible {
    /// The aggregation type that produced this result
    public let aggregationType: AggregationType

    /// The computed aggregate value
    ///
    /// Type depends on aggregation:
    /// - COUNT: Int64
    /// - SUM: Double (or original numeric type)
    /// - AVG: Double
    /// - MIN/MAX: Same type as the field
    public let value: any Sendable

    /// Group key values (empty for non-grouped aggregations)
    ///
    /// Keys are field names, values are the grouping values
    public let groupKey: [String: any Sendable]

    /// Number of records contributing to this aggregate
    /// (if tracked, otherwise nil)
    public let recordCount: Int?

    public init(
        aggregationType: AggregationType,
        value: any Sendable,
        groupKey: [String: any Sendable] = [:],
        recordCount: Int? = nil
    ) {
        self.aggregationType = aggregationType
        self.value = value
        self.groupKey = groupKey
        self.recordCount = recordCount
    }

    public var description: String {
        var desc = "AggregationResult(\(aggregationType)"
        if !groupKey.isEmpty {
            let keyStr = groupKey.map { "\($0.key)=\($0.value)" }.joined(separator: ", ")
            desc += ", groupKey=[\(keyStr)]"
        }
        desc += ", value=\(value)"
        if let count = recordCount {
            desc += ", count=\(count)"
        }
        desc += ")"
        return desc
    }

    // MARK: - Typed Value Access

    /// Check if the result is NULL (empty set for MIN/MAX/AVG)
    public var isNull: Bool {
        value is NullValue
    }

    /// Get value as Int64 (for COUNT)
    public var intValue: Int64? {
        if isNull { return nil }
        if let v = value as? Int64 { return v }
        if let v = value as? Int { return Int64(v) }
        return nil
    }

    /// Get value as Double (for SUM, AVG)
    public var doubleValue: Double? {
        if isNull { return nil }
        if let v = value as? Double { return v }
        if let v = value as? Float { return Double(v) }
        if let v = value as? Int { return Double(v) }
        if let v = value as? Int64 { return Double(v) }
        return nil
    }

    /// Get value as String (for MIN/MAX on strings)
    public var stringValue: String? {
        if isNull { return nil }
        return value as? String
    }
}

// MARK: - AggregationExecutor

/// Executes aggregation queries using precomputed indexes or in-memory computation
///
/// **Two Execution Paths**:
/// 1. **Index-based** (O(1) or O(groups)): When a matching aggregation index exists
///    (CountIndex, SumIndex, etc.), reads directly from precomputed values.
/// 2. **In-memory** (O(n)): When no index exists, scans all records and computes
///    the aggregate in memory.
///
/// **Reference**: FDB Record Layer aggregate function execution pattern
///
/// **Usage**:
/// ```swift
/// let executor = AggregationExecutor(context: context)
///
/// // Simple count (uses CountIndex if available)
/// let count = try await executor.executeCount(type: Order.self)
///
/// // Grouped sum (uses SumIndex if available)
/// let sums = try await executor.executeAggregation(
///     type: Order.self,
///     aggregation: .sum(field: "amount"),
///     groupBy: ["region"]
/// )
/// ```
public final class AggregationExecutor<T: Persistable & Codable>: @unchecked Sendable {
    private let context: FDBContext

    public init(context: FDBContext) {
        self.context = context
    }

    // MARK: - Simple Count

    /// Execute a COUNT aggregation
    ///
    /// - Parameters:
    ///   - predicate: Optional filter predicate
    /// - Returns: Count of matching records
    public func executeCount(predicate: Predicate<T>? = nil) async throws -> Int64 {
        // TODO: Check for CountIndex and use it if available
        // For now, fall back to in-memory counting

        let items = try await context.fetch(T.self).execute()

        if let predicate = predicate {
            let evaluator = PredicateEvaluator<T>()
            let filtered = items.filter { evaluator.evaluate(predicate, on: $0) }
            return Int64(filtered.count)
        }

        return Int64(items.count)
    }

    // MARK: - Single Aggregation

    /// Execute a single aggregation (non-grouped)
    ///
    /// - Parameters:
    ///   - aggregation: The aggregation type to compute
    ///   - predicate: Optional filter predicate
    /// - Returns: The aggregation result
    public func executeSingleAggregation(
        aggregation: AggregationType,
        predicate: Predicate<T>? = nil
    ) async throws -> AggregationResult {
        // TODO: Check for matching aggregation index
        // For now, compute in memory

        var items = try await context.fetch(T.self).execute()

        if let predicate = predicate {
            let evaluator = PredicateEvaluator<T>()
            items = items.filter { evaluator.evaluate(predicate, on: $0) }
        }

        let value = computeAggregate(items: items, aggregation: aggregation)

        return AggregationResult(
            aggregationType: aggregation,
            value: value,
            groupKey: [:],
            recordCount: items.count
        )
    }

    // MARK: - Grouped Aggregation

    /// Execute a grouped aggregation (GROUP BY)
    ///
    /// - Parameters:
    ///   - aggregation: The aggregation type to compute
    ///   - groupBy: Field names to group by
    ///   - predicate: Optional filter predicate
    ///   - having: Optional filter on aggregate results
    /// - Returns: Array of aggregation results, one per group
    public func executeGroupedAggregation(
        aggregation: AggregationType,
        groupBy: [String],
        predicate: Predicate<T>? = nil,
        having: ((AggregationResult) -> Bool)? = nil
    ) async throws -> [AggregationResult] {
        // Fetch all items
        var items = try await context.fetch(T.self).execute()

        // Apply predicate filter
        if let predicate = predicate {
            let evaluator = PredicateEvaluator<T>()
            items = items.filter { evaluator.evaluate(predicate, on: $0) }
        }

        // Group items using proper hashable GroupKey
        var groups: [GroupKey: [T]] = [:]
        for item in items {
            let key = buildGroupKey(item: item, groupBy: groupBy)
            groups[key, default: []].append(item)
        }

        // Compute aggregates for each group
        var results: [AggregationResult] = []
        for (groupKey, groupItems) in groups {
            let groupKeyDict = groupKeyToDictionary(groupKey)
            let value = computeAggregate(items: groupItems, aggregation: aggregation)

            let result = AggregationResult(
                aggregationType: aggregation,
                value: value,
                groupKey: groupKeyDict,
                recordCount: groupItems.count
            )

            // Apply HAVING filter
            if let having = having {
                if having(result) {
                    results.append(result)
                }
            } else {
                results.append(result)
            }
        }

        return results
    }

    // MARK: - Multiple Aggregations

    /// Execute multiple aggregations in a single pass
    ///
    /// - Parameters:
    ///   - aggregations: Array of aggregation types to compute
    ///   - groupBy: Field names to group by (empty for non-grouped)
    ///   - predicate: Optional filter predicate
    /// - Returns: Dictionary mapping aggregation type to results
    public func executeMultipleAggregations(
        aggregations: [AggregationType],
        groupBy: [String] = [],
        predicate: Predicate<T>? = nil
    ) async throws -> [AggregationType: [AggregationResult]] {
        // Fetch items once
        var items = try await context.fetch(T.self).execute()

        if let predicate = predicate {
            let evaluator = PredicateEvaluator<T>()
            items = items.filter { evaluator.evaluate(predicate, on: $0) }
        }

        // If no grouping, compute all aggregations on the full set
        if groupBy.isEmpty {
            var results: [AggregationType: [AggregationResult]] = [:]
            for aggregation in aggregations {
                let value = computeAggregate(items: items, aggregation: aggregation)
                let result = AggregationResult(
                    aggregationType: aggregation,
                    value: value,
                    groupKey: [:],
                    recordCount: items.count
                )
                results[aggregation] = [result]
            }
            return results
        }

        // Group items using proper hashable GroupKey
        var groups: [GroupKey: [T]] = [:]
        for item in items {
            let key = buildGroupKey(item: item, groupBy: groupBy)
            groups[key, default: []].append(item)
        }

        // Compute all aggregations for each group
        var results: [AggregationType: [AggregationResult]] = [:]
        for aggregation in aggregations {
            results[aggregation] = []
        }

        for (groupKey, groupItems) in groups {
            let groupKeyDict = groupKeyToDictionary(groupKey)

            for aggregation in aggregations {
                let value = computeAggregate(items: groupItems, aggregation: aggregation)
                let result = AggregationResult(
                    aggregationType: aggregation,
                    value: value,
                    groupKey: groupKeyDict,
                    recordCount: groupItems.count
                )
                results[aggregation]?.append(result)
            }
        }

        return results
    }

    // MARK: - Private Helpers

    /// Compute a single aggregate value from items
    ///
    /// Returns the computed aggregate value.
    /// For MIN/MAX on empty sets, returns a special NullValue marker.
    private func computeAggregate(items: [T], aggregation: AggregationType) -> any Sendable {
        switch aggregation {
        case .count:
            return Int64(items.count)

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
            // AVG of empty set is NULL (represented as NaN or special value)
            if count == 0 {
                return NullValue.instance
            }
            return sum / Double(count)

        case .min(let field):
            return computeMinMax(items: items, field: field, isMin: true)

        case .max(let field):
            return computeMinMax(items: items, field: field, isMin: false)

        case .distinct(let field):
            var distinctValues = Set<AnyHashable>()
            for item in items {
                if let value = extractFieldValue(from: item, field: field) {
                    if let hashable = value as? AnyHashable {
                        distinctValues.insert(hashable)
                    } else {
                        // Fallback to string representation
                        distinctValues.insert(String(describing: value) as AnyHashable)
                    }
                }
            }
            return Int64(distinctValues.count)

        case .percentile(let field, let percentile):
            var values: [Double] = []
            for item in items {
                if let value = extractNumericValue(from: item, field: field) {
                    values.append(value)
                }
            }
            if values.isEmpty {
                return NullValue.instance
            }
            values.sort()
            // Linear interpolation for percentile
            let index = percentile * Double(values.count - 1)
            let lower = Int(index.rounded(.down))
            let upper = Int(index.rounded(.up))
            if lower == upper || upper >= values.count {
                return values[Swift.min(lower, values.count - 1)]
            }
            let fraction = index - Double(lower)
            return values[lower] + fraction * (values[upper] - values[lower])
        }
    }

    /// Compute MIN or MAX value with proper type handling
    ///
    /// - Parameters:
    ///   - items: Items to aggregate
    ///   - field: Field name to aggregate
    ///   - isMin: true for MIN, false for MAX
    /// - Returns: The MIN/MAX value, or NullValue if empty
    private func computeMinMax(items: [T], field: String, isMin: Bool) -> any Sendable {
        // Collect all non-nil values and normalize to comparable types
        var numericValues: [Double] = []
        var stringValues: [String] = []
        var hasNumeric = false
        var hasString = false

        for item in items {
            guard let value = extractFieldValue(from: item, field: field) else { continue }

            // Try to extract as numeric first (handles Int, Int64, Double, Float)
            if let numValue = extractNumericValue(from: item, field: field) {
                numericValues.append(numValue)
                hasNumeric = true
            } else if let strValue = value as? String {
                stringValues.append(strValue)
                hasString = true
            }
        }

        // Empty set - return NULL
        if !hasNumeric && !hasString {
            return NullValue.instance
        }

        // Prefer numeric comparison if we have numeric values
        if hasNumeric {
            if isMin {
                if let minVal = numericValues.min() {
                    return minVal
                }
            } else {
                if let maxVal = numericValues.max() {
                    return maxVal
                }
            }
        }

        // Fall back to string comparison
        if hasString {
            if isMin {
                if let minVal = stringValues.min() {
                    return minVal
                }
            } else {
                if let maxVal = stringValues.max() {
                    return maxVal
                }
            }
        }

        return NullValue.instance
    }

    /// Extract numeric value from an item field
    private func extractNumericValue(from item: T, field: String) -> Double? {
        guard let value = extractFieldValue(from: item, field: field) else { return nil }

        if let intValue = value as? Int { return Double(intValue) }
        if let doubleValue = value as? Double { return doubleValue }
        if let floatValue = value as? Float { return Double(floatValue) }
        if let int64Value = value as? Int64 { return Double(int64Value) }

        return nil
    }

    /// Extract any field value from an item
    ///
    /// Uses Persistable's dynamicMember subscript for first-level access,
    /// then falls back to Mirror for nested non-Persistable types.
    private func extractFieldValue(from item: T, field: String) -> Any? {
        // Handle nested field paths (e.g., "address.city")
        let components = field.split(separator: ".").map(String.init)
        guard let firstComponent = components.first else { return nil }

        // First level: use Persistable's dynamicMember subscript
        guard let firstValue = item[dynamicMember: firstComponent] else { return nil }

        if components.count == 1 {
            return firstValue
        }

        // Nested levels: use Mirror for non-Persistable types
        var current: Any = firstValue
        for component in components.dropFirst() {
            let currentMirror = Mirror(reflecting: current)
            guard let child = currentMirror.children.first(where: { $0.label == component }) else {
                return nil
            }
            current = child.value
        }

        return current
    }

    /// Build a group key for grouping
    ///
    /// Uses JSON encoding to safely handle values containing special characters.
    /// This ensures that values like "John|Smith" are handled correctly.
    private func buildGroupKey(item: T, groupBy: [String]) -> GroupKey {
        var values: [GroupKeyValue] = []
        for field in groupBy {
            if let value = extractFieldValue(from: item, field: field) {
                values.append(GroupKeyValue(fieldName: field, value: value))
            } else {
                values.append(GroupKeyValue(fieldName: field, value: nil))
            }
        }
        return GroupKey(values: values)
    }

    /// Convert a GroupKey back into a dictionary for the result
    private func groupKeyToDictionary(_ key: GroupKey) -> [String: any Sendable] {
        var result: [String: any Sendable] = [:]
        for keyValue in key.values {
            if let value = keyValue.value {
                // Convert common types to Sendable
                if let str = value as? String {
                    result[keyValue.fieldName] = str
                } else if let int = value as? Int {
                    result[keyValue.fieldName] = int
                } else if let int64 = value as? Int64 {
                    result[keyValue.fieldName] = int64
                } else if let double = value as? Double {
                    result[keyValue.fieldName] = double
                } else if let bool = value as? Bool {
                    result[keyValue.fieldName] = bool
                } else {
                    result[keyValue.fieldName] = String(describing: value)
                }
            }
        }
        return result
    }
}

// MARK: - GroupKey

/// A hashable group key for aggregation grouping
///
/// Uses a proper struct instead of string concatenation to avoid
/// issues with separator characters in values.
private struct GroupKey: Hashable {
    let values: [GroupKeyValue]

    func hash(into hasher: inout Hasher) {
        for value in values {
            hasher.combine(value)
        }
    }

    static func == (lhs: GroupKey, rhs: GroupKey) -> Bool {
        guard lhs.values.count == rhs.values.count else { return false }
        for (l, r) in zip(lhs.values, rhs.values) {
            if l != r { return false }
        }
        return true
    }
}

/// A single value in a group key
private struct GroupKeyValue: Hashable {
    let fieldName: String
    let value: Any?

    func hash(into hasher: inout Hasher) {
        hasher.combine(fieldName)
        if let v = value {
            hasher.combine(String(describing: v))
        } else {
            hasher.combine("__NULL__")
        }
    }

    static func == (lhs: GroupKeyValue, rhs: GroupKeyValue) -> Bool {
        if lhs.fieldName != rhs.fieldName { return false }

        switch (lhs.value, rhs.value) {
        case (nil, nil):
            return true
        case (nil, _), (_, nil):
            return false
        case let (l?, r?):
            // Compare by type and value
            if let ls = l as? String, let rs = r as? String { return ls == rs }
            if let li = l as? Int, let ri = r as? Int { return li == ri }
            if let li = l as? Int64, let ri = r as? Int64 { return li == ri }
            if let ld = l as? Double, let rd = r as? Double { return ld == rd }
            if let lb = l as? Bool, let rb = r as? Bool { return lb == rb }
            // Fallback to string comparison
            return String(describing: l) == String(describing: r)
        }
    }
}

// MARK: - PredicateEvaluator

/// Evaluates predicates against items
///
/// Used by AggregationExecutor to filter items before aggregation.
public struct PredicateEvaluator<T: Persistable> {
    public init() {}

    /// Evaluate a predicate against an item
    public func evaluate(_ predicate: Predicate<T>, on item: T) -> Bool {
        switch predicate {
        case .comparison(let comparison):
            return evaluateComparison(comparison, on: item)

        case .and(let predicates):
            return predicates.allSatisfy { evaluate($0, on: item) }

        case .or(let predicates):
            return predicates.contains { evaluate($0, on: item) }

        case .not(let inner):
            return !evaluate(inner, on: item)

        case .true:
            return true

        case .false:
            return false
        }
    }

    private func evaluateComparison(_ comparison: FieldComparison<T>, on item: T) -> Bool {
        let modelValue = getFieldValue(from: item, keyPath: comparison.keyPath, fieldName: comparison.fieldName)

        switch comparison.op {
        case .isNil:
            return modelValue == nil

        case .isNotNil:
            return modelValue != nil

        default:
            break
        }

        guard let modelValue = modelValue else { return false }
        let expectedValue = comparison.value

        // Convert model value to FieldValue for type-safe comparison
        let modelFieldValue = toFieldValue(modelValue)

        switch comparison.op {
        case .equal:
            return modelFieldValue.isEqual(to: expectedValue)
        case .notEqual:
            return !modelFieldValue.isEqual(to: expectedValue)
        case .lessThan:
            return modelFieldValue.isLessThan(expectedValue)
        case .lessThanOrEqual:
            return modelFieldValue.isLessThan(expectedValue) || modelFieldValue.isEqual(to: expectedValue)
        case .greaterThan:
            return expectedValue.isLessThan(modelFieldValue)
        case .greaterThanOrEqual:
            return expectedValue.isLessThan(modelFieldValue) || modelFieldValue.isEqual(to: expectedValue)
        case .contains:
            if let str = modelValue as? String, let substr = expectedValue.stringValue {
                return str.contains(substr)
            }
            return false
        case .hasPrefix:
            if let str = modelValue as? String, let prefix = expectedValue.stringValue {
                return str.hasPrefix(prefix)
            }
            return false
        case .hasSuffix:
            if let str = modelValue as? String, let suffix = expectedValue.stringValue {
                return str.hasSuffix(suffix)
            }
            return false
        case .in:
            // Check if model value is in the expected array
            if let arrayValues = expectedValue.arrayValue {
                return arrayValues.contains { modelFieldValue.isEqual(to: $0) }
            }
            return false
        case .isNil, .isNotNil:
            return false
        }
    }

    /// Convert Any value to FieldValue
    private func toFieldValue(_ value: Any) -> FieldValue {
        switch value {
        case let v as Bool: return .bool(v)
        case let v as Int: return .int64(Int64(v))
        case let v as Int8: return .int64(Int64(v))
        case let v as Int16: return .int64(Int64(v))
        case let v as Int32: return .int64(Int64(v))
        case let v as Int64: return .int64(v)
        case let v as UInt: return .int64(Int64(v))
        case let v as UInt8: return .int64(Int64(v))
        case let v as UInt16: return .int64(Int64(v))
        case let v as UInt32: return .int64(Int64(v))
        case let v as UInt64: return .int64(Int64(bitPattern: v))
        case let v as Float: return .double(Double(v))
        case let v as Double: return .double(v)
        case let v as String: return .string(v)
        case let v as Data: return .data(v)
        case let v as UUID: return .string(v.uuidString)
        case let v as Date: return .double(v.timeIntervalSince1970)
        default:
            return .string(String(describing: value))
        }
    }

    private func getFieldValue(from item: T, keyPath: AnyKeyPath, fieldName: String) -> Any? {
        if let typedKeyPath = keyPath as? PartialKeyPath<T> {
            return item[keyPath: typedKeyPath]
        }
        return getFieldValueByReflection(from: item, fieldName: fieldName)
    }

    private func getFieldValueByReflection(from object: Any, fieldName: String) -> Any? {
        let components = fieldName.split(separator: ".").map(String.init)
        var current: Any = object

        for component in components {
            let mirror = Mirror(reflecting: current)
            guard let child = mirror.children.first(where: { $0.label == component }) else {
                return nil
            }
            current = child.value
        }

        return current
    }
}

// MARK: - FDBContext Extension

extension FDBContext {
    /// Create an aggregation executor for this context
    ///
    /// **Usage**:
    /// ```swift
    /// let executor = context.aggregationExecutor(for: Order.self)
    ///
    /// // Simple count
    /// let count = try await executor.executeCount()
    ///
    /// // Grouped aggregation
    /// let results = try await executor.executeGroupedAggregation(
    ///     aggregation: .sum(field: "amount"),
    ///     groupBy: ["region"]
    /// )
    /// ```
    public func aggregationExecutor<T: Persistable & Codable>(for type: T.Type) -> AggregationExecutor<T> {
        AggregationExecutor<T>(context: self)
    }
}

// MARK: - Convenience Extensions

extension AggregationExecutor {
    /// Execute a COUNT aggregation
    public func count(predicate: Predicate<T>? = nil) async throws -> Int64 {
        try await executeCount(predicate: predicate)
    }

    /// Execute a SUM aggregation
    public func sum(field: String, predicate: Predicate<T>? = nil) async throws -> Double {
        let result = try await executeSingleAggregation(
            aggregation: .sum(field: field),
            predicate: predicate
        )
        return result.doubleValue ?? 0
    }

    /// Execute an AVG aggregation
    public func avg(field: String, predicate: Predicate<T>? = nil) async throws -> Double {
        let result = try await executeSingleAggregation(
            aggregation: .avg(field: field),
            predicate: predicate
        )
        return result.doubleValue ?? 0
    }

    /// Execute a MIN aggregation
    public func min(field: String, predicate: Predicate<T>? = nil) async throws -> AggregationResult {
        try await executeSingleAggregation(
            aggregation: .min(field: field),
            predicate: predicate
        )
    }

    /// Execute a MAX aggregation
    public func max(field: String, predicate: Predicate<T>? = nil) async throws -> AggregationResult {
        try await executeSingleAggregation(
            aggregation: .max(field: field),
            predicate: predicate
        )
    }
}
