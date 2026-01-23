// AggregationPlanExecutor.swift
// QueryPlanner - Aggregation query execution

import Foundation
import Core
import FoundationDB

/// Errors that can occur during aggregation execution
public enum AggregationExecutionError: Error, Sendable {
    case indexNotFound(String)
    case unsupportedAggregationType(String)
    case typeMismatch(expected: String, actual: String)
    case noDataAvailable
}

extension AggregationExecutionError: CustomStringConvertible {
    public var description: String {
        switch self {
        case .indexNotFound(let name):
            return "Aggregation index not found: \(name)"
        case .unsupportedAggregationType(let type):
            return "Unsupported aggregation type: \(type)"
        case .typeMismatch(let expected, let actual):
            return "Type mismatch: expected \(expected), got \(actual)"
        case .noDataAvailable:
            return "No data available for aggregation"
        }
    }
}

/// Executes aggregation query plans
///
/// Supports both index-backed O(1) aggregations and scan-based aggregations.
///
/// **Usage**:
/// ```swift
/// let executor = AggregationPlanExecutor<User>(
///     context: context,
///     executionContext: context
/// )
/// let results = try await executor.execute(plan: aggregationPlan)
/// ```
///
/// **Index-Backed Aggregations**:
/// When aggregation indexes exist, the executor performs O(1) lookups:
/// - SUM: Read from SumIndexKind
/// - MIN/MAX: Read from MinMaxIndexKind
/// - COUNT DISTINCT: Read from DistinctIndexKind (HyperLogLog)
/// - PERCENTILE: Read from PercentileIndexKind (T-Digest)
///
/// **Scan-Based Aggregations**:
/// When no suitable index exists, the executor scans records:
/// - Streams records using QueryExecutionContext
/// - Computes aggregations incrementally
/// - Supports GROUP BY with hash aggregation
public final class AggregationPlanExecutor<T: Persistable & Codable>: @unchecked Sendable {

    private let context: FDBContext
    private let executionContext: any QueryExecutionContext

    public init(context: FDBContext, executionContext: any QueryExecutionContext) {
        self.context = context
        self.executionContext = executionContext
    }

    /// Execute an aggregation plan
    ///
    /// - Parameter plan: The aggregation plan to execute
    /// - Returns: Array of aggregation results (one per group, or single result for global)
    public func execute(plan: AggregationPlan<T>) async throws -> [AggregationResult] {
        // Check if we have GROUP BY
        if plan.groupByFields.isEmpty {
            // Global aggregation
            return try await executeGlobalAggregation(plan: plan)
        } else {
            // Grouped aggregation
            return try await executeGroupedAggregation(plan: plan)
        }
    }

    // MARK: - Global Aggregation

    /// Execute global (non-grouped) aggregation
    private func executeGlobalAggregation(plan: AggregationPlan<T>) async throws -> [AggregationResult] {
        var results: [AggregationResult] = []

        for (index, spec) in plan.aggregations.enumerated() {
            let strategy = plan.strategies[index]

            let value = try await executeAggregation(
                type: spec.type,
                strategy: strategy,
                filterPredicate: plan.filterPredicate
            )

            results.append(AggregationResult(
                aggregationType: spec.type,
                value: value,
                groupKey: [:]
            ))
        }

        return results
    }

    /// Execute a single aggregation operation
    private func executeAggregation(
        type: AggregationType,
        strategy: AggregationStrategy,
        filterPredicate: Predicate<T>?
    ) async throws -> any Sendable {
        switch strategy {
        case .indexLookup(let indexName):
            return try await executeIndexLookup(type: type, indexName: indexName)

        case .hyperLogLog(let indexName):
            return try await executeHyperLogLogLookup(indexName: indexName)

        case .percentileIndex(let indexName):
            if case .percentile(_, let p) = type {
                return try await executePercentileLookup(indexName: indexName, percentile: p)
            }
            throw AggregationExecutionError.unsupportedAggregationType("percentile without value")

        case .scanAndCompute:
            return try await executeScanAggregation(type: type, filterPredicate: filterPredicate)
        }
    }

    /// Execute index-backed aggregation lookup
    private func executeIndexLookup(type: AggregationType, indexName: String) async throws -> any Sendable {
        let typeSubspace = try await context.indexQueryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(indexName)

        // Read aggregation value from index
        // The value key is typically at the root of the index subspace
        let valueKey = indexSubspace.pack(Tuple("_value"))

        let valueData = try await context.container.database.withTransaction(configuration: .batch) { transaction in
            try await transaction.getValue(for: valueKey)
        }

        guard let data = valueData else {
            // No data means the aggregation hasn't been computed yet
            // Return appropriate zero value
            switch type {
            case .count:
                return Int64(0)
            case .sum:
                return Int64(0)
            case .min, .max:
                return NullValue.instance
            case .avg:
                return NullValue.instance
            case .distinct:
                return Int64(0)
            case .percentile:
                return NullValue.instance
            }
        }

        // Decode value based on aggregation type
        switch type {
        case .count, .sum, .distinct:
            if let intValue = decodeInt64(from: data) {
                return intValue
            }
            return NullValue.instance

        case .min, .max, .avg:
            if let doubleValue = decodeDouble(from: data) {
                return doubleValue
            }
            if let intValue = decodeInt64(from: data) {
                return Double(intValue)
            }
            return NullValue.instance

        case .percentile:
            if let doubleValue = decodeDouble(from: data) {
                return doubleValue
            }
            return NullValue.instance
        }
    }

    /// Execute HyperLogLog lookup for COUNT DISTINCT
    private func executeHyperLogLogLookup(indexName: String) async throws -> Int64 {
        let typeSubspace = try await context.indexQueryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(indexName)

        // HyperLogLog data is stored in a special key
        let hllKey = indexSubspace.pack(Tuple("_hll"))

        let hllData = try await context.container.database.withTransaction(configuration: .batch) { transaction in
            try await transaction.getValue(for: hllKey)
        }

        guard let data = hllData else {
            return 0
        }

        // Decode HyperLogLog and get cardinality
        let decoder = JSONDecoder()
        if let hll = try? decoder.decode(HyperLogLog.self, from: Data(data)) {
            return hll.cardinality()
        }

        return 0
    }

    /// Execute percentile lookup from T-Digest index
    private func executePercentileLookup(indexName: String, percentile: Double) async throws -> any Sendable {
        let typeSubspace = try await context.indexQueryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(indexName)

        // Percentile data is stored in a T-Digest structure
        let digestKey = indexSubspace.pack(Tuple("_digest"))

        let digestData = try await context.container.database.withTransaction(configuration: .batch) { transaction in
            try await transaction.getValue(for: digestKey)
        }

        guard let data = digestData else {
            return NullValue.instance
        }

        // Decode T-Digest and compute percentile
        // Note: TDigest implementation would be needed here
        // For now, return null as placeholder
        _ = data
        return NullValue.instance
    }

    /// Execute scan-based aggregation
    private func executeScanAggregation(
        type: AggregationType,
        filterPredicate: Predicate<T>?
    ) async throws -> any Sendable {
        // Scan all records and compute aggregation
        var accumulator = AggregationAccumulator(type: type)

        for try await record in executionContext.streamRecords(type: T.self) {
            // Apply filter if present
            if let predicate = filterPredicate {
                if !evaluatePredicate(predicate, on: record) {
                    continue
                }
            }

            // Update accumulator
            accumulator.add(record: record)
        }

        return accumulator.result()
    }

    // MARK: - Grouped Aggregation

    /// Execute grouped aggregation with hash aggregation
    private func executeGroupedAggregation(plan: AggregationPlan<T>) async throws -> [AggregationResult] {
        // Use hash aggregation: group key -> accumulators
        var groups: [GroupKey: [AggregationAccumulator]] = [:]

        for try await record in executionContext.streamRecords(type: T.self) {
            // Apply filter if present
            if let predicate = plan.filterPredicate {
                if !evaluatePredicate(predicate, on: record) {
                    continue
                }
            }

            // Extract group key
            let groupKey = extractGroupKey(from: record, fields: plan.groupByFields)

            // Get or create accumulators for this group
            if groups[groupKey] == nil {
                groups[groupKey] = plan.aggregations.map {
                    AggregationAccumulator(type: $0.type)
                }
            }

            // Update all accumulators for this group
            for (index, _) in plan.aggregations.enumerated() {
                groups[groupKey]![index].add(record: record)
            }
        }

        // Convert to results - create one AggregationResult per aggregation per group
        var results: [AggregationResult] = []
        for (key, accumulators) in groups {
            // Convert FieldValue group key to Sendable
            let sendableGroupKey: [String: any Sendable] = key.values.mapValues { fieldValue -> any Sendable in
                switch fieldValue {
                case .string(let s): return s
                case .int64(let i): return i
                case .double(let d): return d
                case .bool(let b): return b
                case .null: return NullValue.instance
                default: return String(describing: fieldValue)
                }
            }

            for (index, spec) in plan.aggregations.enumerated() {
                results.append(AggregationResult(
                    aggregationType: spec.type,
                    value: accumulators[index].result(),
                    groupKey: sendableGroupKey
                ))
            }
        }
        return results
    }

    /// Extract group key from a record
    private func extractGroupKey(from record: T, fields: [String]) -> GroupKey {
        var values: [String: FieldValue] = [:]
        for field in fields {
            if let rawValue = record[dynamicMember: field] {
                values[field] = FieldValue(rawValue) ?? .null
            } else {
                values[field] = .null
            }
        }
        return GroupKey(values: values)
    }

    // MARK: - Helpers

    /// Describe an aggregation type for result naming
    private func describeAggregation(_ type: AggregationType) -> String {
        switch type {
        case .count:
            return "COUNT(*)"
        case .sum(let field):
            return "SUM(\(field))"
        case .min(let field):
            return "MIN(\(field))"
        case .max(let field):
            return "MAX(\(field))"
        case .avg(let field):
            return "AVG(\(field))"
        case .distinct(let field):
            return "COUNT(DISTINCT \(field))"
        case .percentile(let field, let p):
            return "PERCENTILE(\(field), \(p))"
        }
    }

    /// Evaluate a predicate against a record
    private func evaluatePredicate(_ predicate: Predicate<T>, on record: T) -> Bool {
        switch predicate {
        case .comparison(let comparison):
            return evaluateComparison(comparison, on: record)
        case .and(let predicates):
            return predicates.allSatisfy { evaluatePredicate($0, on: record) }
        case .or(let predicates):
            return predicates.contains { evaluatePredicate($0, on: record) }
        case .not(let inner):
            return !evaluatePredicate(inner, on: record)
        case .true:
            return true
        case .false:
            return false
        }
    }

    /// Evaluate a field comparison
    private func evaluateComparison(_ comparison: FieldComparison<T>, on record: T) -> Bool {
        guard let rawValue = record[dynamicMember: comparison.fieldName] else {
            return comparison.op == .isNil
        }

        let fieldValue = FieldValue(rawValue) ?? .null

        switch comparison.op {
        case .equal:
            return fieldValue.isEqual(to: comparison.value)
        case .notEqual:
            return !fieldValue.isEqual(to: comparison.value)
        case .lessThan:
            return fieldValue.isLessThan(comparison.value)
        case .lessThanOrEqual:
            return fieldValue.isLessThan(comparison.value) || fieldValue.isEqual(to: comparison.value)
        case .greaterThan:
            return comparison.value.isLessThan(fieldValue)
        case .greaterThanOrEqual:
            return comparison.value.isLessThan(fieldValue) || fieldValue.isEqual(to: comparison.value)
        case .isNil:
            return fieldValue.isNull
        case .isNotNil:
            return !fieldValue.isNull
        default:
            return false
        }
    }

    /// Decode Int64 from bytes
    private func decodeInt64(from data: [UInt8]) -> Int64? {
        guard data.count >= 8 else { return nil }
        return data.withUnsafeBytes { $0.load(as: Int64.self) }
    }

    /// Decode Double from bytes
    private func decodeDouble(from data: [UInt8]) -> Double? {
        guard data.count >= 8 else { return nil }
        return data.withUnsafeBytes { $0.load(as: Double.self) }
    }
}

// MARK: - Group Key

/// Hashable group key for hash aggregation
private struct GroupKey: Hashable {
    let values: [String: FieldValue]

    func hash(into hasher: inout Hasher) {
        for (key, value) in values.sorted(by: { $0.key < $1.key }) {
            hasher.combine(key)
            hasher.combine(value)
        }
    }

    static func == (lhs: GroupKey, rhs: GroupKey) -> Bool {
        lhs.values == rhs.values
    }
}

// MARK: - Aggregation Accumulator

/// Accumulator for incrementally computing aggregations
private struct AggregationAccumulator {
    let type: AggregationType

    // State for different aggregation types
    private var count: Int64 = 0
    private var sum: Double = 0
    private var min: FieldValue?
    private var max: FieldValue?
    private var distinctSet: Set<FieldValue> = []

    init(type: AggregationType) {
        self.type = type
    }

    mutating func add<T: Persistable>(record: T) {
        count += 1

        switch type {
        case .count:
            // Already incremented count
            break

        case .sum(let field):
            if let rawValue = record[dynamicMember: field],
               let numericValue = toDouble(rawValue) {
                sum += numericValue
            }

        case .min(let field):
            if let rawValue = record[dynamicMember: field] {
                let fieldValue = FieldValue(rawValue) ?? .null
                if let currentMin = min {
                    if fieldValue.isLessThan(currentMin) {
                        min = fieldValue
                    }
                } else {
                    min = fieldValue
                }
            }

        case .max(let field):
            if let rawValue = record[dynamicMember: field] {
                let fieldValue = FieldValue(rawValue) ?? .null
                if let currentMax = max {
                    if currentMax.isLessThan(fieldValue) {
                        max = fieldValue
                    }
                } else {
                    max = fieldValue
                }
            }

        case .avg(let field):
            if let rawValue = record[dynamicMember: field],
               let numericValue = toDouble(rawValue) {
                sum += numericValue
            }

        case .distinct(let field):
            if let rawValue = record[dynamicMember: field] {
                let fieldValue = FieldValue(rawValue) ?? .null
                distinctSet.insert(fieldValue)
            }

        case .percentile:
            // Percentile requires more complex streaming algorithm
            // Would need T-Digest or reservoir sampling
            break
        }
    }

    func result() -> any Sendable {
        switch type {
        case .count:
            return count

        case .sum:
            return sum

        case .min:
            guard let minValue = min else { return NullValue.instance }
            return fieldValueToSendable(minValue)

        case .max:
            guard let maxValue = max else { return NullValue.instance }
            return fieldValueToSendable(maxValue)

        case .avg:
            guard count > 0 else { return NullValue.instance }
            return sum / Double(count)

        case .distinct:
            return Int64(distinctSet.count)

        case .percentile:
            // Would need proper streaming percentile algorithm
            return NullValue.instance
        }
    }

    private func toDouble(_ value: Any) -> Double? {
        switch value {
        case let v as Double: return v
        case let v as Float: return Double(v)
        case let v as Int: return Double(v)
        case let v as Int64: return Double(v)
        case let v as Int32: return Double(v)
        default: return nil
        }
    }

    private func fieldValueToSendable(_ value: FieldValue) -> any Sendable {
        switch value {
        case .int64(let v):
            return v
        case .double(let v):
            return v
        case .string(let s):
            return s
        case .bool(let b):
            return b
        default:
            return NullValue.instance
        }
    }
}
