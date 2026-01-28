// AggregateEvaluator.swift
// GraphIndex - Evaluates QueryIR.AggregateFunction against groups of VariableBinding

import Foundation
import QueryIR
import Core
import DatabaseEngine

/// Evaluates QueryIR.AggregateFunction against groups of VariableBinding.
///
/// This bridges QueryIR's aggregate representation to GraphIndex's
/// VariableBinding-based evaluation. Used for GROUP BY / HAVING evaluation
/// when aggregates are represented as QueryIR types.
///
/// Follows SPARQL §18.5 aggregate semantics:
/// - Empty groups: COUNT = 0, others = null (error)
/// - Type promotion for SUM/AVG
/// - DISTINCT handling with set deduplication
public struct AggregateEvaluator: Sendable {

    private init() {}

    /// Evaluate an aggregate function over a group of bindings.
    ///
    /// - Parameters:
    ///   - aggregate: The QueryIR aggregate function to evaluate.
    ///   - bindings: The group of solution bindings.
    /// - Returns: The aggregate result, or `nil` on error (empty group for non-COUNT).
    public static func evaluate(
        _ aggregate: QueryIR.AggregateFunction,
        bindings: [VariableBinding]
    ) -> FieldValue? {
        switch aggregate {
        case .count(let expr, let distinct):
            return evaluateCount(expr: expr, distinct: distinct, bindings: bindings)
        case .sum(let expr, let distinct):
            return evaluateSum(expr: expr, distinct: distinct, bindings: bindings)
        case .avg(let expr, let distinct):
            return evaluateAvg(expr: expr, distinct: distinct, bindings: bindings)
        case .min(let expr):
            return evaluateMin(expr: expr, bindings: bindings)
        case .max(let expr):
            return evaluateMax(expr: expr, bindings: bindings)
        case .groupConcat(let expr, let separator, let distinct):
            return evaluateGroupConcat(
                expr: expr,
                separator: separator ?? " ",
                distinct: distinct,
                bindings: bindings
            )
        case .sample(let expr):
            return evaluateSample(expr: expr, bindings: bindings)
        case .arrayAgg:
            // SQL-specific, not supported in SPARQL context
            return nil
        }
    }

    // MARK: - Individual Aggregates

    private static func evaluateCount(
        expr: QueryIR.Expression?,
        distinct: Bool,
        bindings: [VariableBinding]
    ) -> FieldValue {
        if expr == nil {
            // COUNT(*) — count all rows
            return .int64(Int64(distinct ? Set(bindings).count : bindings.count))
        }
        let values = collectValues(expr: expr!, bindings: bindings)
        if distinct {
            return .int64(Int64(Set(values.map { FieldValueWrapper($0) }).count))
        }
        return .int64(Int64(values.count))
    }

    private static func evaluateSum(
        expr: QueryIR.Expression,
        distinct: Bool,
        bindings: [VariableBinding]
    ) -> FieldValue? {
        var values = collectNumericValues(expr: expr, bindings: bindings)
        if values.isEmpty { return nil }
        if distinct {
            values = deduplicateFieldValues(values)
        }
        return sumValues(values)
    }

    private static func evaluateAvg(
        expr: QueryIR.Expression,
        distinct: Bool,
        bindings: [VariableBinding]
    ) -> FieldValue? {
        var values = collectNumericValues(expr: expr, bindings: bindings)
        if values.isEmpty { return nil }
        if distinct {
            values = deduplicateFieldValues(values)
        }
        guard let sum = sumValues(values) else { return nil }
        let count = Double(values.count)
        switch sum {
        case .int64(let v): return .double(Double(v) / count)
        case .double(let v): return .double(v / count)
        default: return nil
        }
    }

    private static func evaluateMin(
        expr: QueryIR.Expression,
        bindings: [VariableBinding]
    ) -> FieldValue? {
        let values = collectValues(expr: expr, bindings: bindings)
        guard !values.isEmpty else { return nil }
        return values.reduce(values[0]) { current, next in
            if let cmp = next.compare(to: current), cmp == .orderedAscending {
                return next
            }
            return current
        }
    }

    private static func evaluateMax(
        expr: QueryIR.Expression,
        bindings: [VariableBinding]
    ) -> FieldValue? {
        let values = collectValues(expr: expr, bindings: bindings)
        guard !values.isEmpty else { return nil }
        return values.reduce(values[0]) { current, next in
            if let cmp = next.compare(to: current), cmp == .orderedDescending {
                return next
            }
            return current
        }
    }

    private static func evaluateGroupConcat(
        expr: QueryIR.Expression,
        separator: String,
        distinct: Bool,
        bindings: [VariableBinding]
    ) -> FieldValue? {
        var strings: [String] = []
        for binding in bindings {
            if let val = ExpressionEvaluator.evaluate(expr, binding: binding) {
                strings.append(stringRepresentation(val))
            }
        }
        if distinct {
            var seen = Set<String>()
            strings = strings.filter { seen.insert($0).inserted }
        }
        return .string(strings.joined(separator: separator))
    }

    private static func evaluateSample(
        expr: QueryIR.Expression,
        bindings: [VariableBinding]
    ) -> FieldValue? {
        for binding in bindings {
            if let val = ExpressionEvaluator.evaluate(expr, binding: binding),
               val != .null {
                return val
            }
        }
        return nil
    }

    // MARK: - Collection Helpers

    private static func collectValues(
        expr: QueryIR.Expression,
        bindings: [VariableBinding]
    ) -> [FieldValue] {
        bindings.compactMap { binding in
            let val = ExpressionEvaluator.evaluate(expr, binding: binding)
            guard let v = val, v != .null else { return nil }
            return v
        }
    }

    private static func collectNumericValues(
        expr: QueryIR.Expression,
        bindings: [VariableBinding]
    ) -> [FieldValue] {
        collectValues(expr: expr, bindings: bindings).compactMap { val in
            switch val {
            case .int64, .double:
                return val
            case .string(let s):
                // Numeric coercion
                if let d = Double(s) { return .double(d) }
                return nil
            default:
                return nil
            }
        }
    }

    private static func sumValues(_ values: [FieldValue]) -> FieldValue? {
        guard !values.isEmpty else { return nil }
        var hasDouble = false
        var intSum: Int64 = 0
        var doubleSum: Double = 0

        for val in values {
            switch val {
            case .int64(let v):
                if hasDouble {
                    doubleSum += Double(v)
                } else {
                    intSum += v
                }
            case .double(let v):
                if !hasDouble {
                    doubleSum = Double(intSum)
                    hasDouble = true
                }
                doubleSum += v
            default:
                continue
            }
        }
        return hasDouble ? .double(doubleSum) : .int64(intSum)
    }

    private static func deduplicateFieldValues(_ values: [FieldValue]) -> [FieldValue] {
        var seen = Set<FieldValueWrapper>()
        return values.filter { seen.insert(FieldValueWrapper($0)).inserted }
    }

    private static func stringRepresentation(_ value: FieldValue) -> String {
        switch value {
        case .string(let s): return s
        case .int64(let v): return String(v)
        case .double(let v): return String(v)
        case .bool(let v): return v ? "true" : "false"
        default: return String(describing: value)
        }
    }
}

// MARK: - FieldValue Wrapper (for Set deduplication)

/// Wrapper for FieldValue to use in Set operations.
/// FieldValue is Equatable and Hashable, so this wraps it for use
/// in generic contexts that need Hashable conformance.
private struct FieldValueWrapper: Hashable {
    let value: FieldValue

    init(_ value: FieldValue) {
        self.value = value
    }
}
