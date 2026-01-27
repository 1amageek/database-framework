// AggregateExpression.swift
// GraphIndex - SPARQL aggregate expressions
//
// Represents aggregate functions for GROUP BY queries.
//
// Reference: W3C SPARQL 1.1, Section 11 (Aggregates)

import Foundation
import Core

/// Aggregate expression for SPARQL GROUP BY queries
///
/// **Design**: Represents aggregate functions that operate over groups of bindings.
/// Each aggregate is applied to a specific variable or expression.
/// Returns `FieldValue` to preserve type information in results.
///
/// **Usage**:
/// ```swift
/// // Count all members
/// AggregateExpression.count(nil, distinct: false, alias: "total")
///
/// // Count distinct values
/// AggregateExpression.count("?name", distinct: true, alias: "uniqueNames")
///
/// // Sum, Average, Min, Max
/// AggregateExpression.sum("?age", alias: "totalAge")
/// AggregateExpression.avg("?salary", alias: "avgSalary")
/// ```
///
/// **Reference**: W3C SPARQL 1.1, Section 11 (Aggregates)
public enum AggregateExpression: Sendable, Hashable {

    // MARK: - Aggregate Functions

    /// COUNT aggregate: count number of bindings (or non-null values if variable specified)
    case count(variable: String?, distinct: Bool, alias: String)

    /// SUM aggregate: sum numeric values
    case sum(variable: String, alias: String)

    /// AVG aggregate: average of numeric values
    case avg(variable: String, alias: String)

    /// MIN aggregate: minimum value
    case min(variable: String, alias: String)

    /// MAX aggregate: maximum value
    case max(variable: String, alias: String)

    /// SAMPLE aggregate: any single value from the group
    case sample(variable: String, alias: String)

    /// GROUP_CONCAT aggregate: concatenate all values in the group
    case groupConcat(variable: String, separator: String, distinct: Bool, alias: String)

    // MARK: - Properties

    /// The output alias for this aggregate
    public var alias: String {
        switch self {
        case .count(_, _, let alias),
             .sum(_, let alias),
             .avg(_, let alias),
             .min(_, let alias),
             .max(_, let alias),
             .sample(_, let alias),
             .groupConcat(_, _, _, let alias):
            return alias
        }
    }

    /// The input variable (nil for COUNT(*))
    public var inputVariable: String? {
        switch self {
        case .count(let variable, _, _):
            return variable
        case .sum(let variable, _),
             .avg(let variable, _),
             .min(let variable, _),
             .max(let variable, _),
             .sample(let variable, _),
             .groupConcat(let variable, _, _, _):
            return variable
        }
    }

    /// Whether this aggregate uses DISTINCT
    public var isDistinct: Bool {
        switch self {
        case .count(_, let distinct, _):
            return distinct
        case .groupConcat(_, _, let distinct, _):
            return distinct
        default:
            return false
        }
    }

    // MARK: - Evaluation

    /// Evaluate this aggregate over a group of bindings
    ///
    /// - Parameter bindings: Array of variable bindings in the group
    /// - Returns: The aggregate result as a FieldValue
    public func evaluate(_ bindings: [VariableBinding]) -> FieldValue? {
        switch self {
        case .count(let variable, let distinct, _):
            return evaluateCount(bindings, variable: variable, distinct: distinct)

        case .sum(let variable, _):
            return evaluateSum(bindings, variable: variable)

        case .avg(let variable, _):
            return evaluateAvg(bindings, variable: variable)

        case .min(let variable, _):
            return evaluateMin(bindings, variable: variable)

        case .max(let variable, _):
            return evaluateMax(bindings, variable: variable)

        case .sample(let variable, _):
            return evaluateSample(bindings, variable: variable)

        case .groupConcat(let variable, let separator, let distinct, _):
            return evaluateGroupConcat(bindings, variable: variable, separator: separator, distinct: distinct)
        }
    }

    // MARK: - Private Evaluation Methods

    private func evaluateCount(_ bindings: [VariableBinding], variable: String?, distinct: Bool) -> FieldValue {
        if let variable = variable {
            // Count non-null values for the variable
            let values = bindings.compactMap { $0[variable] }
            if distinct {
                return .int64(Int64(Set(values).count))
            }
            return .int64(Int64(values.count))
        } else {
            // COUNT(*) - count all bindings
            if distinct {
                let uniqueBindings = Set(bindings)
                return .int64(Int64(uniqueBindings.count))
            }
            return .int64(Int64(bindings.count))
        }
    }

    private func evaluateSum(_ bindings: [VariableBinding], variable: String) -> FieldValue? {
        var sum: Double = 0
        var hasValue = false
        for binding in bindings {
            guard let value = binding[variable],
                  let num = Self.numericValue(value) else { continue }
            sum += num
            hasValue = true
        }

        guard hasValue else { return nil }

        // Return as integer if possible.
        // Int64(exactly:) returns nil for non-integer values and values outside
        // Int64 range, avoiding the trap that Int64(_:) causes at boundary values
        // (Double(Int64.max) rounds to 9223372036854775808.0 > Int64.max).
        if let intValue = Int64(exactly: sum) {
            return .int64(intValue)
        }
        return .double(sum)
    }

    private func evaluateAvg(_ bindings: [VariableBinding], variable: String) -> FieldValue? {
        var sum: Double = 0
        var count = 0
        for binding in bindings {
            guard let value = binding[variable],
                  let num = Self.numericValue(value) else { continue }
            sum += num
            count += 1
        }

        guard count > 0 else { return nil }

        return .double(sum / Double(count))
    }

    private func evaluateMin(_ bindings: [VariableBinding], variable: String) -> FieldValue? {
        let values = bindings.compactMap { $0[variable] }.map { Self.promoteToNumeric($0) }
        return values.min()
    }

    private func evaluateMax(_ bindings: [VariableBinding], variable: String) -> FieldValue? {
        let values = bindings.compactMap { $0[variable] }.map { Self.promoteToNumeric($0) }
        return values.max()
    }

    // MARK: - Numeric Coercion

    /// Extract numeric value from a FieldValue, with string-to-number coercion
    ///
    /// SPARQL semantics: SUM/AVG aggregate functions operate on numeric values.
    /// String values that represent numbers are coerced to numeric.
    /// Non-numeric values are skipped.
    ///
    /// Reference: SPARQL 1.1, Section 11.5
    private static func numericValue(_ value: FieldValue) -> Double? {
        switch value {
        case .int64(let v): return Double(v)
        case .double(let v): return v
        case .string(let s): return Double(s)
        default: return nil
        }
    }

    /// Promote a FieldValue to numeric if it's a numeric-looking string
    ///
    /// Used by MIN/MAX to ensure numeric ordering for string-stored numbers.
    /// Non-numeric strings are left unchanged.
    private static func promoteToNumeric(_ value: FieldValue) -> FieldValue {
        guard case .string(let s) = value else { return value }
        if let i = Int64(s) { return .int64(i) }
        if let d = Double(s), d.isFinite { return .double(d) }
        return value
    }

    private func evaluateSample(_ bindings: [VariableBinding], variable: String) -> FieldValue? {
        // Return any value (first non-null)
        return bindings.compactMap { $0[variable] }.first
    }

    private func evaluateGroupConcat(_ bindings: [VariableBinding], variable: String, separator: String, distinct: Bool) -> FieldValue {
        var strings = bindings.compactMap { binding -> String? in
            binding.string(variable)
        }

        if distinct {
            // Preserve order while removing duplicates
            var seen = Set<String>()
            strings = strings.filter { seen.insert($0).inserted }
        }

        return .string(strings.joined(separator: separator))
    }
}

// MARK: - Convenience Constructors

extension AggregateExpression {

    /// Create COUNT(*) aggregate
    public static func countAll(as alias: String) -> AggregateExpression {
        .count(variable: nil, distinct: false, alias: alias)
    }

    /// Create COUNT(DISTINCT *) aggregate
    public static func countAllDistinct(as alias: String) -> AggregateExpression {
        .count(variable: nil, distinct: true, alias: alias)
    }

    /// Create COUNT(?var) aggregate
    public static func count(_ variable: String, as alias: String) -> AggregateExpression {
        .count(variable: variable, distinct: false, alias: alias)
    }

    /// Create COUNT(DISTINCT ?var) aggregate
    public static func countDistinct(_ variable: String, as alias: String) -> AggregateExpression {
        .count(variable: variable, distinct: true, alias: alias)
    }

    /// Create SUM(?var) aggregate
    public static func sum(_ variable: String, as alias: String) -> AggregateExpression {
        .sum(variable: variable, alias: alias)
    }

    /// Create AVG(?var) aggregate
    public static func avg(_ variable: String, as alias: String) -> AggregateExpression {
        .avg(variable: variable, alias: alias)
    }

    /// Create MIN(?var) aggregate
    public static func min(_ variable: String, as alias: String) -> AggregateExpression {
        .min(variable: variable, alias: alias)
    }

    /// Create MAX(?var) aggregate
    public static func max(_ variable: String, as alias: String) -> AggregateExpression {
        .max(variable: variable, alias: alias)
    }

    /// Create SAMPLE(?var) aggregate
    public static func sample(_ variable: String, as alias: String) -> AggregateExpression {
        .sample(variable: variable, alias: alias)
    }

    /// Create GROUP_CONCAT(?var; separator=",") aggregate
    public static func groupConcat(_ variable: String, separator: String = " ", as alias: String) -> AggregateExpression {
        .groupConcat(variable: variable, separator: separator, distinct: false, alias: alias)
    }

    /// Create GROUP_CONCAT(DISTINCT ?var; separator=",") aggregate
    public static func groupConcatDistinct(_ variable: String, separator: String = " ", as alias: String) -> AggregateExpression {
        .groupConcat(variable: variable, separator: separator, distinct: true, alias: alias)
    }
}

// MARK: - CustomStringConvertible

extension AggregateExpression: CustomStringConvertible {
    public var description: String {
        switch self {
        case .count(let variable, let distinct, let alias):
            let distinctStr = distinct ? "DISTINCT " : ""
            let varStr = variable ?? "*"
            return "(COUNT(\(distinctStr)\(varStr)) AS \(alias))"

        case .sum(let variable, let alias):
            return "(SUM(\(variable)) AS \(alias))"

        case .avg(let variable, let alias):
            return "(AVG(\(variable)) AS \(alias))"

        case .min(let variable, let alias):
            return "(MIN(\(variable)) AS \(alias))"

        case .max(let variable, let alias):
            return "(MAX(\(variable)) AS \(alias))"

        case .sample(let variable, let alias):
            return "(SAMPLE(\(variable)) AS \(alias))"

        case .groupConcat(let variable, let separator, let distinct, let alias):
            let distinctStr = distinct ? "DISTINCT " : ""
            return "(GROUP_CONCAT(\(distinctStr)\(variable); separator=\"\(separator)\") AS \(alias))"
        }
    }
}
