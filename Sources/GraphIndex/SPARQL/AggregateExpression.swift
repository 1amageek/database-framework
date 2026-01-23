// AggregateExpression.swift
// GraphIndex - SPARQL aggregate expressions
//
// Represents aggregate functions for GROUP BY queries.
//
// Reference: W3C SPARQL 1.1, Section 11 (Aggregates)

import Foundation

/// Aggregate expression for SPARQL GROUP BY queries
///
/// **Design**: Represents aggregate functions that operate over groups of bindings.
/// Each aggregate is applied to a specific variable or expression.
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
    ///
    /// - Parameters:
    ///   - variable: Variable to count (nil for COUNT(*))
    ///   - distinct: Whether to count only distinct values
    ///   - alias: Result variable name
    case count(variable: String?, distinct: Bool, alias: String)

    /// SUM aggregate: sum numeric values
    ///
    /// - Parameters:
    ///   - variable: Variable containing numeric values
    ///   - alias: Result variable name
    case sum(variable: String, alias: String)

    /// AVG aggregate: average of numeric values
    ///
    /// - Parameters:
    ///   - variable: Variable containing numeric values
    ///   - alias: Result variable name
    case avg(variable: String, alias: String)

    /// MIN aggregate: minimum value (lexicographic for strings, numeric for numbers)
    ///
    /// - Parameters:
    ///   - variable: Variable to find minimum
    ///   - alias: Result variable name
    case min(variable: String, alias: String)

    /// MAX aggregate: maximum value
    ///
    /// - Parameters:
    ///   - variable: Variable to find maximum
    ///   - alias: Result variable name
    case max(variable: String, alias: String)

    /// SAMPLE aggregate: any single value from the group
    ///
    /// - Parameters:
    ///   - variable: Variable to sample
    ///   - alias: Result variable name
    case sample(variable: String, alias: String)

    /// GROUP_CONCAT aggregate: concatenate all values in the group
    ///
    /// - Parameters:
    ///   - variable: Variable to concatenate
    ///   - separator: Separator string (default is space)
    ///   - distinct: Whether to concatenate only distinct values
    ///   - alias: Result variable name
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
    /// - Returns: The aggregate result as a string
    public func evaluate(_ bindings: [VariableBinding]) -> String? {
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

    private func evaluateCount(_ bindings: [VariableBinding], variable: String?, distinct: Bool) -> String {
        if let variable = variable {
            // Count non-null values for the variable
            let values = bindings.compactMap { $0[variable] }
            if distinct {
                return String(Set(values).count)
            }
            return String(values.count)
        } else {
            // COUNT(*) - count all bindings
            if distinct {
                // For distinct COUNT(*), we need to compare entire bindings
                let uniqueBindings = Set(bindings)
                return String(uniqueBindings.count)
            }
            return String(bindings.count)
        }
    }

    private func evaluateSum(_ bindings: [VariableBinding], variable: String) -> String? {
        let values = bindings.compactMap { $0[variable] }

        // Try to sum as Double
        var sum: Double = 0
        for value in values {
            if let num = Double(value) {
                sum += num
            }
        }

        // Return as integer if possible
        if sum == sum.rounded() && sum >= Double(Int.min) && sum <= Double(Int.max) {
            return String(Int(sum))
        }
        return String(sum)
    }

    private func evaluateAvg(_ bindings: [VariableBinding], variable: String) -> String? {
        let values = bindings.compactMap { binding -> Double? in
            guard let str = binding[variable] else { return nil }
            return Double(str)
        }

        guard !values.isEmpty else { return nil }

        let avg = values.reduce(0, +) / Double(values.count)
        return String(avg)
    }

    private func evaluateMin(_ bindings: [VariableBinding], variable: String) -> String? {
        let values = bindings.compactMap { $0[variable] }
        guard !values.isEmpty else { return nil }

        // Try numeric comparison first
        let numericValues = values.compactMap { Double($0) }
        if numericValues.count == values.count, let minNum = numericValues.min() {
            if minNum == minNum.rounded() && minNum >= Double(Int.min) && minNum <= Double(Int.max) {
                return String(Int(minNum))
            }
            return String(minNum)
        }

        // Fall back to string comparison
        return values.min()
    }

    private func evaluateMax(_ bindings: [VariableBinding], variable: String) -> String? {
        let values = bindings.compactMap { $0[variable] }
        guard !values.isEmpty else { return nil }

        // Try numeric comparison first
        let numericValues = values.compactMap { Double($0) }
        if numericValues.count == values.count, let maxNum = numericValues.max() {
            if maxNum == maxNum.rounded() && maxNum >= Double(Int.min) && maxNum <= Double(Int.max) {
                return String(Int(maxNum))
            }
            return String(maxNum)
        }

        // Fall back to string comparison
        return values.max()
    }

    private func evaluateSample(_ bindings: [VariableBinding], variable: String) -> String? {
        // Return any value (first non-null)
        return bindings.compactMap { $0[variable] }.first
    }

    private func evaluateGroupConcat(_ bindings: [VariableBinding], variable: String, separator: String, distinct: Bool) -> String {
        var values = bindings.compactMap { $0[variable] }

        if distinct {
            // Preserve order while removing duplicates
            var seen = Set<String>()
            values = values.filter { seen.insert($0).inserted }
        }

        return values.joined(separator: separator)
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
