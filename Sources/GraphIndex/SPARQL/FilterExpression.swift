// FilterExpression.swift
// GraphIndex - SPARQL-like filter expressions
//
// Represents filter conditions that can be applied to bindings.

import Foundation

/// Filter expression for FILTER clauses
///
/// **Design**: Recursive expression tree supporting common SPARQL filter operations.
/// Expressions are evaluated against a VariableBinding to produce a boolean result.
///
/// **Usage**:
/// ```swift
/// // Simple equality
/// let filter = FilterExpression.equals("?age", "18")
///
/// // Compound expression
/// let filter = FilterExpression.and(
///     .bound("?email"),
///     .notEquals("?status", "inactive")
/// )
///
/// // Custom predicate
/// let filter = FilterExpression.custom { binding in
///     guard let age = binding.int("?age") else { return false }
///     return age >= 18
/// }
/// ```
///
/// **Reference**: W3C SPARQL 1.1, Section 17 (Expressions and Testing Values)
public indirect enum FilterExpression: Sendable {

    // MARK: - Comparison

    /// Variable equals value: ?var == "value"
    case equals(String, String)

    /// Variable not equals value: ?var != "value"
    case notEquals(String, String)

    /// Variable less than value: ?var < "value" (string comparison)
    case lessThan(String, String)

    /// Variable less than or equal: ?var <= "value"
    case lessThanOrEqual(String, String)

    /// Variable greater than: ?var > "value"
    case greaterThan(String, String)

    /// Variable greater than or equal: ?var >= "value"
    case greaterThanOrEqual(String, String)

    // MARK: - Variable Comparison

    /// Two variables are equal: ?var1 == ?var2
    case variableEquals(String, String)

    /// Two variables are not equal: ?var1 != ?var2
    case variableNotEquals(String, String)

    // MARK: - Bound Check

    /// Variable is bound (not null): BOUND(?var)
    case bound(String)

    /// Variable is not bound (null): !BOUND(?var)
    case notBound(String)

    // MARK: - String Operations

    /// Variable matches regex: REGEX(?var, "pattern")
    case regex(String, String)

    /// Variable matches regex with flags: REGEX(?var, "pattern", "i")
    case regexWithFlags(String, String, String)

    /// Variable contains substring: CONTAINS(?var, "substr")
    case contains(String, String)

    /// Variable starts with prefix: STRSTARTS(?var, "prefix")
    case startsWith(String, String)

    /// Variable ends with suffix: STRENDS(?var, "suffix")
    case endsWith(String, String)

    // MARK: - Logical Operations

    /// Logical AND: expr1 && expr2
    case and(FilterExpression, FilterExpression)

    /// Logical OR: expr1 || expr2
    case or(FilterExpression, FilterExpression)

    /// Logical NOT: !expr
    case not(FilterExpression)

    // MARK: - Custom

    /// Custom predicate using closure
    ///
    /// For complex filters that can't be expressed with built-in operations.
    case custom(@Sendable (VariableBinding) -> Bool)

    /// Always true (identity for AND)
    case alwaysTrue

    /// Always false (identity for OR)
    case alwaysFalse

    // MARK: - Evaluation

    /// Evaluate the filter against a binding
    ///
    /// - Parameter binding: The variable binding to evaluate against
    /// - Returns: `true` if the filter matches, `false` otherwise
    public func evaluate(_ binding: VariableBinding) -> Bool {
        switch self {
        // Comparison with literal
        // SPARQL semantics: unbound variables evaluate to false in comparisons
        case .equals(let variable, let value):
            guard let v = binding[variable] else { return false }
            return v == value

        case .notEquals(let variable, let value):
            guard let v = binding[variable] else { return false }
            return v != value

        case .lessThan(let variable, let value):
            guard let v = binding[variable] else { return false }
            return v < value

        case .lessThanOrEqual(let variable, let value):
            guard let v = binding[variable] else { return false }
            return v <= value

        case .greaterThan(let variable, let value):
            guard let v = binding[variable] else { return false }
            return v > value

        case .greaterThanOrEqual(let variable, let value):
            guard let v = binding[variable] else { return false }
            return v >= value

        // Variable comparison
        case .variableEquals(let var1, let var2):
            guard let v1 = binding[var1], let v2 = binding[var2] else { return false }
            return v1 == v2

        case .variableNotEquals(let var1, let var2):
            guard let v1 = binding[var1], let v2 = binding[var2] else { return false }
            return v1 != v2

        // Bound check
        case .bound(let variable):
            return binding.isBound(variable)

        case .notBound(let variable):
            return !binding.isBound(variable)

        // String operations
        case .regex(let variable, let pattern):
            guard let value = binding[variable] else { return false }
            return matchesRegex(value, pattern: pattern, flags: "")

        case .regexWithFlags(let variable, let pattern, let flags):
            guard let value = binding[variable] else { return false }
            return matchesRegex(value, pattern: pattern, flags: flags)

        case .contains(let variable, let substring):
            guard let value = binding[variable] else { return false }
            return value.contains(substring)

        case .startsWith(let variable, let prefix):
            guard let value = binding[variable] else { return false }
            return value.hasPrefix(prefix)

        case .endsWith(let variable, let suffix):
            guard let value = binding[variable] else { return false }
            return value.hasSuffix(suffix)

        // Logical operations
        case .and(let left, let right):
            return left.evaluate(binding) && right.evaluate(binding)

        case .or(let left, let right):
            return left.evaluate(binding) || right.evaluate(binding)

        case .not(let expr):
            return !expr.evaluate(binding)

        // Custom and constants
        case .custom(let predicate):
            return predicate(binding)

        case .alwaysTrue:
            return true

        case .alwaysFalse:
            return false
        }
    }

    // MARK: - Helpers

    private func matchesRegex(_ value: String, pattern: String, flags: String) -> Bool {
        var options: NSRegularExpression.Options = []
        if flags.contains("i") {
            options.insert(.caseInsensitive)
        }
        if flags.contains("m") {
            options.insert(.anchorsMatchLines)
        }
        if flags.contains("s") {
            options.insert(.dotMatchesLineSeparators)
        }

        guard let regex = try? NSRegularExpression(pattern: pattern, options: options) else {
            return false
        }

        let range = NSRange(value.startIndex..., in: value)
        return regex.firstMatch(in: value, range: range) != nil
    }

    // MARK: - Variables

    /// All variables referenced in this expression
    public var variables: Set<String> {
        switch self {
        case .equals(let v, _), .notEquals(let v, _),
             .lessThan(let v, _), .lessThanOrEqual(let v, _),
             .greaterThan(let v, _), .greaterThanOrEqual(let v, _),
             .bound(let v), .notBound(let v),
             .regex(let v, _), .regexWithFlags(let v, _, _),
             .contains(let v, _), .startsWith(let v, _), .endsWith(let v, _):
            return [v]

        case .variableEquals(let v1, let v2), .variableNotEquals(let v1, let v2):
            return [v1, v2]

        case .and(let left, let right), .or(let left, let right):
            return left.variables.union(right.variables)

        case .not(let expr):
            return expr.variables

        case .custom, .alwaysTrue, .alwaysFalse:
            return []
        }
    }
}

// MARK: - Convenience Constructors

extension FilterExpression {
    /// Create an AND of multiple expressions
    public static func allOf(_ expressions: [FilterExpression]) -> FilterExpression {
        guard let first = expressions.first else { return .alwaysTrue }
        return expressions.dropFirst().reduce(first) { .and($0, $1) }
    }

    /// Create an OR of multiple expressions
    public static func anyOf(_ expressions: [FilterExpression]) -> FilterExpression {
        guard let first = expressions.first else { return .alwaysFalse }
        return expressions.dropFirst().reduce(first) { .or($0, $1) }
    }

    /// Create a numeric comparison filter
    ///
    /// - Parameters:
    ///   - variable: Variable name
    ///   - op: Comparison operator ("<", "<=", ">", ">=", "==", "!=")
    ///   - value: Numeric value to compare against
    /// - Returns: Filter expression for numeric comparison
    public static func numeric(_ variable: String, _ op: String, _ value: Int) -> FilterExpression {
        .custom { binding in
            guard let v = binding.int(variable) else { return false }
            switch op {
            case "<": return v < value
            case "<=": return v <= value
            case ">": return v > value
            case ">=": return v >= value
            case "==", "=": return v == value
            case "!=", "<>": return v != value
            default: return false
            }
        }
    }
}

// MARK: - CustomStringConvertible

extension FilterExpression: CustomStringConvertible {
    public var description: String {
        switch self {
        case .equals(let v, let val):
            return "\(v) = \"\(val)\""
        case .notEquals(let v, let val):
            return "\(v) != \"\(val)\""
        case .lessThan(let v, let val):
            return "\(v) < \"\(val)\""
        case .lessThanOrEqual(let v, let val):
            return "\(v) <= \"\(val)\""
        case .greaterThan(let v, let val):
            return "\(v) > \"\(val)\""
        case .greaterThanOrEqual(let v, let val):
            return "\(v) >= \"\(val)\""
        case .variableEquals(let v1, let v2):
            return "\(v1) = \(v2)"
        case .variableNotEquals(let v1, let v2):
            return "\(v1) != \(v2)"
        case .bound(let v):
            return "BOUND(\(v))"
        case .notBound(let v):
            return "!BOUND(\(v))"
        case .regex(let v, let p):
            return "REGEX(\(v), \"\(p)\")"
        case .regexWithFlags(let v, let p, let f):
            return "REGEX(\(v), \"\(p)\", \"\(f)\")"
        case .contains(let v, let s):
            return "CONTAINS(\(v), \"\(s)\")"
        case .startsWith(let v, let p):
            return "STRSTARTS(\(v), \"\(p)\")"
        case .endsWith(let v, let s):
            return "STRENDS(\(v), \"\(s)\")"
        case .and(let l, let r):
            return "(\(l)) && (\(r))"
        case .or(let l, let r):
            return "(\(l)) || (\(r))"
        case .not(let e):
            return "!(\(e))"
        case .custom:
            return "CUSTOM(...)"
        case .alwaysTrue:
            return "TRUE"
        case .alwaysFalse:
            return "FALSE"
        }
    }
}

// MARK: - Equatable

extension FilterExpression: Equatable {
    public static func == (lhs: FilterExpression, rhs: FilterExpression) -> Bool {
        switch (lhs, rhs) {
        case (.equals(let l1, let l2), .equals(let r1, let r2)):
            return l1 == r1 && l2 == r2
        case (.notEquals(let l1, let l2), .notEquals(let r1, let r2)):
            return l1 == r1 && l2 == r2
        case (.lessThan(let l1, let l2), .lessThan(let r1, let r2)):
            return l1 == r1 && l2 == r2
        case (.lessThanOrEqual(let l1, let l2), .lessThanOrEqual(let r1, let r2)):
            return l1 == r1 && l2 == r2
        case (.greaterThan(let l1, let l2), .greaterThan(let r1, let r2)):
            return l1 == r1 && l2 == r2
        case (.greaterThanOrEqual(let l1, let l2), .greaterThanOrEqual(let r1, let r2)):
            return l1 == r1 && l2 == r2
        case (.variableEquals(let l1, let l2), .variableEquals(let r1, let r2)):
            return l1 == r1 && l2 == r2
        case (.variableNotEquals(let l1, let l2), .variableNotEquals(let r1, let r2)):
            return l1 == r1 && l2 == r2
        case (.bound(let l), .bound(let r)):
            return l == r
        case (.notBound(let l), .notBound(let r)):
            return l == r
        case (.regex(let l1, let l2), .regex(let r1, let r2)):
            return l1 == r1 && l2 == r2
        case (.regexWithFlags(let l1, let l2, let l3), .regexWithFlags(let r1, let r2, let r3)):
            return l1 == r1 && l2 == r2 && l3 == r3
        case (.contains(let l1, let l2), .contains(let r1, let r2)):
            return l1 == r1 && l2 == r2
        case (.startsWith(let l1, let l2), .startsWith(let r1, let r2)):
            return l1 == r1 && l2 == r2
        case (.endsWith(let l1, let l2), .endsWith(let r1, let r2)):
            return l1 == r1 && l2 == r2
        case (.and(let ll, let lr), .and(let rl, let rr)):
            return ll == rl && lr == rr
        case (.or(let ll, let lr), .or(let rl, let rr)):
            return ll == rl && lr == rr
        case (.not(let l), .not(let r)):
            return l == r
        case (.alwaysTrue, .alwaysTrue):
            return true
        case (.alwaysFalse, .alwaysFalse):
            return true
        case (.custom, .custom):
            // Custom closures can't be compared
            return false
        default:
            return false
        }
    }
}
