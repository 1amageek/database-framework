// FilterExpression.swift
// GraphIndex - SPARQL-like filter expressions
//
// Represents filter conditions that can be applied to bindings.

import Foundation
import Core

/// Filter expression for FILTER clauses
///
/// **Design**: Recursive expression tree supporting common SPARQL filter operations.
/// Expressions are evaluated against a VariableBinding to produce a boolean result.
/// Comparison operators use `FieldValue` for type-safe numeric and string comparisons.
///
/// **Usage**:
/// ```swift
/// // Numeric comparison (uses FieldValue's Comparable)
/// let filter = FilterExpression.greaterThan("?age", 18)
///
/// // String equality
/// let filter = FilterExpression.equals("?name", "Alice")
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

    /// Variable equals value: ?var == value
    case equals(String, FieldValue)

    /// Variable not equals value: ?var != value
    case notEquals(String, FieldValue)

    /// Variable less than value: ?var < value (uses FieldValue.Comparable)
    case lessThan(String, FieldValue)

    /// Variable less than or equal: ?var <= value (uses FieldValue.Comparable)
    case lessThanOrEqual(String, FieldValue)

    /// Variable greater than: ?var > value (uses FieldValue.Comparable)
    case greaterThan(String, FieldValue)

    /// Variable greater than or equal: ?var >= value (uses FieldValue.Comparable)
    case greaterThanOrEqual(String, FieldValue)

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
    /// **Warning**: Variables cannot be extracted from this case, breaking filter pushdown.
    /// Use `customWithVariables` when variable information is available.
    case custom(@Sendable (VariableBinding) -> Bool)

    /// Custom predicate with explicit variable tracking
    ///
    /// Stores the referenced variables alongside the closure, enabling proper
    /// filter pushdown optimization. This is the preferred form when converting
    /// from QueryIR.Expression.
    case customWithVariables(@Sendable (VariableBinding) -> Bool, variables: Set<String>)

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
        // Comparison with literal value
        // SPARQL semantics: unbound variables evaluate to false in comparisons
        // Numeric promotion: string values are coerced to numeric when compared
        // against numeric values (SPARQL 1.1, Section 17.3 Operator Mapping)
        case .equals(let variable, let value):
            guard let v = binding[variable] else { return false }
            let (lhs, rhs) = Self.numericPromote(v, value)
            if Self.hasNull(lhs, rhs) { return false }
            return lhs == rhs

        case .notEquals(let variable, let value):
            guard let v = binding[variable] else { return false }
            let (lhs, rhs) = Self.numericPromote(v, value)
            if Self.hasNull(lhs, rhs) { return false }
            return lhs != rhs

        case .lessThan(let variable, let value):
            guard let v = binding[variable] else { return false }
            let (lhs, rhs) = Self.numericPromote(v, value)
            if Self.hasNull(lhs, rhs) { return false }
            guard let cmp = lhs.compare(to: rhs) else { return false }
            return cmp == .orderedAscending

        case .lessThanOrEqual(let variable, let value):
            guard let v = binding[variable] else { return false }
            let (lhs, rhs) = Self.numericPromote(v, value)
            if Self.hasNull(lhs, rhs) { return false }
            guard let cmp = lhs.compare(to: rhs) else { return false }
            return cmp != .orderedDescending

        case .greaterThan(let variable, let value):
            guard let v = binding[variable] else { return false }
            let (lhs, rhs) = Self.numericPromote(v, value)
            if Self.hasNull(lhs, rhs) { return false }
            guard let cmp = lhs.compare(to: rhs) else { return false }
            return cmp == .orderedDescending

        case .greaterThanOrEqual(let variable, let value):
            guard let v = binding[variable] else { return false }
            let (lhs, rhs) = Self.numericPromote(v, value)
            if Self.hasNull(lhs, rhs) { return false }
            guard let cmp = lhs.compare(to: rhs) else { return false }
            return cmp != .orderedAscending

        // Variable comparison — also applies numeric promotion
        case .variableEquals(let var1, let var2):
            guard let v1 = binding[var1], let v2 = binding[var2] else { return false }
            let (lhs, rhs) = Self.numericPromote(v1, v2)
            if Self.hasNull(lhs, rhs) { return false }
            return lhs == rhs

        case .variableNotEquals(let var1, let var2):
            guard let v1 = binding[var1], let v2 = binding[var2] else { return false }
            let (lhs, rhs) = Self.numericPromote(v1, v2)
            if Self.hasNull(lhs, rhs) { return false }
            return lhs != rhs

        // Bound check
        case .bound(let variable):
            return binding.isBound(variable)

        case .notBound(let variable):
            return !binding.isBound(variable)

        // String operations — extract string representation from FieldValue
        case .regex(let variable, let pattern):
            guard let value = binding.string(variable) else { return false }
            return matchesRegex(value, pattern: pattern, flags: "")

        case .regexWithFlags(let variable, let pattern, let flags):
            guard let value = binding.string(variable) else { return false }
            return matchesRegex(value, pattern: pattern, flags: flags)

        case .contains(let variable, let substring):
            guard let value = binding.string(variable) else { return false }
            return value.contains(substring)

        case .startsWith(let variable, let prefix):
            guard let value = binding.string(variable) else { return false }
            return value.hasPrefix(prefix)

        case .endsWith(let variable, let suffix):
            guard let value = binding.string(variable) else { return false }
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

        case .customWithVariables(let predicate, _):
            return predicate(binding)

        case .alwaysTrue:
            return true

        case .alwaysFalse:
            return false
        }
    }

    // MARK: - SPARQL Three-Valued Logic

    /// SPARQL three-valued logic: comparisons involving NULL produce error → false
    ///
    /// FieldValue's Equatable treats `.null == .null` as `true` (correct for system-wide use),
    /// but SPARQL Section 17.2 requires that any comparison involving NULL yields "error",
    /// which FILTER evaluates as `false`.
    ///
    /// Reference: W3C SPARQL 1.1, Section 17.2 (Filter Evaluation)
    private static func hasNull(_ lhs: FieldValue, _ rhs: FieldValue) -> Bool {
        lhs.isNull || rhs.isNull
    }

    // MARK: - Numeric Promotion

    /// Promote string values to numeric when comparing against numeric values
    ///
    /// SPARQL semantics: When comparing values of different types,
    /// string values that represent numbers can be promoted to numeric types.
    /// This happens at the operation boundary, not at storage time,
    /// preserving the original type in the binding.
    ///
    /// Reference: SPARQL 1.1, Section 17.3 (Operator Mapping)
    private static func numericPromote(_ lhs: FieldValue, _ rhs: FieldValue) -> (FieldValue, FieldValue) {
        switch (lhs, rhs) {
        case (.string(let s), _) where rhs.isNumeric:
            if let promoted = parseNumeric(s) { return (promoted, rhs) }
        case (_, .string(let s)) where lhs.isNumeric:
            if let promoted = parseNumeric(s) { return (lhs, promoted) }
        default:
            break
        }
        return (lhs, rhs)
    }

    /// Parse a string as a numeric FieldValue
    ///
    /// Tries Int64 first (for exact integer representation),
    /// then Double (for floating-point). Rejects non-finite values.
    private static func parseNumeric(_ s: String) -> FieldValue? {
        if let i = Int64(s) { return .int64(i) }
        if let d = Double(s), d.isFinite { return .double(d) }
        return nil
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

        case .custom:
            return []

        case .customWithVariables(_, let vars):
            return vars

        case .alwaysTrue, .alwaysFalse:
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
        let fieldValue = FieldValue.int64(Int64(value))
        switch op {
        case "<": return .lessThan(variable, fieldValue)
        case "<=": return .lessThanOrEqual(variable, fieldValue)
        case ">": return .greaterThan(variable, fieldValue)
        case ">=": return .greaterThanOrEqual(variable, fieldValue)
        case "==", "=": return .equals(variable, fieldValue)
        case "!=", "<>": return .notEquals(variable, fieldValue)
        default: return .alwaysFalse
        }
    }
}

// MARK: - CustomStringConvertible

extension FilterExpression: CustomStringConvertible {
    public var description: String {
        switch self {
        case .equals(let v, let val):
            return "\(v) = \(val)"
        case .notEquals(let v, let val):
            return "\(v) != \(val)"
        case .lessThan(let v, let val):
            return "\(v) < \(val)"
        case .lessThanOrEqual(let v, let val):
            return "\(v) <= \(val)"
        case .greaterThan(let v, let val):
            return "\(v) > \(val)"
        case .greaterThanOrEqual(let v, let val):
            return "\(v) >= \(val)"
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
        case .customWithVariables(_, let vars):
            return "CUSTOM(vars: \(vars.sorted().joined(separator: ", ")))"
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
        case (.customWithVariables(_, let lv), .customWithVariables(_, let rv)):
            // Compare by variables only (closures can't be compared)
            return lv == rv
        default:
            return false
        }
    }
}
