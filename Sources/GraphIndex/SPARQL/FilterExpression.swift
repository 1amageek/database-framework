// FilterExpression.swift
// GraphIndex - SPARQL-like filter expressions
//
// Represents filter conditions that can be applied to bindings.

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import Core
import DatabaseEngine
import DatabaseValue
import QueryIR

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

    /// Trigram similarity: sim(?var, "pattern") >= threshold
    case similarTo(String, String, Double)

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
    case custom(@Sendable (VariableBinding) throws -> Bool)

    /// Custom predicate with explicit variable tracking
    ///
    /// Stores the referenced variables alongside the closure, enabling proper
    /// filter pushdown optimization. This is the preferred form when converting
    /// from QueryIR.Expression.
    case customWithVariables(
        @Sendable (VariableBinding) throws -> Bool,
        variables: Set<String>
    )

    /// Canonical SPARQL expression plan. This preserves the QueryIR tree and
    /// execution traits instead of erasing it into a closure.
    case query(SPARQLExpressionPlan)

    /// Always true (identity for AND)
    case alwaysTrue

    /// Always false (identity for OR)
    case alwaysFalse

    // MARK: - Evaluation

    /// Evaluate the filter against a binding
    ///
    /// - Parameter binding: The variable binding to evaluate against
    /// - Returns: `true` if the filter matches, `false` otherwise
    public func evaluate(_ binding: VariableBinding) throws -> Bool {
        switch self {
        // Comparison with literal value
        // SPARQL semantics: unbound variables evaluate to false in comparisons
        // Numeric promotion applies only to typed numeric values and RDF literals.
        case .equals(let variable, let value):
            guard let v = binding[variable] else { return false }
            if Self.hasNull(v, value) { return false }
            return try Self.valuesEqual(v, value)

        case .notEquals(let variable, let value):
            guard let v = binding[variable] else { return false }
            if Self.hasNull(v, value) { return false }
            return try !Self.valuesEqual(v, value)

        case .lessThan(let variable, let value):
            guard let v = binding[variable] else { return false }
            if Self.hasNull(v, value) { return false }
            guard let cmp = try Self.compare(v, value) else { return false }
            return cmp == .orderedAscending

        case .lessThanOrEqual(let variable, let value):
            guard let v = binding[variable] else { return false }
            if Self.hasNull(v, value) { return false }
            guard let cmp = try Self.compare(v, value) else { return false }
            return cmp != .orderedDescending

        case .greaterThan(let variable, let value):
            guard let v = binding[variable] else { return false }
            if Self.hasNull(v, value) { return false }
            guard let cmp = try Self.compare(v, value) else { return false }
            return cmp == .orderedDescending

        case .greaterThanOrEqual(let variable, let value):
            guard let v = binding[variable] else { return false }
            if Self.hasNull(v, value) { return false }
            guard let cmp = try Self.compare(v, value) else { return false }
            return cmp != .orderedAscending

        // Variable comparison uses the same typed numeric semantics.
        case .variableEquals(let var1, let var2):
            guard let v1 = binding[var1], let v2 = binding[var2] else { return false }
            if Self.hasNull(v1, v2) { return false }
            return try Self.valuesEqual(v1, v2)

        case .variableNotEquals(let var1, let var2):
            guard let v1 = binding[var1], let v2 = binding[var2] else { return false }
            if Self.hasNull(v1, v2) { return false }
            return try !Self.valuesEqual(v1, v2)

        // Bound check
        case .bound(let variable):
            return binding.isBound(variable)

        case .notBound(let variable):
            return !binding.isBound(variable)

        // String operations — extract string representation from FieldValue
        case .regex(let variable, let pattern):
            guard let value = binding.string(variable) else { return false }
            return try matchesRegex(value, pattern: pattern, flags: "")

        case .regexWithFlags(let variable, let pattern, let flags):
            guard let value = binding.string(variable) else { return false }
            return try matchesRegex(value, pattern: pattern, flags: flags)

        case .contains(let variable, let substring):
            guard let value = binding.string(variable) else { return false }
            return DatabaseText.contains(substring, in: value)

        case .startsWith(let variable, let prefix):
            guard let value = binding.string(variable) else { return false }
            return value.hasPrefix(prefix)

        case .endsWith(let variable, let suffix):
            guard let value = binding.string(variable) else { return false }
            return value.hasSuffix(suffix)

        case .similarTo(let variable, let pattern, let threshold):
            guard let value = binding.string(variable) else { return false }
            return TrigramSimilarity.score(value, pattern) >= threshold

        // Logical operations
        case .and(let left, let right):
            return try left.evaluate(binding) && right.evaluate(binding)

        case .or(let left, let right):
            return try left.evaluate(binding) || right.evaluate(binding)

        case .not(let expr):
            return try !expr.evaluate(binding)

        // Custom and constants
        case .custom(let predicate):
            return try predicate(binding)

        case .customWithVariables(let predicate, _):
            return try predicate(binding)

        case .query(let plan):
            return try ExpressionEvaluator.evaluateAsBoolean(
                plan.expression,
                binding: binding
            )

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

    // MARK: - Numeric Comparison

    private static func valuesEqual(
        _ left: FieldValue,
        _ right: FieldValue
    ) throws -> Bool {
        if case .rdfTerm(.literal(let leftLiteral)) = left,
           case .rdfTerm(.literal(let rightLiteral)) = right {
            if leftLiteral == rightLiteral { return true }
            switch try SPARQLValueComparator().compare(leftLiteral, rightLiteral) {
            case .equal: return true
            case .less, .greater: return false
            case .unordered, .typeError:
                throw SPARQLExpressionEvaluationError.typeError(
                    "RDF literals are not value-comparable"
                )
            }
        }
        if let leftNumeric = SPARQLNumericValue(left),
           let rightNumeric = SPARQLNumericValue(right) {
            guard let comparison = leftNumeric.compare(to: rightNumeric) else {
                throw SPARQLExpressionEvaluationError.typeError(
                    "numeric values are unordered"
                )
            }
            return comparison == .orderedSame
        }
        return left == right
    }

    private static func compare(
        _ left: FieldValue,
        _ right: FieldValue
    ) throws -> ComparisonResult? {
        if case .rdfTerm(.literal(let leftLiteral)) = left,
           case .rdfTerm(.literal(let rightLiteral)) = right {
            switch try SPARQLValueComparator().compare(leftLiteral, rightLiteral) {
            case .less: return .orderedAscending
            case .equal: return .orderedSame
            case .greater: return .orderedDescending
            case .unordered, .typeError:
                throw SPARQLExpressionEvaluationError.typeError(
                    "RDF literals are not order-comparable"
                )
            }
        }
        if let leftNumeric = SPARQLNumericValue(left),
           let rightNumeric = SPARQLNumericValue(right) {
            guard let comparison = leftNumeric.compare(to: rightNumeric) else {
                throw SPARQLExpressionEvaluationError.typeError(
                    "numeric values are unordered"
                )
            }
            return comparison
        }
        return left.compare(to: right)
    }

    // MARK: - Helpers

    private func matchesRegex(
        _ value: String,
        pattern: String,
        flags: String
    ) throws -> Bool {
        try SPARQLRegularExpression.evaluateMatch(
            value,
            pattern: pattern,
            flags: flags
        )
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
             .contains(let v, _), .startsWith(let v, _), .endsWith(let v, _),
             .similarTo(let v, _, _):
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

        case .query(let plan):
            return plan.referencedVariables

        case .alwaysTrue, .alwaysFalse:
            return []
        }
    }

    public var isPushdownSafe: Bool {
        switch self {
        case .custom, .customWithVariables:
            return false
        case .query(let plan):
            return plan.isFilterPushdownSafe
        case .and(let left, let right), .or(let left, let right):
            return left.isPushdownSafe && right.isPushdownSafe
        case .not(let expression):
            return expression.isPushdownSafe
        default:
            return true
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
        case .similarTo(let v, let p, let t):
            return "TRIGRAM_SIM(\(v), \"\(p)\") >= \(t)"
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
        case .query(let plan):
            return "QUERY_IR(\(plan.expression))"
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
        case (.similarTo(let l1, let l2, let l3), .similarTo(let r1, let r2, let r3)):
            return l1 == r1 && l2 == r2 && l3 == r3
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
        case (.query(let left), .query(let right)):
            return left == right
        default:
            return false
        }
    }
}
